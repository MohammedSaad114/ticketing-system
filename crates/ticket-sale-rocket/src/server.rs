use std::collections::{hash_map::Entry, HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, RwLock,
};
use std::time::{Duration, Instant};

use ticket_sale_core::{Request, RequestKind};
use uuid::Uuid;

use super::database::Database;

/// Represents a server that handles ticket sales.
pub struct Server {
    /// Unique identifier for the server.
    id: Uuid,

    /// Shared database instance.
    database: Arc<RwLock<Database>>,

    /// Queue of available tickets managed by this server.
    available_tickets: Mutex<VecDeque<u32>>,

    /// Map of customer reservations with reservation expiration timestamps.
    reservations: Mutex<HashMap<Uuid, Reservation>>,

    /// Timeout duration for reservations.
    reservation_timeout: Duration,

    /// Last estimate of available tickets received from the estimator.
    last_estimate: Mutex<u32>,

    /// Flag indicating whether the server is currently running.
    running: AtomicBool,

    /// Flag indicating whether the server is currently terminating.
    terminating: AtomicBool,
}

/// Represents a ticket reservation.
struct Reservation {
    ticket: u32,
    timestamp: Instant,
}

impl Reservation {
    /// Creates a new `Reservation` instance with the current timestamp.
    ///
    /// # Arguments
    ///
    /// * `ticket` - The ticket ID being reserved.
    ///
    /// # Returns
    ///
    /// * `Self` - New instance of `Reservation`.
    #[inline]
    fn new(ticket: u32) -> Self {
        Self {
            ticket,
            timestamp: Instant::now(),
        }
    }

    /// Returns the age of the ticket.
    ///
    /// # Arguments
    ///
    /// * `self` - instance of the ticket.
    ///
    /// # Returns
    ///
    /// * `age` - in seconds.
    #[inline]
    fn age_secs(&self) -> u64 {
        self.timestamp.elapsed().as_secs()
    }
}

impl Server {
    /// Creates a new `Server` instance.
    ///
    /// # Arguments
    ///
    /// * `database` - Shared database instance.
    /// * `ticket_count` - Number of tickets to allocate to this server.
    /// * `reservation_timeout` - Timeout duration for reservations in seconds.
    ///
    /// # Returns
    ///
    /// * `Self` - New instance of `Server`.
    pub fn new(
        database: Arc<RwLock<Database>>,
        ticket_count: u32,
        reservation_timeout: u32,
    ) -> Self {
        let id = Uuid::new_v4();
        let available_tickets = {
            // Lock the database for writing and allocate the specified number of tickets.
            let mut db = database.write().unwrap();
            let allocated_tickets = db.allocate(ticket_count);
            Mutex::new(VecDeque::from(allocated_tickets))
        };

        Self {
            id,
            database,
            available_tickets,
            reservations: Mutex::new(HashMap::new()),
            reservation_timeout: Duration::from_secs(reservation_timeout.into()),
            last_estimate: Mutex::new(0),
            running: AtomicBool::new(true),
            terminating: AtomicBool::new(false),
        }
    }

    /// Returns the unique identifier of the server.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Abort and remove expired reservations
    fn clear_expired_reservations(&mut self) {
        let mut reservations = self.reservations.lock().unwrap();
        reservations.retain(|_, res| {
            if res.age_secs() > self.reservation_timeout.as_secs() {
                self.available_tickets.lock().unwrap().push_back(res.ticket);
                false
            } else {
                true
            }
        });
    }

    /// Handles incoming requests by dispatching them to the appropriate handler based on
    /// the request type.
    ///
    /// # Arguments
    ///
    /// * `rq` - The request to handle.
    pub fn handle_request(&mut self, mut rq: Request) {
        // Check if the server is currently terminating.
        if self.terminating.load(Ordering::SeqCst) {
            rq.respond_with_err("Server is terminating.");
            return;
        }

        // Check if the server is currently running.
        if !self.running.load(Ordering::SeqCst) {
            rq.respond_with_err("Server is shutting down.");
            return;
        }

        // Clear expired reservations before handling the request
        self.clear_expired_reservations();

        match rq.kind() {
            // Handle fetching the number of available tickets.
            RequestKind::NumAvailableTickets => {
                // Return the current estimate of available tickets.
                let estimate = *self.last_estimate.lock().unwrap();
                rq.respond_with_int(estimate);
            }

            // Handle ticket reservation logic.
            RequestKind::ReserveTicket => {
                // If the server is terminating, reject reservation requests
                if self.terminating.load(Ordering::SeqCst) {
                    rq.respond_with_err("Server is terminating. Cannot reserve tickets.");
                } else {
                    rq.set_server_id(self.id);

                    let customer_id = rq.customer_id();

                    // Lock both reservations and available_tickets at once.
                    let mut reservations = self.reservations.lock().unwrap();
                    let mut available_tickets = self.available_tickets.lock().unwrap();

                    // Check if the customer has already reserved a ticket.
                    match reservations.entry(customer_id) {
                        Entry::Occupied(_) => {
                            rq.respond_with_err("A ticket has already been reserved!");
                        }
                        Entry::Vacant(entry) => {
                            // Attempt to reserve a ticket.
                            if let Some(ticket) = available_tickets.pop_front() {
                                entry.insert(Reservation::new(ticket));
                                rq.respond_with_int(ticket);
                            } else {
                                // No tickets left to reserve; check the database for more.
                                let mut db = self.database.write().unwrap();
                                let additional_tickets = db.allocate(10); // Attempt to allocate more tickets

                                if additional_tickets.is_empty() {
                                    // No additional tickets were allocated; notify that sold out
                                    rq.respond_with_sold_out();
                                } else {
                                    // Add the newly allocated tickets to the server's queue
                                    available_tickets.extend(additional_tickets);
                                    // Try reserving a ticket again
                                    if let Some(ticket) = available_tickets.pop_front() {
                                        entry.insert(Reservation::new(ticket));
                                        rq.respond_with_int(ticket);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Handle the purchase of a reserved ticket.
            RequestKind::BuyTicket => {
                rq.set_server_id(self.id);

                if let Some(ticket_id) = rq.read_u32() {
                    let customer_id = rq.customer_id();
                    let mut reservations = self.reservations.lock().unwrap();

                    // Check if the reservation exists for the customer.
                    if let Some(reservation) = reservations.get(&customer_id) {
                        // Validate that the ticket ID matches the reserved ticket.
                        if ticket_id != reservation.ticket {
                            rq.respond_with_err("Invalid ticket id provided!");
                        } else {
                            // Complete the purchase and remove the reservation.
                            reservations.remove(&customer_id);
                            rq.respond_with_int(ticket_id);
                        }
                    } else {
                        // No reservation found for the customer.
                        rq.respond_with_err("No ticket has been reserved!");
                    }
                } else {
                    // The client didn't provide a ticket ID.
                    rq.respond_with_err("No ticket id provided!");
                }
            }

            // Handle aborting a purchase (canceling a reservation).
            RequestKind::AbortPurchase => {
                rq.set_server_id(self.id);

                if let Some(ticket_id) = rq.read_u32() {
                    let customer_id = rq.customer_id();
                    let mut reservations = self.reservations.lock().unwrap();
                    let mut available_tickets = self.available_tickets.lock().unwrap();

                    // Check if the reservation exists for the customer.
                    if let Some(reservation) = reservations.get(&customer_id) {
                        // Validate that the ticket ID matches the reserved ticket.
                        if ticket_id != reservation.ticket {
                            rq.respond_with_err("Invalid ticket id provided!");
                        } else {
                            // Cancel the reservation and return the ticket to the available pool.
                            reservations.remove(&customer_id);
                            available_tickets.push_back(ticket_id);
                            rq.respond_with_int(ticket_id);
                        }
                    } else {
                        // No reservation found for the customer.
                        rq.respond_with_err("No ticket has been reserved!");
                    }
                } else {
                    // The client didn't provide a ticket ID.
                    rq.respond_with_err("No ticket id provided!");
                }
            }

            // Handle unsupported request types.
            _ => rq.respond_with_err("Unsupported request kind."),
        }
    }

    /// Gets the current number of allocated tickets managed by this server.
    ///
    /// # Returns
    ///
    /// * `u32` - Number of allocated tickets.
    pub fn get_allocated_ticket_count(&self) -> u32 {
        self.available_tickets.lock().unwrap().len() as u32
    }

    /// Updates the estimate of available tickets with the new value received from the
    /// estimator.
    ///
    /// # Arguments
    ///
    /// * `new_estimate` - The new estimate value to be updated.
    pub fn update_estimate(&self, new_estimate: u32) {
        let mut last_estimate = self.last_estimate.lock().unwrap();
        *last_estimate = new_estimate;
    }

    /// Gracefully shuts down the server, stopping it from processing new requests.
    pub fn shutdown(&self) {
        // Set the running flag to false to stop handling new requests.
        self.running.store(false, Ordering::SeqCst);
        println!("Server {} is shutting down", self.id);

        // Ensure reservations are handled or released properly
        let mut reservations = self.reservations.lock().unwrap();
        let mut available_tickets = self.available_tickets.lock().unwrap();

        // Process any pending reservations before shutdown
        for (customer_id, reservation) in reservations.drain() {
            println!(
                "Releasing ticket {} for customer {}",
                reservation.ticket, customer_id
            );
            available_tickets.push_back(reservation.ticket);
        }

        println!("Server {} has been fully shut down", self.id);
    }

    /// Mark the server for termination, ensuring no new requests are processed
    /// and handle ongoing reservations.
    pub fn terminate(&self) {
        self.terminating.store(true, Ordering::SeqCst);
        println!("Server {} is terminating", self.id);

        // Gracefully shut down the server
        self.shutdown();

        // Process any pending reservations during termination
        let mut reservations = self.reservations.lock().unwrap();
        let mut available_tickets = self.available_tickets.lock().unwrap();

        // Return canceled tickets to the central database
        let mut db = self.database.write().unwrap();
        let tickets_to_return: Vec<u32> = reservations
            .drain()
            .map(|(_, reservation)| reservation.ticket)
            .collect();

        // Add tickets back to the central database
        db.deallocate(&tickets_to_return);

        // Also add remaining available tickets to the database
        let remaining_tickets: Vec<u32> = available_tickets.drain(..).collect();
        db.deallocate(&remaining_tickets);

        println!("Server {} has been fully terminated", self.id);
    }
}
