use std::collections::{hash_map::Entry, HashMap, VecDeque};
use std::sync::mpsc::Sender;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, RwLock,
};
use std::time::{Duration, Instant};

use ticket_sale_core::{Request, RequestKind};
use uuid::Uuid;

use super::database::Database;
use super::messages::CoordinatorMessage; // Assuming CoordinatorMessage is defined in the coordinator module

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

    /// Prioritized channel for incoming requests.
    receiver: Arc<Mutex<Receiver<ServerOrRequestMessage>>>,

    /// Flag indicating whether the server is currently terminating.
    pub terminating: AtomicBool,

    /// Flag indicating whether the server is currently in the process of stopping.
    is_stopping: AtomicBool, // Added flag

    /// Flag indicating whether the server has fully stopped.
    has_stopped: AtomicBool, // Added flag

    /// Channel to send messages to the Coordinator.
    coordinator_channel: Sender<CoordinatorMessage>, // Added for messaging
}

/// Represents a ticket reservation.
struct Reservation {
    ticket: u32,
    timestamp: Instant,
}

impl Reservation {
    /// Creates a new `Reservation` instance with the current timestamp.
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
    /// * `coordinator_channel` - Channel to send messages to the coordinator.
    ///
    /// # Returns
    ///
    /// * `Self` - New instance of `Server`.
    pub fn new(
        database: Arc<RwLock<Database>>,
        ticket_count: u32,
        reservation_timeout: u32,
        coordinator_channel: Sender<CoordinatorMessage>,
    ) -> Self {
        let id = Uuid::new_v4();

        let available_tickets = {
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
            is_stopping: AtomicBool::new(false), // Initialize the flag
            has_stopped: AtomicBool::new(false), // Initialize the flag
            coordinator_channel,                 // Set up the coordinator channel
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Handles incoming requests
    pub fn handle_request(&mut self, mut rq: Request) {
        // Check if the server is currently stopping.
        if self.is_stopping.load(Ordering::SeqCst) {
            rq.respond_with_err("Server is stopping.");
            return;
        }

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

        self.clear_expired_reservations();

        match rq.kind() {
            RequestKind::NumAvailableTickets => {
                let estimate = *self.last_estimate.lock().unwrap();
                rq.respond_with_int(estimate);
            }

            RequestKind::ReserveTicket => {
                if self.server_state == ServerState::Terminating
                    || self.server_state == ServerState::HasStopped
                {
                    rq.respond_with_err(
                        "Server is terminating/has terminated. Cannot reserve tickets.",
                    );
                } else {
                    rq.set_server_id(self.id);

                    let customer_id = rq.customer_id();
                    let mut reservations = self.reservations.lock().unwrap();
                    let mut available_tickets = self.available_tickets.lock().unwrap();

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
                                // No tickets available, check database
                                {
                                    let mut db = self.database.write().unwrap();
                                    if let Some(new_ticket) = db.allocate(1).pop() {
                                        available_tickets.push_back(new_ticket);
                                        if let Some(ticket) = available_tickets.pop_front() {
                                            entry.insert(Reservation::new(ticket));
                                            rq.respond_with_int(ticket);
                                        }
                                    } else {
                                        // Handle the case where no tickets can be allocated
                                        rq.respond_with_sold_out();
                                    }
                                }
                            }
                        }
                    }
                }
            }

            RequestKind::BuyTicket => {
                rq.set_server_id(self.id);

                if let Some(ticket_id) = rq.read_u32() {
                    let customer_id = rq.customer_id();
                    let mut reservations: std::sync::MutexGuard<HashMap<Uuid, Reservation>> =
                        self.reservations.lock().unwrap();

                    if let Some(reservation) = reservations.get(&customer_id) {
                        if ticket_id != reservation.ticket {
                            rq.respond_with_err("Invalid ticket id provided!");
                        } else {
                            reservations.remove(&customer_id);
                            rq.respond_with_int(ticket_id);

                            // Allocate a new ticket from the database to replace the sold one.
                            let mut db = self.database.write().unwrap();
                            if let Some(new_ticket) = db.allocate(1).pop() {
                                let mut available_tickets = self.available_tickets.lock().unwrap();
                                available_tickets.push_back(new_ticket);
                            }
                        }
                    } else {
                        rq.respond_with_err("No ticket has been reserved!");
                    }
                } else {
                    rq.respond_with_err("No ticket id provided!");
                }
            }

            RequestKind::AbortPurchase => {
                rq.set_server_id(self.id);

                if let Some(ticket_id) = rq.read_u32() {
                    let customer_id = rq.customer_id();
                    let mut reservations = self.reservations.lock().unwrap();
                    let mut available_tickets = self.available_tickets.lock().unwrap();

                    if let Some(reservation) = reservations.get(&customer_id) {
                        if ticket_id != reservation.ticket {
                            rq.respond_with_err("Invalid ticket id provided!");
                        } else {
                            reservations.remove(&customer_id);
                            available_tickets.push_back(ticket_id);
                            rq.respond_with_int(ticket_id);
                        }
                    } else {
                        rq.respond_with_err("No ticket has been reserved!");
                    }
                } else {
                    rq.respond_with_err("No ticket id provided!");
                }
            }

            _ => rq.respond_with_err("Unsupported request kind."),
        }
    }

    pub fn get_allocated_ticket_count(&self) -> u32 {
        self.available_tickets.lock().unwrap().len() as u32
    }

    fn update_ticket_estimate(&self, new_estimate: u32) {
        let mut last_estimate = self.last_estimate.lock().unwrap();
        *last_estimate = new_estimate;
        println!(
            "Server {} updated ticket estimate to {}",
            self.id, new_estimate
        );
    }

    pub fn has_stopped(&self) -> bool {
        self.server_state == ServerState::HasStopped
    }

    // Immediate shutdown method
    pub fn shutdown(&mut self) {
        self.server_state = ServerState::Terminating;
        println!("Server {} is shutting down", self.id);

        let mut reservations = self.reservations.lock().unwrap();
        let mut available_tickets = self.available_tickets.lock().unwrap();

        // Cancel all reservations and release tickets
        for (customer_id, reservation) in reservations.drain() {
            println!(
                "Releasing ticket {} for customer {}",
                reservation.ticket, customer_id
            );
            available_tickets.push_back(reservation.ticket);
        }
        let mut db = self.database.write().unwrap();

        while let Some(ticket) = available_tickets.pop_front() {
            db.deallocate(&[ticket]); // Pass a slice of `u32` to the `deallocate` method
            println!(
                "Server {} returned ticket {} to the database.",
                self.id, ticket
            );
        }

        self.server_state = ServerState::HasStopped;
        println!("Server {} has been fully shut down", self.id);
    }

    pub fn terminate(&mut self) {
        self.server_state = ServerState::Terminating;
        println!("Server {} is terminating", self.id);

        loop {
            self.clear_expired_reservations();

            // Check if there are any active reservations remaining
            println!("Checking for active reservations...");
            let reservations_empty = {
                let reservations = self.reservations.lock().unwrap();
                reservations.is_empty()
            };
            println!("Active reservations: {}", !reservations_empty);

            // If there are no active reservations, the server can be terminated
            if reservations_empty {
                break;
            }

            println!("attemting to handle messages");
            // Handle incoming messages
            // Attempt to lock the receiver and handle messages without blocking indefinitely
            let message = {
                let receiver = self.receiver.lock().unwrap();
                receiver.recv_timeout(Duration::from_secs(1)) // Non-blocking or with a
                                                              // timeout
            };

            println!("message received");
            if let Ok(message) = message {
                match message {
                    ServerOrRequestMessage::ServerMessage(server_message) => {
                        self.handle_server_message(server_message);
                    }
                    ServerOrRequestMessage::ClientRequest(request) => {
                        match *request.kind() {
                            RequestKind::ReserveTicket => {
                                // Refuse new reservations and redirect client to another server
                                let other_server_id: Option<Uuid> = None;
                                if let Some(other_id) = other_server_id {
                                    request.respond_with_err(&format!(
                                        "Server is terminating, please try another server: {}",
                                        other_id
                                    ));
                                } else {
                                    request.respond_with_err(
                                        "Server is terminating, no available servers.",
                                    );
                                }
                            }
                            RequestKind::BuyTicket | RequestKind::AbortPurchase => {
                                // Handle buy and abort purchase requests
                                self.handle_request(request);
                            }
                            _ => {
                                // Respond with an error for any other types of requests
                                request.respond_with_err(
                                    "Server is terminating, only purchase and cancellation requests are allowed.",
                                );
                            }
                        }
                    }
                }
            } else {
                // If no messages are received, wait before checking again
                std::thread::sleep(Duration::from_millis(100));
            }
        }

        let mut reservations = self.reservations.lock().unwrap();
        let mut available_tickets = self.available_tickets.lock().unwrap();

        // Cancel all reservations and release tickets
        for (customer_id, reservation) in reservations.drain() {
            println!(
                "Releasing ticket {} for customer {}",
                reservation.ticket, customer_id
            );
            available_tickets.push_back(reservation.ticket);
        }

        println!("Server {} has been fully shut down", self.id);

        // Mark the server as fully stopped.
        self.has_stopped.store(true, Ordering::SeqCst);

        // Notify the Coordinator about the shutdown.
        let _ = self
            .coordinator_channel
            .send(CoordinatorMessage::ServerUpdate(self.id, 0));
    }

    /// Aborts and removes expired reservations.
    fn clear_expired_reservations(&mut self) {
        let mut reservations = self.reservations.lock().unwrap();
        let mut available_tickets: std::sync::MutexGuard<VecDeque<u32>> =
            self.available_tickets.lock().unwrap();

        let mut expired_count = 0;
        reservations.retain(|_, res| {
            if res.age_secs() > self.reservation_timeout.as_secs() {
                available_tickets.push_back(res.ticket);
                println!(
                    "Server {} cleared expired reservation for ticket {}",
                    self.id, res.ticket
                );
                expired_count += 1;
                false
            } else {
                true
            }
        });

        // Add tickets back to the central database.
        db.deallocate(&tickets_to_return);

        // Also add remaining available tickets to the database.
        let remaining_tickets: Vec<u32> = available_tickets.drain(..).collect();
        db.deallocate(&remaining_tickets);

        println!("Server {} has been fully terminated", self.id);

        // Notify the Coordinator about the termination.
        let _ = self
            .coordinator_channel
            .send(CoordinatorMessage::ServerUpdate(self.id, 0));
    }

    /// Initiates the stopping process for the server.
    pub fn stop(&self) {
        // Mark the server as stopping.
        self.is_stopping.store(true, Ordering::SeqCst);
        println!("Server {} is stopping", self.id);

        // Proceed to shutdown the server.
        self.shutdown();
    }
}
