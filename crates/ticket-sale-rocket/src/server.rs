use std::collections::{hash_map::Entry, HashMap, VecDeque};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use ticket_sale_core::{Request, RequestKind};
use uuid::Uuid;

use super::database::Database;
use crate::coordinator::ServerState;
use crate::messages::{ServerMessage, ServerOrRequestMessage};

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

    /// State of the server.
    server_state: Arc<Mutex<ServerState>>,
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
    pub fn new(
        database: Arc<RwLock<Database>>,
        ticket_count: u32,
        reservation_timeout: u32,
        receiver: Arc<Mutex<Receiver<ServerOrRequestMessage>>>,
        server_state: Arc<Mutex<ServerState>>,
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
            receiver,
            server_state,
        }
    }

    pub fn run(&mut self) {
        self.handle_messages();
    }

    /// Handles incoming messages based on their priority.
    pub fn handle_messages(&mut self) {
        loop {
            // Check server state at the start of the loop
            let state = *self.server_state.lock().unwrap();
            if state != ServerState::Running {
                break;
            }

            let message = {
                let receiver = self.receiver.lock().unwrap();
                receiver.recv()
            };

            if let Ok(message) = message {
                match message {
                    ServerOrRequestMessage::ServerMessage(server_message) => {
                        self.handle_server_message(server_message);
                    }
                    ServerOrRequestMessage::ClientRequest(request) => {
                        self.handle_request(request);
                    }
                }
            } else {
                // Handle the case where receiving a message fails
                if *self.server_state.lock().unwrap() == ServerState::Running {
                    println!("Error receiving message on server {}.", self.id);
                }
                break; // Exit the loop on error
            }

            // Check if we need to terminate or stop
            if *self.server_state.lock().unwrap() != ServerState::Running {
                break;
            }
        }
    }

    fn handle_server_message(&mut self, message: ServerMessage) {
        match message {
            ServerMessage::ShutdownServer => self.shutdown(),
            ServerMessage::TerminateServer => self.terminate(),
            ServerMessage::UpdateTicketEstimate(new_estimate) => {
                self.update_ticket_estimate(new_estimate);
            }
            ServerMessage::RequestTicketCount(sender) => {
                let ticket_count = self.get_allocated_ticket_count();
                let _ = sender.send(ticket_count);
            }
            ServerMessage::CurrentState(sender) => {
                let _ = sender.send(*self.server_state.lock().unwrap());
            }
            ServerMessage::CheckIfTerminating(response_sender) => {
                let is_terminating = *self.server_state.lock().unwrap() == ServerState::Terminating;
                let _ = response_sender.send(is_terminating);
            }
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Handles incoming requests
    pub fn handle_request(&mut self, mut rq: Request) {
        {
            let state = self.server_state.lock().unwrap();
            if *state == ServerState::Terminating || *state == ServerState::HasStopped {
                rq.respond_with_err(
                    "Server is terminating/has terminated. Cannot reserve tickets.",
                );
                return;
            }
        }
        self.clear_expired_reservations();

        match rq.kind() {
            RequestKind::NumAvailableTickets => {
                let estimate = *self.last_estimate.lock().unwrap();
                rq.respond_with_int(estimate);
            }

            RequestKind::ReserveTicket => {
                // Set the server ID on the request, indicating which server is handling the
                // reservation.
                rq.set_server_id(self.id);

                // Extract the customer ID from the request.
                let customer_id = rq.customer_id();

                // Lock and access the reservations and available tickets.
                // `reservations` holds currently reserved tickets for customers.
                let mut reservations = self.reservations.lock().unwrap();
                // `available_tickets` is a queue of tickets currently available for reservation.
                let mut available_tickets = self.available_tickets.lock().unwrap();

                // Check if there is already a reservation for this customer.
                match reservations.entry(customer_id) {
                    Entry::Occupied(_) => {
                        // If the customer already has a reservation, respond with an error.
                        rq.respond_with_err("A ticket has already been reserved!");
                    }
                    Entry::Vacant(entry) => {
                        // If the customer does not have a reservation yet, attempt to allocate a
                        // ticket.

                        // Try to take a ticket from the front of the `available_tickets` queue.
                        if let Some(ticket) = available_tickets.pop_front() {
                            // Insert the new reservation for this customer with the allocated
                            // ticket.
                            entry.insert(Reservation::new(ticket));
                            // Respond to the customer with the allocated ticket ID.
                            rq.respond_with_int(ticket);
                        } else {
                            // If no tickets are available in the queue, attempt to allocate a new
                            // ticket from the database.
                            let mut db = self.database.write().unwrap();
                            // Allocate a new ticket (attempt to allocate 1 ticket).
                            if let Some(new_ticket) = db.allocate(1).pop() {
                                // Add the newly allocated ticket to the `available_tickets` queue.
                                available_tickets.push_back(new_ticket);
                                // Try to allocate a ticket again from the updated
                                // `available_tickets` queue.
                                if let Some(ticket) = available_tickets.pop_front() {
                                    // Insert the new reservation for the customer.
                                    entry.insert(Reservation::new(ticket));
                                    // Respond with the allocated ticket ID.
                                    rq.respond_with_int(ticket);
                                }
                            } else {
                                // If no tickets could be allocated from the database, respond with
                                // a sold-out message.
                                rq.respond_with_sold_out();
                            }
                        }
                    }
                }
            }

            RequestKind::BuyTicket => {
                rq.set_server_id(self.id);
                println!("Server {} handling buy ticket request", self.id);
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

                            // Return the ticket to the database
                            let mut db = self.database.write().unwrap();
                            db.deallocate(&[ticket_id]);
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

    /// Terminates the server gracefully.
    pub fn terminate(&mut self) {
        // Set server state to terminating
        {
            let mut state = self.server_state.lock().unwrap();
            *state = ServerState::Terminating;
        }
        println!("Server {} is terminating", self.id);

        // Return all non-reserved tickets
        self.return_all_non_reserved_tickets();

        // Handle incoming messages until it is safe to stop
        while !self.can_safely_stop() {
            let message = {
                let receiver = self.receiver.lock().unwrap();
                receiver.recv_timeout(Duration::from_millis(50))
            };

            if let Ok(message) = message {
                match message {
                    ServerOrRequestMessage::ServerMessage(server_message) => {
                        self.handle_server_message(server_message);
                    }
                    ServerOrRequestMessage::ClientRequest(request) => {
                        self.handle_request(request);
                    }
                }
            } else {
                std::thread::sleep(Duration::from_millis(50));
            }
        }

        // Update the server's state to `HasStopped`
        {
            let mut state = self.server_state.lock().unwrap();
            *state = ServerState::HasStopped;
        }
        println!("Server {} has been gracefully terminated", self.id);
    }

    /// Handles server shutdown.
    pub fn shutdown(&mut self) {
        let state = self.server_state.lock().unwrap();
        if *state != ServerState::Running {
            println!("Server {} is already shut down or terminating.", self.id);
            return;
        }

        println!("Shutting down server {}", self.id);
        *self.server_state.lock().unwrap() = ServerState::Terminating;
    }

    /// Updates the ticket estimate with the given value.
    fn update_ticket_estimate(&self, new_estimate: u32) {
        let mut last_estimate = self.last_estimate.lock().unwrap();
        *last_estimate = new_estimate;
        println!("Updated ticket estimate to {}", new_estimate);
    }

    /// Gets the allocated ticket count from the available tickets queue.
    fn get_allocated_ticket_count(&self) -> u32 {
        let available_tickets = self.available_tickets.lock().unwrap();
        available_tickets.len() as u32
    }

    /// Aborts and removes expired reservations.
    fn clear_expired_reservations(&mut self) {
        let mut reservations = self.reservations.lock().unwrap();
        let mut available_tickets = self.available_tickets.lock().unwrap();

        reservations.retain(|_, res| {
            if res.age_secs() > self.reservation_timeout.as_secs() {
                available_tickets.push_back(res.ticket);
                false
            } else {
                true
            }
        });
    }

    /// Returns all non-reserved tickets to the database.
    fn return_all_non_reserved_tickets(&self) {
        let mut available_tickets = self.available_tickets.lock().unwrap();
        let mut db = self.database.write().unwrap();
        while let Some(ticket) = available_tickets.pop_front() {
            db.deallocate(&[ticket]);
        }
    }

    /// Checks if the server can safely stop.
    fn can_safely_stop(&self) -> bool {
        let reservations = self.reservations.lock().unwrap();
        reservations.is_empty() && self.available_tickets.lock().unwrap().is_empty()
    }
}
