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
                rq.set_server_id(self.id);
                let customer_id = rq.customer_id();
                let mut reservations = self.reservations.lock().unwrap();
                let mut available_tickets = self.available_tickets.lock().unwrap();
                match reservations.entry(customer_id) {
                    Entry::Occupied(_) => {
                        rq.respond_with_err("A ticket has already been reserved!");
                    }
                    Entry::Vacant(entry) => {
                        if let Some(ticket) = available_tickets.pop_front() {
                            entry.insert(Reservation::new(ticket));
                            rq.respond_with_int(ticket);
                        } else {
                            let mut db = self.database.write().unwrap();
                            if let Some(new_ticket) = db.allocate(1).pop() {
                                available_tickets.push_back(new_ticket);
                                if let Some(ticket) = available_tickets.pop_front() {
                                    entry.insert(Reservation::new(ticket));
                                    rq.respond_with_int(ticket);
                                }
                            } else {
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

    pub fn shutdown(&mut self) {
        println!("Server {} is shutting down", self.id);
        self.return_all_non_reserved_tickets();
        {
            let mut state = self.server_state.lock().unwrap();
            *state = ServerState::HasStopped;
        }
        println!("Server {} has been fully shut down", self.id);
    }

    fn can_safely_stop(&self) -> bool {
        let reservations_empty = {
            let reservations = self.reservations.lock().unwrap();
            reservations.is_empty()
        };

        reservations_empty
    }

    pub fn handle_request_at_termination(&mut self, mut rq: Request) {
        let mut reservations = self.reservations.lock().unwrap();

        match rq.kind() {
            RequestKind::ReserveTicket => {
                // Refuse new reservations since the server is terminating
                rq.respond_with_err("Server is terminating, unable to process new reservations.");
            }
            RequestKind::BuyTicket => {
                rq.set_server_id(self.id);
                if let Some(ticket_id) = rq.read_u32() {
                    let customer_id = rq.customer_id();
                    if let Some(reservation) = reservations.get(&customer_id) {
                        if ticket_id != reservation.ticket {
                            rq.respond_with_err("Invalid ticket id provided!");
                        } else {
                            // Complete the purchase and remove the reservation
                            reservations.remove(&customer_id);
                            rq.respond_with_int(ticket_id);

                            // Optionally reallocate a new ticket to the available pool
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
                    if let Some(reservation) = reservations.get(&customer_id) {
                        if ticket_id != reservation.ticket {
                            rq.respond_with_err("Invalid ticket id provided!");
                        } else {
                            // Abort the purchase and return the ticket to the available pool
                            reservations.remove(&customer_id);
                            let mut available_tickets = self.available_tickets.lock().unwrap();
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
            _ => {
                rq.respond_with_err("Server is terminating, unable to process the request.");
            }
        }
    }

    pub fn terminate(&mut self) {
        {
            let mut state = self.server_state.lock().unwrap();
            *state = ServerState::Terminating;
        }
        println!("Server {} is terminating", self.id);

        // (a) Return all non-reserved tickets immediately
        self.return_all_non_reserved_tickets();

        while !self.can_safely_stop() {
            self.clear_expired_reservations();
            println!("Attempting to handle messages");

            let message = {
                let receiver = self.receiver.lock().unwrap();
                receiver.recv_timeout(Duration::from_millis(50)) // Fixed non-blocking
            };

            if let Ok(message) = message {
                match message {
                    ServerOrRequestMessage::ServerMessage(server_message) => {
                        self.handle_server_message(server_message);
                    }
                    ServerOrRequestMessage::ClientRequest(request) => {
                        self.handle_request_at_termination(request);
                    }
                }
            } else {
                // Sleep for a fixed duration if no message is received
                std::thread::sleep(Duration::from_millis(50));
            }
        }

        self.return_all_non_reserved_tickets();
        {
            let mut state = self.server_state.lock().unwrap();
            *state = ServerState::HasStopped;
        }
        println!("Server {} has been gracefully terminated", self.id);
    }

    fn return_all_non_reserved_tickets(&mut self) {
        let mut available_tickets = self.available_tickets.lock().unwrap();

        // Deallocate all non-reserved tickets and return them to the central database
        if !available_tickets.is_empty() {
            let tickets_to_return: Vec<u32> = available_tickets.drain(..).collect();
            let mut db = self.database.write().unwrap();
            db.deallocate(&tickets_to_return);
            println!(
                "Server {} returned {} non-reserved tickets to the database.",
                self.id,
                tickets_to_return.len()
            );
        }
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
}
