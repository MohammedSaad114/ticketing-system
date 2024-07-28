use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use ticket_sale_core::{Config, Request, RequestKind};
use uuid::Uuid;

use super::database::Database;

/// A server in the ticket sales system
pub struct Server {
    /// The server's ID
    id: Uuid,

    /// The database
    database: Arc<RwLock<Database>>,

    /// Allocated tickets held by the server
    allocated_tickets: Vec<u32>,

    /// Reservations mapping customer ID to ticket and reservation time
    reservations: HashMap<Uuid, (u32, SystemTime)>,

    /// Reservation timeout in seconds
    reservation_timeout: u32,

    /// Flag indicating if the server is terminating
    terminating: bool,

    /// Estimated tickets available elsewhere
    estimated_tickets: u32,
}

impl Server {
    /// Create a new [`Server`]
    pub fn new(database: Arc<RwLock<Database>>, config: &Config) -> Server {
        let id = Uuid::new_v4();
        let initial_allocation = 10; // Arbitrary initial allocation, can be adjusted as needed
        let reservation_timeout = config.timeout;
        let allocated_tickets = database.write().unwrap().allocate(initial_allocation);
        Self {
            id,
            database,
            allocated_tickets,
            reservations: HashMap::new(),
            reservation_timeout,
            terminating: false,
            estimated_tickets: 0,
        }
    }

    /// Handle a [`Request`]
    fn handle_request(&mut self, rq: Request) {
        self.clean_expired_reservations();
        match rq.kind() {
            RequestKind::NumAvailableTickets => {
                let available_tickets =
                    self.allocated_tickets.len() as u32 + self.estimated_tickets;
                rq.respond_with_int(available_tickets);
            }
            RequestKind::ReserveTicket => {
                if self.allocated_tickets.is_empty() {
                    // Try to allocate more tickets
                    let new_tickets = self.database.write().unwrap().allocate(100); // Adjust as needed
                    if new_tickets.is_empty() {
                        rq.respond_with_sold_out();
                        return;
                    }
                    self.allocated_tickets.extend(new_tickets);
                }
                let ticket = self.allocated_tickets.pop().unwrap();
                self.reservations
                    .insert(rq.customer_id(), (ticket, SystemTime::now()));
                rq.respond_with_int(ticket);
            }
            RequestKind::BuyTicket => {
                if let Some((ticket, _)) = self.reservations.remove(&rq.customer_id()) {
                    rq.respond_with_int(ticket);
                } else {
                    rq.respond_with_err("No reservation found.");
                }
            }
            RequestKind::AbortPurchase => {
                if let Some((ticket, _)) = self.reservations.remove(&rq.customer_id()) {
                    self.allocated_tickets.push(ticket);
                    rq.respond_with_int(ticket);
                } else {
                    rq.respond_with_err("No reservation found.");
                }
            }
            _ => rq.respond_with_err("Unsupported request kind."),
        }
    }

    /// Clean expired reservations
    fn clean_expired_reservations(&mut self) {
        let now = SystemTime::now();
        let timeout = Duration::new(self.reservation_timeout.into(), 0);
        let mut expired_tickets = Vec::new();

        self.reservations.retain(|_, &mut (ticket, reserved_at)| {
            if now
                .duration_since(reserved_at)
                .unwrap_or_else(|_| Duration::new(0, 0))
                < timeout
            {
                true
            } else {
                expired_tickets.push(ticket);
                false
            }
        });

        self.allocated_tickets.extend(expired_tickets);
    }

    /// Get the current allocated tickets
    pub fn get_allocated_tickets(&self) -> &[u32] {
        &self.allocated_tickets
    }

    /// Update the estimated number of available tickets
    pub fn update_estimate(&mut self, db_available: u32) {
        self.estimated_tickets = db_available + self.allocated_tickets.len() as u32;
    }

    /// Handle requests when the server is terminating
    fn handle_terminating_request(&mut self, rq: Request) {
        match rq.kind() {
            RequestKind::ReserveTicket => {
                rq.respond_with_err("Server is terminating. Please try another server.");
            }
            _ => self.handle_request(rq),
        }
    }
}
