// server.rs
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use ticket_sale_core::{Config, Request, RequestKind};
use uuid::Uuid;

use super::database::Database;

/// A server in the ticket sales system
pub struct Server {
    /// The server's ID
    pub id: Uuid,

    /// The database
    database: Arc<RwLock<Database>>,

    /// Allocated tickets held by the server
    allocated_tickets: Vec<u32>,

    /// Reservations mapping customer ID to ticket and reservation time
    reservations: HashMap<Uuid, (u32, SystemTime)>,

    /// Reservation timeout in seconds
    reservation_timeout: u32,

    /// Estimated tickets available elsewhere
    estimated_tickets: u32,
}

impl Server {
    /// Create a new [`Server`]
    pub fn new(database: Arc<RwLock<Database>>, config: &Config) -> Server {
        let id = Uuid::new_v4();
        let initial_allocation = 20; // Arbitrary initial allocation, can be adjusted as needed
        let reservation_timeout = config.timeout;
        let allocated_tickets = {
            let mut db = database.write().unwrap();
            db.allocate(initial_allocation)
        };
        Self {
            id,
            database,
            allocated_tickets,
            reservations: HashMap::new(),
            reservation_timeout,
            estimated_tickets: 0,
        }
    }

    /// Handle a [`Request`]
    pub fn handle_request(&mut self, rq: Request) {
        self.clean_expired_reservations();
        match rq.kind() {
            RequestKind::NumAvailableTickets => {
                let available_tickets =
                    self.allocated_tickets.len() as u32 + self.estimated_tickets;
                rq.respond_with_int(available_tickets);
            }
            RequestKind::ReserveTicket => {
                self.reserve_ticket(rq);
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

    /// Reserve a ticket for a customer
    fn reserve_ticket(&mut self, rq: Request) {
        if self.allocated_tickets.is_empty() {
            // Try to allocate more tickets
            let new_tickets = {
                let mut db = self.database.write().unwrap();
                db.allocate(100) // Adjust as needed
            };
            if new_tickets.is_empty() {
                rq.respond_with_sold_out();
                return;
            }
            self.allocated_tickets.extend(new_tickets);
        }
        if let Some(ticket) = self.allocated_tickets.pop() {
            self.reservations
                .insert(rq.customer_id(), (ticket, SystemTime::now()));
            rq.respond_with_int(ticket);
        } else {
            rq.respond_with_err("Failed to allocate tickets.");
        }
    }

    pub fn get_allocated_tickets(&self) -> Vec<u32> {
        self.allocated_tickets.clone()
    }

    /// Update the estimated number of available tickets
    pub fn update_estimate(&mut self, db_available: u32) {
        self.estimated_tickets = db_available + self.allocated_tickets.len() as u32;
    }
}
