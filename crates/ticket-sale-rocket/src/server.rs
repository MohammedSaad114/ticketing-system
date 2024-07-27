//! Implementation of the server
//! server.rs

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use ticket_sale_core::{Request, RequestKind};
use uuid::Uuid;

use super::database::Database;

/// A server in the ticket sales system
pub struct Server {
    /// The server's ID
    id: Uuid,

    /// The database
    database: Arc<Mutex<Database>>,

    allocated_tickets: Vec<u32>,
    reservations: HashMap<Uuid, (u32, SystemTime)>,
    reservation_timeout: u32,
    terminating: bool,
    estimated_tickets: u32,
}

impl Server {
    pub fn new(
        database: Arc<Mutex<Database>>,
        initial_allocation: u32,
        reservation_timeout: u32,
    ) -> Server {
        let id = Uuid::new_v4();
        let allocated_tickets = database.lock().unwrap().allocate(initial_allocation);
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

    pub fn get_id(&self) -> Uuid {
        self.id
    }

    pub fn is_terminating(&self) -> bool {
        self.terminating
    }

    pub fn terminate(&mut self) {
        self.terminating = true;
        self.database
            .lock()
            .unwrap()
            .deallocate(&self.allocated_tickets);
        self.allocated_tickets.clear();
    }

    pub fn can_shutdown(&self) -> bool {
        self.reservations.is_empty()
    }

    pub fn process_request(&mut self, rq: Request) {
        self.handle_request(rq);
    }

    pub fn update_estimate(&mut self, db_available: u32) {
        self.estimated_tickets = db_available + self.allocated_tickets.len() as u32;
    }

    fn handle_request(&mut self, rq: Request) {
        self.clean_expired_reservations();

        if self.terminating {
            match rq.kind() {
                RequestKind::BuyTicket | RequestKind::AbortPurchase => {
                    self.handle_reservation_request(rq);
                }
                _ => {
                    rq.respond_with_err("Server is terminating");
                }
            }
        } else {
            match rq.kind() {
                RequestKind::NumAvailableTickets => {
                    let available_tickets = self.allocated_tickets.len() as u32;
                    rq.respond_with_int(available_tickets + self.estimated_tickets);
                }
                RequestKind::ReserveTicket => {
                    if let Some(ticket) = self.allocated_tickets.pop() {
                        let now = SystemTime::now();
                        self.reservations.insert(rq.customer_id(), (ticket, now));
                        rq.respond_with_int(ticket);
                    } else {
                        let mut allocated_tickets = {
                            let mut db = self.database.lock().unwrap();
                            db.allocate(10)
                        };
                        self.allocated_tickets.append(&mut allocated_tickets);
                        self.handle_request(rq);
                    }
                }
                RequestKind::BuyTicket | RequestKind::AbortPurchase => {
                    self.handle_reservation_request(rq);
                }
                RequestKind::Debug => {
                    rq.respond_with_string("Debugging request handled");
                }
                _ => {
                    rq.respond_with_err("Invalid request for server");
                }
            }
        }
    }

    fn handle_reservation_request(&mut self, rq: Request) {
        if let Some((ticket, reservation_time)) = self.reservations.remove(&rq.customer_id()) {
            if rq.kind() == &RequestKind::BuyTicket {
                if reservation_time.elapsed().unwrap_or_default().as_secs()
                    > self.reservation_timeout as u64
                {
                    rq.respond_with_err("Reservation expired");
                    self.allocated_tickets.push(ticket);
                } else {
                    rq.respond_with_int(ticket);
                }
            } else if rq.kind() == &RequestKind::AbortPurchase {
                self.database.lock().unwrap().deallocate(&[ticket]);
                rq.respond_with_int(ticket);
            }
        } else {
            rq.respond_with_err("No reservation found");
        }
    }

    fn clean_expired_reservations(&mut self) {
        let now = SystemTime::now();
        let expired: Vec<Uuid> = self
            .reservations
            .iter()
            .filter(|(_, &(_, time))| {
                now.duration_since(time).unwrap_or_default().as_secs()
                    > self.reservation_timeout as u64
            })
            .map(|(&id, _)| id)
            .collect();

        for id in expired {
            if let Some((ticket, _)) = self.reservations.remove(&id) {
                self.allocated_tickets.push(ticket);
            }
        }
    }
}
