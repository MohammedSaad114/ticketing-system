use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, RwLock,
};
use std::time::{Duration, Instant};

use ticket_sale_core::{Request, RequestKind};
use uuid::Uuid;

use super::database::Database;

/// Represents a single ticket
#[derive(Clone, Copy, Debug)]
pub enum TicketState {
    Available,
    Reserved,
    Sold,
}

/// Represents a server that handles ticket sales
pub struct Server {
    id: Uuid,
    database: Arc<RwLock<Database>>,
    tickets: Mutex<VecDeque<Uuid>>,
    reservations: Mutex<HashMap<Uuid, (Uuid, Instant)>>,
    reservation_timeout: Duration,
    last_estimate: Mutex<u32>, // Field to store the last estimate received from the estimator
    running: AtomicBool,       // Added field to control the running state
}

impl Server {
    pub fn new(
        database: Arc<RwLock<Database>>,
        ticket_count: u32,
        reservation_timeout: u32,
    ) -> Self {
        let id = Uuid::new_v4();
        let tickets = {
            let db = database.write().unwrap();
            let allocated_tickets = db.allocate_tickets(ticket_count as usize);
            Mutex::new(VecDeque::from(allocated_tickets))
        };

        Self {
            id,
            database,
            tickets,
            reservations: Mutex::new(HashMap::new()),
            reservation_timeout: Duration::from_secs(reservation_timeout.into()),
            last_estimate: Mutex::new(0), // Initialize the last estimate
            running: AtomicBool::new(true), // Initialize running state
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn handle_request(&self, rq: Request) {
        if !self.running.load(Ordering::SeqCst) {
            rq.respond_with_err("Server is shutting down.");
            return;
        }

        match rq.kind() {
            RequestKind::ReserveTicket => self.handle_reserve_ticket(rq),
            RequestKind::BuyTicket => self.handle_buy_ticket(rq),
            RequestKind::AbortPurchase => self.handle_abort_purchase(rq),
            RequestKind::NumAvailableTickets => self.handle_num_available_tickets(rq),
            _ => rq.respond_with_err("Unsupported request kind."),
        }
    }

    fn handle_reserve_ticket(&self, rq: Request) {
        if !self.running.load(Ordering::SeqCst) {
            rq.respond_with_err("Server is shutting down.");
            return;
        }

        let customer_id = rq.customer_id();
        let now = Instant::now();

        let mut reservations = self.reservations.lock().unwrap();
        let tickets = self.tickets.lock().unwrap();

        // Expire old reservations
        reservations.retain(|_, &mut (_, timestamp)| {
            now.duration_since(timestamp) < self.reservation_timeout
        });

        // Check if the customer already has a reservation
        if let Some(&(ticket_id, _)) = reservations.get(&customer_id) {
            rq.respond_with_int(ticket_id.as_u128() as u32);
            return;
        }

        // Find an available ticket
        if let Some(&ticket_id) = tickets.front() {
            reservations.insert(customer_id, (ticket_id, now));
            rq.respond_with_int(ticket_id.as_u128() as u32);
        } else {
            rq.respond_with_sold_out();
        }
    }

    fn handle_buy_ticket(&self, mut rq: Request) {
        if !self.running.load(Ordering::SeqCst) {
            rq.respond_with_err("Server is shutting down.");
            return;
        }

        let customer_id = rq.customer_id();
        let ticket_id = Uuid::from_u128(rq.read_u32().unwrap_or_default() as u128);

        let mut reservations = self.reservations.lock().unwrap();

        if let Some(&(reserved_ticket_id, _)) = reservations.get(&customer_id) {
            if reserved_ticket_id == ticket_id {
                let db = self.database.write().unwrap();
                db.set_ticket_state(ticket_id, TicketState::Sold);
                reservations.remove(&customer_id);
                rq.respond_with_int(ticket_id.as_u128() as u32);
            } else {
                rq.respond_with_err("Ticket ID mismatch.");
            }
        } else {
            rq.respond_with_err("No reservation found.");
        }
    }

    fn handle_abort_purchase(&self, mut rq: Request) {
        if !self.running.load(Ordering::SeqCst) {
            rq.respond_with_err("Server is shutting down.");
            return;
        }

        let customer_id = rq.customer_id();
        let ticket_id = Uuid::from_u128(rq.read_u32().unwrap_or_default() as u128);

        let mut reservations = self.reservations.lock().unwrap();
        let mut tickets = self.tickets.lock().unwrap();

        if let Some(&(reserved_ticket_id, _)) = reservations.get(&customer_id) {
            if reserved_ticket_id == ticket_id {
                reservations.remove(&customer_id);
                tickets.push_back(ticket_id);
                rq.respond_with_int(ticket_id.as_u128() as u32);
            } else {
                rq.respond_with_err("Ticket ID mismatch.");
            }
        } else {
            rq.respond_with_err("No reservation found.");
        }
    }

    fn handle_num_available_tickets(&self, rq: Request) {
        if !self.running.load(Ordering::SeqCst) {
            rq.respond_with_err("Server is shutting down.");
            return;
        }

        let tickets = self.tickets.lock().unwrap();
        let estimate = self.last_estimate.lock().unwrap();
        let available_tickets = tickets.len() as u32 + *estimate;
        rq.respond_with_int(available_tickets);
    }

    /// Get the current number of allocated tickets.
    pub fn get_allocated_ticket_count(&self) -> u32 {
        let tickets = self.tickets.lock().unwrap();
        tickets.len() as u32
    }

    /// Update the estimate with the new value.
    pub fn update_estimate(&self, new_estimate: u32) {
        let mut last_estimate = self.last_estimate.lock().unwrap();
        *last_estimate = new_estimate;
    }

    /// Shut down the server, stopping it from processing new requests.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
        println!("Server {} is shutting down", self.id);
    }
}
