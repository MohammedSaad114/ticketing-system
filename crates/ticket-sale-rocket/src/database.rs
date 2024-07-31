use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

// use std::time::Instant;
use uuid::Uuid;

use crate::server::TicketState;

#[derive(Clone)]
pub struct Database {
    ticket_count: Arc<AtomicUsize>,
    tickets: Arc<RwLock<HashMap<Uuid, TicketState>>>,
}

impl Database {
    pub fn new(ticket_count: usize) -> Self {
        let mut tickets = HashMap::new();
        for _ in 0..ticket_count {
            tickets.insert(Uuid::new_v4(), TicketState::Available);
        }

        Self {
            ticket_count: Arc::new(AtomicUsize::new(ticket_count)),
            tickets: Arc::new(RwLock::new(tickets)),
        }
    }

    pub fn allocate_tickets(&self, count: usize) -> Vec<Uuid> {
        let mut allocated_tickets = Vec::new();
        let mut tickets = self.tickets.write().unwrap();

        for (ticket_id, state) in tickets.iter_mut() {
            if allocated_tickets.len() >= count {
                break;
            }

            if matches!(state, TicketState::Available) {
                *state = TicketState::Reserved;
                allocated_tickets.push(*ticket_id);
            }
        }

        allocated_tickets
    }

    pub fn get_ticket_state(&self, ticket_id: Uuid) -> Option<TicketState> {
        self.tickets.read().unwrap().get(&ticket_id).cloned()
    }

    pub fn set_ticket_state(&self, ticket_id: Uuid, state: TicketState) {
        self.tickets.write().unwrap().insert(ticket_id, state);
    }

    pub fn get_ticket_count(&self) -> usize {
        self.ticket_count.load(Ordering::SeqCst)
    }

    pub fn adjust_ticket_count(&self, adjustment: isize) {
        self.ticket_count
            .fetch_add(adjustment as usize, Ordering::SeqCst);
    }

    /// Get the number of available tickets
    pub fn get_num_available(&self) -> usize {
        let tickets = self.tickets.read().unwrap();
        tickets
            .values()
            .filter(|&&state| matches!(state, TicketState::Available))
            .count()
    }
}
