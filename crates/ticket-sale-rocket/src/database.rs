//! Implementation of the central database for tickets

/// Implementation of the central database for tickets
#[derive(Clone)]
pub struct Database {
    /// List of available tickets that have not yet been allocated by any server
    unallocated: Vec<u32>,
}

impl Database {
    /// Create a new [`Database`].
    pub fn new(num_tickets: u32) -> Self {
        let unallocated: Vec<u32> = (0..num_tickets).collect();
        Self { unallocated }
    }

    /// Get the number of available tickets.
    pub fn get_num_available(&self) -> u32 {
        self.unallocated.len() as u32
    }

    /// Allocate `num_tickets` many tickets.
    ///
    /// The tickets are removed from the database.
    pub fn allocate(&mut self, num_tickets: u32) -> Vec<u32> {
        let mut tickets = Vec::with_capacity(num_tickets as usize);

        if num_tickets >= self.unallocated.len() as u32 {
            return std::mem::take(&mut self.unallocated);
        }

        let split = self.unallocated.len() - num_tickets as usize;
        tickets.extend_from_slice(&self.unallocated[split..]);
        self.unallocated.truncate(split);
        tickets
    }

    /// Deallocate `tickets`.
    ///
    /// The tickets are added to the database.
    pub fn deallocate(&mut self, tickets: &[u32]) {
        self.unallocated.extend_from_slice(tickets);
    }
}
