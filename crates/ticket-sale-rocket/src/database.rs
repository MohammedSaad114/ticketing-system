/// Implementation of the central database for tickets
#[derive(Clone)]
pub struct Database {
    /// List of available tickets that have not yet been allocated by any server
    unallocated: Vec<u32>,
}

impl Database {
    /// Create a new [`Database`].
    ///
    /// Initializes the database with `num_tickets` tickets, all of which are unallocated.
    pub fn new(num_tickets: u32) -> Self {
        // Create a vector of ticket IDs from 0 to num_tickets - 1
        let unallocated: Vec<u32> = (0..num_tickets).collect();
        Self { unallocated }
    }

    /// Get the number of available tickets.
    ///
    /// Returns the total number of tickets that are currently unallocated.
    pub fn get_num_available(&self) -> u32 {
        self.unallocated.len() as u32
    }

    /// Allocate `num_tickets` many tickets.
    ///
    /// The tickets are removed from the database and returned to the caller.
    ///
    /// # Parameters
    /// - `num_tickets`: The number of tickets to allocate.
    ///
    /// # Returns
    /// A vector containing the IDs of the allocated tickets.
    pub fn allocate(&mut self, num_tickets: u32) -> Vec<u32> {
        let mut tickets = Vec::with_capacity(num_tickets as usize);

        // If requesting more tickets than available, return all available tickets
        if num_tickets >= self.unallocated.len() as u32 {
            return std::mem::take(&mut self.unallocated);
        }

        // Calculate the split point to get the last `num_tickets` tickets
        let split = self.unallocated.len() - num_tickets as usize;
        // Extend the `tickets` vector with the last `num_tickets` tickets from `unallocated`
        tickets.extend_from_slice(&self.unallocated[split..]);
        // Truncate the `unallocated` vector to remove the allocated tickets
        self.unallocated.truncate(split);
        tickets
    }

    /// Deallocate `tickets`.
    ///
    /// The provided tickets are added back to the pool of available tickets.
    ///
    /// # Parameters
    /// - `tickets`: A slice of ticket IDs to be added back to the database.
    pub fn deallocate(&mut self, tickets: &[u32]) {
        // Extend the `unallocated` vector with the given tickets
        self.unallocated.extend_from_slice(tickets);
    }
}
