//! Implementation of the coordinator

use super::database::Database;

/// Coordinator orchestrating all the components of the system
pub struct Coordinator {
    /// The reservation timeout
    reservation_timeout: u32,

    /// Reference to the [`Database`]
    ///
    /// To be handed over to new servers.
    database: Database,
}

impl Coordinator {
    /// Create the [`Coordinator`]
    pub fn new(reservation_timeout: u32, database: Database) -> Self {
        Self {
            reservation_timeout,
            database,
        }
    }
}
