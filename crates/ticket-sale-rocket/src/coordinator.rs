//! Implementation of the coordinator

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use super::database::Database;
use super::server::Server;

/// Coordinator orchestrating all the components of the system
pub struct Coordinator {
    /// The reservation timeout
    reservation_timeout: u32,

    /// Reference to the [`Database`]
    ///
    /// To be handed over to new servers.
    database: Arc<Mutex<Database>>,

    /// Map of active servers
    servers: Arc<Mutex<HashMap<Uuid, Server>>>,
}

impl Coordinator {
    /// Create the [`Coordinator`]
    pub fn new(reservation_timeout: u32, database: Arc<Mutex<Database>>) -> Self {
        Self {
            reservation_timeout,
            database,
            servers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get the list of active servers
    pub fn get_servers(&self) -> Vec<Uuid> {
        self.servers.lock().unwrap().keys().cloned().collect()
    }

    /// Add a new server to the coordinator
    pub fn add_server(&self, initial_tickets: u32) {
        let db = Arc::clone(&self.database);
        let server = Server::new(db, initial_tickets, self.reservation_timeout);
        let id = server.get_id();
        self.servers.lock().unwrap().insert(id, server);
    }

    /// Remove a server from the coordinator
    pub fn remove_server(&self, server_id: Uuid) {
        self.servers.lock().unwrap().remove(&server_id);
    }
}
