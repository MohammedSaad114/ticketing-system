use std::collections::HashMap;
use std::sync::{atomic::AtomicBool, Arc, RwLock};

use ticket_sale_core::Config;
use uuid::Uuid;

use crate::database::Database;
use crate::estimator::Estimator;
use crate::server::Server;

/// Coordinator manages the servers and the database, handling server scaling and request
/// routing.
pub struct Coordinator {
    /// The central database shared among all servers
    database: Arc<RwLock<Database>>,

    /// Map of server IDs to their corresponding `Server` objects
    servers: RwLock<HashMap<Uuid, Arc<RwLock<Server>>>>,

    /// Flag indicating if the coordinator is running
    running: AtomicBool,

    /// Optional estimator used for scaling decisions
    estimator: Option<Arc<Estimator>>,
}

impl Coordinator {
    /// Creates a new `Coordinator`.
    ///
    /// # Arguments
    ///
    /// * `reservation_timeout` - Timeout for reservations
    /// * `database` - Shared database instance
    /// * `config` - Configuration containing initial server count and ticket settings
    ///
    /// # Returns
    ///
    /// * `Self` - New instance of `Coordinator`
    pub fn new(reservation_timeout: u32, database: Arc<RwLock<Database>>, config: &Config) -> Self {
        let mut servers = HashMap::new();

        // Initialize the servers based on the initial server count
        for _ in 0..config.initial_servers {
            let server = Arc::new(RwLock::new(Server::new(
                Arc::clone(&database),
                10,
                reservation_timeout,
            )));
            let server_id = server.read().unwrap().id().clone();
            servers.insert(server_id, server);
        }

        Self {
            database,
            servers: RwLock::new(servers),
            running: AtomicBool::new(true),
            estimator: None,
        }
    }

    /// Retrieves a list of all server IDs.
    ///
    /// # Returns
    ///
    /// * `Vec<Uuid>` - List of server IDs
    pub fn get_servers(&self) -> Vec<Uuid> {
        self.servers.read().unwrap().keys().cloned().collect()
    }

    /// Retrieves a specific server by its ID.
    ///
    /// # Arguments
    ///
    /// * `id` - ID of the server to retrieve
    ///
    /// # Returns
    ///
    /// * `Option<Arc<RwLock<Server>>>` - `Some(server)` if found, `None` otherwise
    pub fn get_server(&self, id: Uuid) -> Option<Arc<RwLock<Server>>> {
        self.servers.read().unwrap().get(&id).cloned()
    }
}
