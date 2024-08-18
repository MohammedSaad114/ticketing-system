use std::collections::HashMap;
use std::sync::{atomic::AtomicBool, mpsc::Sender, Arc, RwLock};
use std::thread;

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

    /// Sender for communicating the updated server list to the Balancer.
    server_update_tx: Sender<Vec<Uuid>>,

    /// Timeout for server reservations.
    reservation_timeout: u32,
}

impl Coordinator {
    /// Creates a new `Coordinator`.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration containing initial server count and timeout settings
    /// * `database` - Shared database instance
    /// * `server_update_tx` - Sender for communicating with the Balancer.
    ///
    /// # Returns
    ///
    /// * `Self` - New instance of `Coordinator`
    pub fn new(
        config: &Config,
        database: Arc<RwLock<Database>>,
        server_update_tx: Sender<Vec<Uuid>>,
    ) -> Self {
        let mut servers = HashMap::new();

        // Initialize the servers based on the initial server count
        for _ in 0..config.initial_servers {
            let server = Self::spawn_server(database.clone(), config.timeout);
            let server_id = server.read().unwrap().id().clone();
            servers.insert(server_id, server);
        }

        Self {
            database,
            servers: RwLock::new(servers),
            running: AtomicBool::new(true),
            estimator: None,
            server_update_tx,
            reservation_timeout: config.timeout,
        }
    }

    /// Spawns a new server in a separate thread.
    ///
    /// # Arguments
    ///
    /// * `database` - Shared database instance.
    /// * `reservation_timeout` - Timeout for reservations.
    ///
    /// # Returns
    ///
    /// * `Arc<RwLock<Server>>` - The newly spawned server.
    fn spawn_server(
        database: Arc<RwLock<Database>>,
        reservation_timeout: u32,
    ) -> Arc<RwLock<Server>> {
        let server = Arc::new(RwLock::new(Server::new(
            Arc::clone(&database),
            10, // Example setting, can be modified based on needs
            reservation_timeout,
        )));
        let server_clone = Arc::clone(&server);

        // Spawn a new thread to handle the server
        thread::spawn(move || {
            let server_id = server_clone.read().unwrap().id();
            println!("Server {} started", server_id);
            // Here, the server could enter its main loop, e.g., waiting for and handling
            // requests. This is just a placeholder for actual server logic.
        });

        server
    }

    /// Handles the `SetNumServers` request by spawning or removing servers.
    ///
    /// # Arguments
    ///
    /// * `num_servers` - The desired number of servers.
    pub fn set_num_servers(&self, num_servers: usize) {
        let mut servers = self.servers.write().unwrap();

        if num_servers > servers.len() {
            // Add new servers if the desired number is greater than the current count.
            for _ in servers.len()..num_servers {
                let server = Self::spawn_server(self.database.clone(), self.reservation_timeout);
                let server_id = server.read().unwrap().id().clone();
                servers.insert(server_id, server);
            }
        } else if num_servers < servers.len() {
            // Remove excess servers if the desired number is less than the current count.
            let server_ids: Vec<Uuid> = servers.keys().cloned().collect();
            for server_id in server_ids.iter().skip(num_servers) {
                servers.remove(server_id);
                println!("Server {} removed", server_id);
                // Properly shut down the server if needed (e.g., sending a shutdown
                // signal)
            }
        }

        // Send the updated list of server IDs to the Balancer.
        let server_ids = servers.keys().cloned().collect();
        self.server_update_tx
            .send(server_ids)
            .expect("Failed to send server update");
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
