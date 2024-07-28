use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};

use ticket_sale_core::{Config, Request};
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
    database: Arc<RwLock<Database>>,

    /// List of active servers
    servers: Vec<Arc<RwLock<Server>>>,

    /// Flag indicating if the coordinator is running
    running: AtomicBool,
}

impl Coordinator {
    /// Create the [`Coordinator`]
    pub fn new(reservation_timeout: u32, database: Database, config: &Config) -> Self {
        let database = Arc::new(RwLock::new(database));
        let config = Config {
            tickets: config.tickets,
            timeout: reservation_timeout,
            initial_servers: config.initial_servers, // Assuming a default number of initial servers
            estimator_roundtrip_time: config.estimator_roundtrip_time, /* Assuming a default                                        * estimator */
            bonus: config.bonus,
        };

        let mut servers = Vec::new();

        // Initialize the initial set of servers
        for _ in 0..config.initial_servers {
            servers.push(Arc::new(RwLock::new(Server::new(
                Arc::clone(&database),
                &config,
            ))));
        }

        Self {
            reservation_timeout,
            database,
            servers,
            running: AtomicBool::new(false),
        }
    }

    /// Get a list of active servers
    pub fn get_servers(&self) -> Vec<Uuid> {
        self.servers
            .iter()
            .map(|server| server.read().unwrap().id)
            .collect()
    }

    /// Add a new server to the list of active servers
    pub fn add_server(&mut self, config: &Config) {
        let new_server = Arc::new(RwLock::new(Server::new(Arc::clone(&self.database), config)));
        self.servers.push(new_server);
    }

    pub fn get_server_by_id(&self, server_id: Uuid) -> Option<Arc<RwLock<Server>>> {
        self.servers
            .iter()
            .find(|server| server.read().unwrap().id == server_id)
            .cloned()
    }

    /// Handle a request by forwarding it to an appropriate server
    pub fn handle_request(&self, rq: Request) {
        // For simplicity, forward the request to the first server in the list
        if let Some(server) = self.servers.first() {
            server.write().unwrap().handle_request(rq);
        } else {
            rq.respond_with_err("No servers available.");
        }
    }

    pub fn running(&self) -> &AtomicBool {
        &self.running
    }
}
