use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use std::sync::Mutex;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::Sender,
    Arc, RwLock,
};
use std::thread;

use ticket_sale_core::Config;
use uuid::Uuid;

use crate::database::Database;
use crate::messages::CoordinatorMessage;
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

    /// Sender for communicating the updated server list to the Balancer.
    message_tx: Sender<CoordinatorMessage>, // Unified message channel

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
        message_tx: Sender<CoordinatorMessage>,
    ) -> Self {
        let mut servers = HashMap::new();

        // Initialize the servers based on the initial server count
        for _ in 0..config.initial_servers {
            let server = Self::spawn_server(database.clone(), config.timeout);
            let server_id = server.read().unwrap().id();
            servers.insert(server_id, server);
        }

        Self {
            database,
            servers: RwLock::new(servers),
            running: AtomicBool::new(true),

            message_tx,
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

        match num_servers.cmp(&servers.len()) {
            std::cmp::Ordering::Greater => {
                for _ in servers.len()..num_servers {
                    let server =
                        Self::spawn_server(self.database.clone(), self.reservation_timeout);
                    let server_id = server.read().unwrap().id();
                    servers.insert(server_id, server);
                }
            }
            std::cmp::Ordering::Less => {
                let server_ids: Vec<Uuid> = servers.keys().cloned().collect();
                for server_id in server_ids.iter().skip(num_servers) {
                    if let Some(server) = servers.remove(server_id) {
                        server.write().unwrap().shutdown(); // Gracefully shut down the server
                        println!("Server {} removed", server_id);
                    }
                }
            }
            _ => {}
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

    pub fn run(&self, rx: Arc<Mutex<Receiver<CoordinatorMessage>>>) {
        while let Ok(message) = rx.lock().unwrap().recv() {
            match message {
                CoordinatorMessage::GetNumServers(sender) => {
                    let num_servers = self.get_servers().len() as u32;
                    sender.send(num_servers).unwrap();
                }
                CoordinatorMessage::SetNumServers(num, sender) => {
                    self.set_num_servers(num);
                    sender.send(num as u32).unwrap();
                }
                CoordinatorMessage::GetServers(sender) => {
                    let server_ids = self.get_servers();
                    sender.send(server_ids).unwrap();
                }
                CoordinatorMessage::ServerUpdate(server_id, update_value) => {
                    // Find the server by its ID and forward the update
                    if let Some(server) = self.get_server(server_id) {
                        server.write().unwrap().update_estimate(update_value);
                    } else {
                        println!("Server with ID {} not found for update.", server_id);
                    }
                }
                CoordinatorMessage::Shutdown => {
                    self.shutdown();
                    break;
                }
            }
        }
    }

    pub fn get_message_tx(&self) -> Sender<CoordinatorMessage> {
        self.message_tx.clone()
    }

    /// Gracefully shuts down the coordinator and all its servers.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
        println!("Coordinator is shutting down");

        let servers = self.servers.read().unwrap();
        for (server_id, server) in servers.iter() {
            println!("Shutting down server {}", server_id);
            server.write().unwrap().shutdown();
        }

        println!("Coordinator has been fully shut down");
    }
}
