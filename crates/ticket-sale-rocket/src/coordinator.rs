use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Mutex;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};
use std::thread::{self, JoinHandle};

use ticket_sale_core::Config;
use uuid::Uuid;

use crate::database::Database;
use crate::messages::{CoordinatorMessage, ServerMessage, ServerOrRequestMessage};
use crate::server::Server;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerState {
    Running,
    Terminating,
    HasStopped,
}

/// Coordinator manages the servers and the database, handling server scaling and request
/// routing.
type ServerMap = Arc<RwLock<HashMap<Uuid, (Sender<ServerOrRequestMessage>, JoinHandle<()>)>>>;
pub struct Coordinator {
    /// The central database shared among all servers.
    database: Arc<RwLock<Database>>,

    /// Map of server IDs to their corresponding Server objects.
    servers: ServerMap,
    /// Flag indicating if the coordinator is running.
    running: AtomicBool,

    /// Sender for communicating the updated server list to the Balancer.
    message_tx: Sender<CoordinatorMessage>,

    /// Timeout for server reservations.
    reservation_timeout: u32,
}

impl Coordinator {
    /// Creates a new Coordinator.
    ///
    /// # Arguments
    ///
    /// * config - Configuration containing initial server count and timeout settings.
    /// * database - Shared database instance.
    /// * message_tx - Sender for communicating with the Balancer.
    ///
    /// # Returns
    ///
    /// * Self - New instance of Coordinator.
    pub fn new(
        config: &Config,
        database: Arc<RwLock<Database>>,
        message_tx: Sender<CoordinatorMessage>,
    ) -> Self {
        let mut servers = HashMap::new();
        for _ in 0..config.initial_servers {
            let server = Self::spawn_server(database.clone(), config.timeout, message_tx.clone());
            let server_id = server.read().unwrap().id();
            servers.insert(server_id, server);
        }

        Self {
            database,
            servers: RwLock::new(servers).into(),
            running: AtomicBool::new(true),
            message_tx,
            reservation_timeout: config.timeout,
        }
    }

    /// Spawns a new server in a separate thread.
    ///
    /// # Arguments
    ///
    /// * database - Shared database instance.
    /// * reservation_timeout - Timeout for reservations.
    ///
    /// # Returns
    ///
    /// * (Arc<RwLock<Server>>, Sender<ServerOrRequestMessage>) - The newly spawned server
    ///   and its sender.
    fn spawn_server(
        database: Arc<RwLock<Database>>,
        reservation_timeout: u32,
        message_tx: Sender<CoordinatorMessage>, // Add message_tx as a parameter
    ) -> Arc<RwLock<Server>> {
        let server = Arc::new(RwLock::new(Server::new(
            Arc::clone(&database),
            10,
            reservation_timeout,
            message_tx.clone(), // Use the passed message_tx
        )));

        let server_clone = Arc::clone(&server);
        let handle = thread::spawn(move || {
            let mut server = server_clone.write().unwrap();
            server.handle_messages();
        });

        (server, tx, handle) // Return the JoinHandle
    }

    /// Handles the SetNumServers request by spawning or removing servers.
    ///
    /// # Arguments
    ///
    /// * num_servers - The desired number of servers.
    pub fn set_num_servers(&self, num_servers: usize) {
        let mut servers = self.servers.write().unwrap();

        match num_servers.cmp(&servers.len()) {
            // Increase the number of servers.
            std::cmp::Ordering::Greater => {
                for _ in servers.len()..num_servers {
                    let server = Self::spawn_server(
                        self.database.clone(),
                        self.reservation_timeout,
                        self.message_tx.clone(),
                    );
                    let server_id = server.read().unwrap().id();
                    servers.insert(server_id, server);
                    // Notify the Balancer of the new server
                    self.message_tx
                        .send(CoordinatorMessage::ServerUpdate(
                            server.read().unwrap().id(),
                            0,
                        ))
                        .unwrap_or_else(|e| eprintln!("Failed to send server update: {}", e));
                }
            }
            // Decrease the number of servers.
            std::cmp::Ordering::Less => {
                let server_ids: Vec<Uuid> = servers.keys().cloned().collect();
                for server_id in server_ids.iter().skip(num_servers) {
                    if let Some(server) = servers.remove(server_id) {
                        server.write().unwrap().stop(); // Trigger stop process
                        println!("Server {} stopped", server_id);
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
    /// * Vec<Uuid> - List of server IDs.
    pub fn get_servers(&self) -> Vec<Uuid> {
        self.servers.read().unwrap().keys().cloned().collect()
    }

    /// Retrieves the sender for a specific server by its ID.
    ///
    /// # Arguments
    ///
    /// * server_id - The ID of the server to retrieve the sender for.
    ///
    /// # Returns
    ///
    /// * Option<Sender<ServerOrRequestMessage>> - The sender associated with the server,
    ///   if it exists.
    pub fn get_server_sender(&self, server_id: Uuid) -> Option<Sender<ServerOrRequestMessage>> {
        self.servers
            .read()
            .unwrap()
            .get(&server_id)
            .map(|(sender, _)| sender.clone())
    }

    /// Handles incoming messages and updates the coordinator state accordingly.
    pub fn run(&self, rx: Receiver<CoordinatorMessage>) {
        while self.running.load(Ordering::SeqCst) {
            if let Ok(message) = rx.recv() {
                match message {
                    CoordinatorMessage::GetNumServers(sender) => {
                        let num_servers = self.get_servers().len() as u32;
                        sender.send(num_servers).unwrap_or_else(|e| {
                            eprintln!("Failed to send number of servers: {}", e)
                        });
                    }
                    CoordinatorMessage::SetNumServers(num, sender) => {
                        self.set_num_servers(num);
                        sender.send(num as u32).unwrap_or_else(|e| {
                            eprintln!("Failed to send set server response: {}", e)
                        });
                    }
                    CoordinatorMessage::GetServers(sender) => {
                        let server_ids = self.get_servers();
                        sender
                            .send(server_ids)
                            .unwrap_or_else(|e| eprintln!("Failed to send server list: {}", e));
                    }
                    CoordinatorMessage::IsServerTerminating(server_id, sender) => {
                        let is_terminating = self.is_server_terminating(server_id);
                        sender.send(is_terminating).unwrap_or_else(|e| {
                            eprintln!("Failed to send termination status: {}", e)
                        });
                    }
                    CoordinatorMessage::GetServerSender(server_id, sender) => {
                        if let Some(server_sender) = self.get_server_sender(server_id) {
                            sender.send(server_sender).unwrap_or_else(|e| {
                                eprintln!("Failed to send server sender: {}", e)
                            });
                        }
                    }
                    CoordinatorMessage::ServerUpdate(_, _) => {
                        // Handle the ServerUpdate case
                        // Add logic here if needed, or just ignore
                    }
                    CoordinatorMessage::Shutdown => {
                        self.shutdown();
                        break;
                    }
                }
            } else {
                eprintln!("Coordinator failed to receive message.");
            }
        }
    }

    /// Checks if the server with the given ID is terminating.
    ///
    /// # Arguments
    ///
    /// * id - ID of the server to check.
    ///
    /// # Returns
    ///
    /// * bool - true if the server is terminating, false otherwise.
    pub fn is_server_terminating(&self, id: Uuid) -> bool {
        if let Some(server) = self.get_server(id) {
            server.read().unwrap().terminating.load(Ordering::SeqCst)
        } else {
            false
        }
    }

    /// Handles incoming messages and updates the coordinator state accordingly.
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
                CoordinatorMessage::StopRequest => {
                    self.stop();
                    break;
                }
            }
        }
    }

    /// Returns the message sender for communicating with the Balancer.
    pub fn get_message_tx(&self) -> Sender<CoordinatorMessage> {
        self.message_tx.clone()
    }

    /// Shuts down the Coordinator and all managed servers.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
        println!("Coordinator is shutting down");

        let mut servers = self.servers.write().unwrap(); // Changed to mutable to allow clearing

        for (server_id, (sender, handle)) in servers.drain() {
            // Drain instead of iter to clear the map
            println!("Sending shutdown message to Server {}", server_id);
            sender
                .send(ServerOrRequestMessage::ServerMessage(
                    ServerMessage::ShutdownServer,
                ))
                .unwrap_or_else(|e| {
                    eprintln!(
                        "Failed to send shutdown message to server {}: {}",
                        server_id, e
                    );
                });
            handle.join().unwrap(); // Join the server thread after shutdown
            println!("Joined thread for Server {}", server_id);
        }

        println!("Coordinator has been fully shut down");
    }

    /// Stops the Coordinator and all managed servers immediately.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        println!("Coordinator is stopping");

        let servers = self.servers.read().unwrap();
        for (server_id, server) in servers.iter() {
            println!("Stopping server {}", server_id);
            server.write().unwrap().stop();
        }

        println!("Coordinator has been fully stopped");
    }
}
