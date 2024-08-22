use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Mutex;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

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
type ServerMap = Arc<
    RwLock<
        HashMap<
            Uuid,
            (
                Sender<ServerOrRequestMessage>,
                Arc<Mutex<ServerState>>,
                JoinHandle<()>,
            ),
        >,
    >,
>;

type TerminatingServersHandles = Arc<RwLock<Vec<(Uuid, Option<JoinHandle<()>>)>>>;
pub struct Coordinator {
    /// The central database shared among all servers.
    database: Arc<RwLock<Database>>,

    /// Map of server IDs to their corresponding Server objects.
    servers: ServerMap,
    /// Flag indicating if the coordinator is running.
    running: AtomicBool,
    terminating_handles: TerminatingServersHandles,
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
            let (server, sender, server_state, handle) =
                Self::spawn_server(database.clone(), config.timeout);
            let server_id = server.read().unwrap().id();
            servers.insert(server_id, (sender, server_state, handle));
        }

        Self {
            database,
            servers: RwLock::new(servers).into(),
            running: AtomicBool::new(true),
            message_tx,
            reservation_timeout: config.timeout,
            terminating_handles: Arc::new(RwLock::new(Vec::new())),
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
    ) -> (
        Arc<RwLock<Server>>,
        Sender<ServerOrRequestMessage>,
        Arc<Mutex<ServerState>>,
        JoinHandle<()>,
    ) {
        let (tx, rx) = mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));
        let server_state = Arc::new(Mutex::new(ServerState::Running));

        let server: Arc<RwLock<Server>> = Arc::new(RwLock::new(Server::new(
            database,
            5,
            reservation_timeout,
            rx,
            server_state.clone(),
        )));

        let server_clone = Arc::clone(&server);
        let handle = thread::spawn(move || {
            let mut server = server_clone.write().unwrap();
            server.run();
        });

        (server, tx, server_state, handle)
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
                    let (server, sender, server_state, handle) =
                        Self::spawn_server(self.database.clone(), self.reservation_timeout);
                    servers.insert(server.read().unwrap().id(), (sender, server_state, handle));
                }
            }
            // Decrease the number of servers.
            std::cmp::Ordering::Less => {
                // Collect the server IDs from the `servers` map into a vector.
                let server_ids: Vec<Uuid> = servers.keys().cloned().collect();

                // Iterate over the server IDs that need to be terminated, starting from
                // `num_servers`.
                for server_id in server_ids.iter().skip(num_servers) {
                    // Retrieve the server's components: sender, state, and handle, without removing
                    // from map.
                    if let Some((sender, server_state, handle)) = servers.remove(server_id) {
                        // Set the server's state to `Terminating` to indicate that it is shutting
                        // down.
                        {
                            let mut state = server_state.lock().unwrap();
                            *state = ServerState::Terminating;
                        }

                        // Send a termination message to the server to initiate the shutdown
                        // process.
                        sender
                            .send(ServerOrRequestMessage::ServerMessage(
                                ServerMessage::TerminateServer,
                            ))
                            .unwrap_or_else(|e| {
                                eprintln!(
                                    "Failed to send shutdown message to server {}: {}",
                                    server_id, e
                                );
                            });

                        // Record the handle of the terminating server to keep track of it.
                        let mut terminating_handles = self.terminating_handles.write().unwrap();
                        terminating_handles.push((*server_id, Some(handle))); // Move the handle
                    }
                }
                // Clean up terminated servers after adjusting the number of servers
                self.cleanup_terminated_servers();
            }

            _ => {}
        }
    }

    /// Retrieves a list of all server IDs of servers that are running.
    ///
    /// # Returns
    ///
    /// * Vec<Uuid> - List of server IDs.
    pub fn get_running_servers(&self) -> Vec<Uuid> {
        self.servers
            .read()
            .unwrap()
            .iter()
            .filter_map(|(server_id, (_, server_state, _))| {
                if let ServerState::Running = *server_state.lock().unwrap() {
                    Some(*server_id)
                } else {
                    None
                }
            })
            .collect()
    }

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
            .map(|(sender, _, _)| sender.clone())
    }
    /// Handles incoming messages and updates the coordinator state accordingly.
    pub fn run(&self, rx: Receiver<CoordinatorMessage>) {
        while self.running.load(Ordering::SeqCst) {
            // Now process the incoming messages
            if let Ok(message) = rx.recv_timeout(Duration::from_millis(100)) {
                // Add a small timeout to avoid blocking indefinitely
                match message {
                    CoordinatorMessage::GetNumServers(sender) => {
                        let num_servers = self.get_running_servers().len() as u32;
                        sender.send(num_servers).unwrap_or_else(|e| {
                            eprintln!("Failed to send number of servers: {}", e)
                        });
                    }
                    CoordinatorMessage::SetNumServers(num, sender) => {
                        self.set_num_servers(num);
                        let actual_num_servers = self.get_running_servers().len() as u32;
                        sender.send(actual_num_servers).unwrap_or_else(|e| {
                            eprintln!("Failed to send set server response: {}", e)
                        });
                    }
                    CoordinatorMessage::GetServers(sender) => {
                        let server_ids = self.get_running_servers();
                        sender
                            .send(server_ids)
                            .unwrap_or_else(|e| eprintln!("Failed to send server list: {}", e));
                    }
                    CoordinatorMessage::GetServerSender(server_id, sender) => {
                        if let Some(server_sender) = self.get_server_sender(server_id) {
                            sender.send(server_sender).unwrap_or_else(|e| {
                                eprintln!("Failed to send server sender: {}", e)
                            });
                        }
                    }
                    CoordinatorMessage::Shutdown => {
                        self.shutdown();
                        break;
                    }
                }
            } else {
            }
        }
    }

    fn cleanup_terminated_servers(&self) {
        let mut servers = self.servers.write().unwrap();
        let server_ids: Vec<Uuid> = servers.keys().cloned().collect();

        for server_id in server_ids {
            if let Some((_, server_state, _)) = servers.get(&server_id) {
                if *server_state.lock().unwrap() == ServerState::HasStopped {
                    // If the server has stopped, remove it from the servers map
                    servers.remove(&server_id);
                    println!(
                        "Server {} has fully terminated and been removed.",
                        server_id
                    );
                }
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
        self.servers.read().unwrap().get(&id).is_none()
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

        for (server_id, (sender, _, handle)) in servers.drain() {
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
}
