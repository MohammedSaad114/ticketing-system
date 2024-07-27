//! Implementation of the coordinator
//! coordinator.rs

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use ticket_sale_core::Request;
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
    servers: Arc<Mutex<HashMap<Uuid, Arc<Mutex<Server>>>>>,
    running: Arc<AtomicBool>,
}

impl Coordinator {
    /// Create the [`Coordinator`]
    pub fn new(reservation_timeout: u32, database: Arc<Mutex<Database>>) -> Self {
        Self {
            reservation_timeout,
            database,
            servers: Arc::new(Mutex::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn add_server(&self, initial_allocation: u32) -> Uuid {
        let server = Arc::new(Mutex::new(Server::new(
            self.database.clone(),
            initial_allocation,
            self.reservation_timeout,
        )));
        let id = server.lock().unwrap().get_id();
        self.servers.lock().unwrap().insert(id, server);
        id
    }

    pub fn remove_server(&self, id: Uuid) {
        let server = {
            let mut servers = self.servers.lock().unwrap();
            servers.remove(&id)
        };
        if let Some(server) = server {
            server.lock().unwrap().terminate();
        }
    }

    pub fn get_servers(&self) -> Vec<Uuid> {
        self.servers.lock().unwrap().keys().cloned().collect()
    }

    pub fn get_server(&self, id: Uuid) -> Option<Arc<Mutex<Server>>> {
        self.servers.lock().unwrap().get(&id).cloned()
    }

    pub fn handle_request(&self, rq: Request) {
        if let Some(server_id) = rq.server_id() {
            if let Some(server) = self.get_server(server_id) {
                server.lock().unwrap().process_request(rq);
            } else {
                rq.respond_with_err("Server not found");
            }
        } else {
            rq.respond_with_err("No server ID provided");
        }
    }

    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
        let servers = self.get_servers();
        for id in servers {
            if let Some(server) = self.get_server(id) {
                server.lock().unwrap().terminate();
            }
        }
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn running(&self) -> Arc<AtomicBool> {
        self.running.clone()
    }
}
