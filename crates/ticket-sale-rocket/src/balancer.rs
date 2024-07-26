//! Implementation of the load balancer

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ticket_sale_core::{Request, RequestHandler, RequestKind};
use uuid::Uuid;

use super::database::Database;
use super::server::Server;

/// Implementation of the load balancer
///
/// ⚠️ This struct must implement the [`RequestHandler`] trait, and it must be
/// exposed from the crate root (to be used from the tester as
/// `ticket_sale_rocket::Balancer`).
pub struct Balancer {
    servers: Arc<Mutex<HashMap<Uuid, Server>>>,
    database: Arc<Mutex<Database>>,
    initial_tickets_per_server: u32,
    reservation_timeout: u32,
    customer_to_server: Arc<Mutex<HashMap<Uuid, Uuid>>>,
}

impl Balancer {
    /// Create a new [`Balancer`]
    pub fn new(
        database: Arc<Mutex<Database>>,
        initial_tickets_per_server: u32,
        reservation_timeout: u32,
    ) -> Self {
        Self {
            servers: Arc::new(Mutex::new(HashMap::new())),
            database,
            initial_tickets_per_server,
            reservation_timeout,
            customer_to_server: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn allocate_server(&self) -> Uuid {
        let db = Arc::clone(&self.database);
        let server = Server::new(
            db,
            self.initial_tickets_per_server,
            self.reservation_timeout,
        );
        let id = server.get_id();
        self.servers.lock().unwrap().insert(id, server);
        id
    }

    fn terminate_server(&self, server_id: Uuid) {
        let mut servers = self.servers.lock().unwrap();
        if let Some(server) = servers.get_mut(&server_id) {
            server.terminate();
        }
    }

    fn get_num_servers(&self) -> u32 {
        self.servers.lock().unwrap().len() as u32
    }

    fn set_num_servers(&self, num_servers: u32) {
        let mut servers = self.servers.lock().unwrap();
        let current_num_servers = servers.len() as u32;

        if num_servers > current_num_servers {
            for _ in 0..(num_servers - current_num_servers) {
                self.allocate_server();
            }
        } else if num_servers < current_num_servers {
            let server_ids: Vec<Uuid> = servers.keys().cloned().collect();
            for &server_id in server_ids
                .iter()
                .take((current_num_servers - num_servers) as usize)
            {
                self.terminate_server(server_id);
            }
        }
    }

    fn get_servers(&self) -> Vec<Uuid> {
        self.servers.lock().unwrap().keys().cloned().collect()
    }

    fn get_another_server_id(&self, excluding_id: Uuid) -> Option<Uuid> {
        self.servers
            .lock()
            .unwrap()
            .iter()
            .filter(|(&id, server)| id != excluding_id && !server.is_terminating())
            .map(|(&id, _)| id)
            .next()
    }
}

impl RequestHandler for Balancer {
    // 📌 Hint: Look into the `RequestHandler` trait definition for specification
    // docstrings of `handle()` and `shutdown()`.

    fn handle(&self, mut rq: Request) {
        match rq.kind() {
            RequestKind::GetNumServers => {
                let num_servers = self.get_num_servers();
                rq.respond_with_int(num_servers);
            }
            RequestKind::SetNumServers => {
                if let Some(num_servers) = rq.read_u32() {
                    self.set_num_servers(num_servers);
                    rq.respond_with_int(num_servers);
                } else {
                    rq.respond_with_err("Invalid number of servers");
                }
            }
            RequestKind::GetServers => {
                let servers = self.get_servers();
                rq.respond_with_server_list(&servers);
            }
            RequestKind::Debug => {
                rq.respond_with_string("Happy Debugging! 🚫🐛");
            }
            _ => {
                let customer_id = rq.customer_id();
                let server_id = {
                    let server_id_opt = {
                        let mut customer_to_server = self.customer_to_server.lock().unwrap(); // highlight
                        customer_to_server.get(&customer_id).cloned() // highlight
                    }; // highlight

                    if let Some(server_id) = server_id_opt {
                        // highlight
                        server_id // highlight
                    } else {
                        // highlight
                        let new_server_id = self.allocate_server(); // highlight
                        let mut customer_to_server = self.customer_to_server.lock().unwrap(); // highlight
                        customer_to_server.insert(customer_id, new_server_id); // highlight
                        new_server_id // highlight
                    } // highlight
                };

                if let Some(server) = self.servers.lock().unwrap().get_mut(&server_id) {
                    server.process_request(rq);
                } else {
                    rq.respond_with_err("Invalid server ID");
                }
            }
        }
    }

    fn shutdown(self) {
        let server_ids: Vec<Uuid> = self.servers.lock().unwrap().keys().cloned().collect();
        for server_id in server_ids {
            self.terminate_server(server_id);
        }
    }
}
