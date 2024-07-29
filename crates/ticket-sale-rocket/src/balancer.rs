use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};

use ticket_sale_core::{Request, RequestHandler, RequestKind};
use uuid::Uuid;

use crate::coordinator::Coordinator; // Import Coordinator

/// Implementation of the load balancer
///
/// ⚠️ This struct must implement the [`RequestHandler`] trait, and it must be
/// exposed from the crate root (to be used from the tester as
/// `ticket_sale_rocket::Balancer`).
pub struct Balancer {
    /// Mapping of customers to their assigned server IDs
    customer_server_map: Arc<RwLock<HashMap<Uuid, Uuid>>>,

    /// List of available server IDs
    servers: Arc<RwLock<Vec<Uuid>>>,

    /// Flag to indicate if the balancer is shutting down
    shutting_down: AtomicBool,

    /// Reference to the Coordinator
    coordinator: Option<Arc<Coordinator>>,
}

impl Balancer {
    /// Create a new [`Balancer`]
    pub fn new(coordinator: Option<Arc<Coordinator>>) -> Self {
        Self {
            customer_server_map: Arc::new(RwLock::new(HashMap::new())),
            servers: Arc::new(RwLock::new(Vec::new())),
            shutting_down: AtomicBool::new(false),
            coordinator,
        }
    }

    /// Helper function to assign a server to a customer
    fn assign_server_to_customer(&self, customer_id: Uuid) -> Option<Uuid> {
        let servers = self.servers.read().unwrap();
        if servers.is_empty() {
            None
        } else {
            // Simple round-robin assignment
            let server_id = servers[customer_id.as_u128() as usize % servers.len()];
            self.customer_server_map
                .write()
                .unwrap()
                .insert(customer_id, server_id);
            Some(server_id)
        }
    }
}

impl RequestHandler for Balancer {
    fn handle(&self, mut rq: Request) {
        if self.shutting_down.load(Ordering::SeqCst) {
            rq.respond_with_err("Balancer is shutting down.");
            return;
        }

        match rq.kind() {
            RequestKind::GetNumServers => {
                let num_servers = self.servers.read().unwrap().len() as u32;
                rq.respond_with_int(num_servers);
            }
            RequestKind::SetNumServers => {
                let num_servers = rq.read_u32().unwrap_or(0) as usize;
                let mut servers = self.servers.write().unwrap();
                servers.clear();
                servers.extend((0..num_servers).map(|_| Uuid::new_v4()));
                rq.respond_with_int(num_servers as u32);
            }
            RequestKind::GetServers => {
                let servers = self.servers.read().unwrap();
                rq.respond_with_server_list(&servers);
            }
            RequestKind::Debug => {
                rq.respond_with_string("Happy Debugging! 🚫🐛");
            }
            _ => {
                let customer_id = rq.customer_id();
                let server_id = {
                    let customer_server_map = self.customer_server_map.read().unwrap();
                    if let Some(&server_id) = customer_server_map.get(&customer_id) {
                        server_id
                    } else {
                        match self.assign_server_to_customer(customer_id) {
                            Some(server_id) => server_id,
                            None => {
                                rq.respond_with_err("No available servers");
                                return;
                            }
                        }
                    }
                };

                // Simulate request forwarding to the assigned server
                rq.set_server_id(server_id);
                rq.respond_with_string(format!("Forwarded to server {}", server_id));
            }
        }
    }

    fn shutdown(self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        println!("Balancer is shutting down");

        // Ensure coordinator is shut down
        if let Some(coordinator) = self.coordinator {
            coordinator.stop(); // Stop the coordinator
        }
    }
}
