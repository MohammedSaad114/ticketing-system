//! Implementation of the load balancer

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ticket_sale_core::{Request, RequestHandler, RequestKind};
use uuid::Uuid;

use super::coordinator::Coordinator;

/// Implementation of the load balancer
///
/// ⚠️ This struct must implement the [`RequestHandler`] trait, and it must be
/// exposed from the crate root (to be used from the tester as
/// `ticket_sale_rocket::Balancer`).
pub struct Balancer {
    coordinator: Arc<Coordinator>,
    customer_to_server: Arc<Mutex<HashMap<Uuid, Uuid>>>,
}

impl Balancer {
    /// Create a new [`Balancer`]
    pub fn new(coordinator: Arc<Coordinator>) -> Self {
        Self {
            coordinator,
            customer_to_server: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn get_server_for_customer(&self, customer: Uuid) -> Option<Uuid> {
        self.customer_to_server
            .lock()
            .unwrap()
            .get(&customer)
            .cloned()
    }

    fn assign_server_to_customer(&self, customer: Uuid) -> Uuid {
        let server_id = {
            let servers = self.coordinator.get_servers();
            let index = rand::random::<usize>() % servers.len();
            servers[index]
        };
        self.customer_to_server
            .lock()
            .unwrap()
            .insert(customer, server_id);
        server_id
    }
    /*
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
        let servers = self.servers.lock().unwrap();
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
    /*
    fn get_another_server_id(&self, excluding_id: Uuid) -> Option<Uuid> {
        self.servers
            .lock()
            .unwrap()
            .iter()
            .filter(|(&id, server)| id != excluding_id && !server.is_terminating())
            .map(|(&id, _)| id)
            .next()
    }*/*/
}

impl RequestHandler for Balancer {
    // 📌 Hint: Look into the `RequestHandler` trait definition for specification
    // docstrings of `handle()` and `shutdown()`.

    fn handle(&self, mut rq: Request) {
        match rq.kind() {
            RequestKind::GetNumServers => {
                let num_servers = self.coordinator.get_servers().len() as u32;
                rq.respond_with_int(num_servers);
            }
            RequestKind::SetNumServers => {
                if let Some(num) = rq.read_u32() {
                    let current_num = self.coordinator.get_servers().len() as u32;
                    match num.cmp(&current_num) {
                        std::cmp::Ordering::Greater => {
                            for _ in 0..(num - current_num) {
                                self.coordinator.add_server(10);
                            }
                        }
                        std::cmp::Ordering::Less => {
                            let servers = self.coordinator.get_servers();
                            for id in servers.iter().take((current_num - num) as usize) {
                                self.coordinator.remove_server(*id);
                            }
                        }
                        std::cmp::Ordering::Equal => {}
                    }
                    rq.respond_with_int(num);
                } else {
                    rq.respond_with_err("No number of servers provided");
                }
            }
            RequestKind::GetServers => {
                let servers = self.coordinator.get_servers();
                rq.respond_with_server_list(&servers);
            }
            _ => {
                let customer_id = rq.customer_id();
                let server_id = self
                    .get_server_for_customer(customer_id)
                    .unwrap_or_else(|| self.assign_server_to_customer(customer_id));
                rq.set_server_id(server_id);
                self.coordinator.handle_request(rq);
            }
        }
    }

    fn shutdown(self) {
        // Wait for all servers to finish processing
        let servers = self.coordinator.get_servers();
        for id in servers {
            if let Some(server) = self.coordinator.get_server(id) {
                server.lock().unwrap().terminate();
            }
        }
    }
}
