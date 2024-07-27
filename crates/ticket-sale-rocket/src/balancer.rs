//! Implementation of the load balancer
//! balancer.rs
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
    estimator_handle: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
}

impl Balancer {
    pub fn new(
        coordinator: Arc<Coordinator>,
        estimator_handle: std::thread::JoinHandle<()>,
    ) -> Self {
        Self {
            coordinator,
            customer_to_server: Arc::new(Mutex::new(HashMap::new())),
            estimator_handle: Arc::new(Mutex::new(Some(estimator_handle))),
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
            if !servers.is_empty() {
                let index = rand::random::<usize>() % servers.len();
                servers[index]
            } else {
                panic!("No servers available.");
            }
        };
        self.customer_to_server
            .lock()
            .unwrap()
            .insert(customer, server_id);
        server_id
    }
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
                        _ => {}
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
        self.coordinator.shutdown();

        if let Some(handle) = self.estimator_handle.lock().unwrap().take() {
            handle.join().expect("Estimator thread panicked");
        }
    }
}
