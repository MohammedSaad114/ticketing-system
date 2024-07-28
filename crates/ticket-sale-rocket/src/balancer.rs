//! Implementation of the load balancer
//! balancer.rs
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use ticket_sale_core::{Config, Request, RequestHandler, RequestKind};
use uuid::Uuid;

use super::coordinator::Coordinator;

/// Implementation of the load balancer
///
/// ⚠️ This struct must implement the [`RequestHandler`] trait, and it must be
/// exposed from the crate root (to be used from the tester as
/// `ticket_sale_rocket::Balancer`).
pub struct Balancer {
    coordinator: Arc<Coordinator>,
    customer_to_server: Arc<RwLock<HashMap<Uuid, Uuid>>>,
    estimator_handle: Arc<RwLock<Option<std::thread::JoinHandle<()>>>>,
    config: Config,
}

impl Balancer {
    pub fn new(
        coordinator: Arc<Coordinator>,
        estimator_handle: std::thread::JoinHandle<()>,
        config: Config,
    ) -> Self {
        Self {
            coordinator,
            customer_to_server: Arc::new(RwLock::new(HashMap::new())),
            estimator_handle: Arc::new(RwLock::new(Some(estimator_handle))),
            config,
        }
    }

    fn get_server_for_customer(&self, customer: Uuid) -> Option<Uuid> {
        self.customer_to_server
            .read()
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
                return Default::default();
            }
        };
        self.customer_to_server
            .write()
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
                    let mut coordinator = Arc::clone(&self.coordinator);
                    let adjusted_count = Arc::get_mut(&mut coordinator)
                        .unwrap()
                        .adjust_server_count(num, &self.config);
                    rq.respond_with_int(adjusted_count);
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
        if let Some(handle) = self.estimator_handle.write().unwrap().take() {
            handle.join().expect("Estimator thread panicked");
        }
    }
}
