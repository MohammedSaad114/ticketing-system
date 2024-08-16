use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};

use ticket_sale_core::{Request, RequestHandler, RequestKind};
use uuid::Uuid;

use crate::coordinator::Coordinator;

/// Implementation of the load balancer
pub struct Balancer {
    /// Maps customer IDs to server IDs to ensure requests from the same customer
    /// are routed to the same server.
    customer_server_map: RwLock<HashMap<Uuid, Uuid>>,

    /// Optional `Coordinator` instance used for communication.
    coordinator: Option<Arc<Coordinator>>,

    /// Flag indicating if the balancer is currently shutting down.
    shutting_down: AtomicBool,
}

impl Balancer {
    /// Creates a new `Balancer` instance.
    ///
    /// # Arguments
    ///
    /// * `coordinator` - An optional `Coordinator` instance for communication.
    ///
    /// # Returns
    ///
    /// * `Self` - New instance of `Balancer`.
    pub fn new(coordinator: Option<Arc<Coordinator>>) -> Self {
        Self {
            customer_server_map: RwLock::new(HashMap::new()),
            coordinator,
            shutting_down: AtomicBool::new(false),
        }
    }

    /// Assigns a server to a customer based on their ID using the `Coordinator` to get
    /// server IDs.
    ///
    /// # Arguments
    ///
    /// * `customer_id` - The ID of the customer requesting a server.
    ///
    /// # Returns
    ///
    /// * `Uuid` - The ID of the assigned server.
    fn assign_server(&self, customer_id: Uuid) -> Uuid {
        // Acquire write lock on the customer-to-server map.
        let mut customer_server_map = self.customer_server_map.write().unwrap();

        // Check if the customer already has an assigned server.
        if let Some(&server_id) = customer_server_map.get(&customer_id) {
            return server_id; // Return the previously assigned server ID.
        }

        // Fetch server IDs from the coordinator.
        if let Some(coordinator) = &self.coordinator {
            let server_ids = coordinator.get_servers();

            // Assign a new server to the customer.
            if let Some(&server_id) = server_ids.first() {
                // Pick the first server for simplicity.
                customer_server_map.insert(customer_id, server_id);
                server_id
            } else {
                panic!("No servers available"); // Panic if no servers are available.
            }
        } else {
            panic!("Coordinator not available"); // Panic if coordinator is not available.
        }
    }
}

impl RequestHandler for Balancer {
    /// Handle incoming requests
    ///
    /// # Arguments
    ///
    /// * `rq` - The `Request` to be handled
    fn handle(&self, mut rq: Request) {
        // Check if the balancer is shutting down.
        if self.shutting_down.load(Ordering::SeqCst) {
            rq.respond_with_err("Balancer is shutting down.");
            return;
        }

        match rq.kind() {
            // Handle the request for getting the number of servers
            RequestKind::GetNumServers => {
                if let Some(coordinator) = &self.coordinator {
                    let num_servers = coordinator.get_servers().len() as u32;
                    rq.respond_with_int(num_servers);
                } else {
                    rq.respond_with_err("Coordinator not available.");
                }
            }

            // Handle the request for setting the number of servers
            RequestKind::SetNumServers => {
                // Set the number of servers as read from the adminstrator..

                let num_servers = rq.read_u32().unwrap_or(0) as usize;
                let mut servers = self.customer_server_map.write().unwrap();
                servers.clear();
                servers.extend((0..num_servers).map(|_| (Uuid::new_v4(), Uuid::new_v4())));
                rq.respond_with_int(num_servers as u32);
            }

            // Handle the request for getting the list of servers
            RequestKind::GetServers => {
                // Respond with the list of available servers
                let servers = self.customer_server_map.read().unwrap();
                rq.respond_with_server_list(
                    servers.values().cloned().collect::<Vec<Uuid>>().as_slice(),
                );
            }

            // Default case for handling all other requests
            _ => {
                // Determine the server ID to handle this request.
                let customer_id = rq.customer_id();
                let server_id = self.assign_server(customer_id);

                // Forward the request to the assigned server.
                if let Some(coordinator) = &self.coordinator {
                    if let Some(server) = coordinator.get_server(server_id) {
                        server.read().unwrap().handle_request(rq);
                    } else {
                        rq.respond_with_err("Server not found.");
                    }
                } else {
                    rq.respond_with_err("Coordinator not available.");
                }
            }
        }
    }

    /// Shut down the balancer
    fn shutdown(self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        println!("Balancer is shutting down");
    }
}
