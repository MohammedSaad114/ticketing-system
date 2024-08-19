use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex, RwLock,
};
use std::thread::JoinHandle;

use ticket_sale_core::{Request, RequestHandler, RequestKind};
use uuid::Uuid;

use crate::coordinator::Coordinator;

/// Implementation of the load balancer
pub struct Balancer {
    /// Maps customer IDs to server IDs to ensure requests from the same customer
    /// are routed to the same server.
    customer_server_map: RwLock<HashMap<Uuid, Uuid>>,

    /// List of server IDs for round-robin assignment
    server_ids: RwLock<Vec<Uuid>>,

    /// Index for the round-robin assignment
    round_robin_index: AtomicUsize,

    /// Optional `Coordinator` instance used for communication.
    coordinator: Option<Arc<Coordinator>>,

    /// Receiver for server update messages
    server_update_rx: Arc<Mutex<Receiver<Vec<Uuid>>>>,

    /// Flag indicating if the balancer is currently shutting down.
    shutting_down: AtomicBool,
    estimator_handle: Option<JoinHandle<()>>, // Field to store the Estimator's JoinHandle
}

impl Balancer {
    /// Creates a new `Balancer` instance.
    ///
    /// # Arguments
    ///
    /// * `coordinator` - An optional `Coordinator` instance for communication.
    /// * `server_update_rx` - Receiver for server update messages.
    ///
    /// # Returns
    ///
    /// * `Self` - New instance of `Balancer`.
    pub fn new(
        coordinator: Option<Arc<Coordinator>>,
        server_update_rx: Arc<Mutex<Receiver<Vec<Uuid>>>>,
    ) -> Self {
        Self {
            customer_server_map: RwLock::new(HashMap::new()),
            server_ids: RwLock::new(Vec::new()),
            round_robin_index: AtomicUsize::new(0),
            coordinator,
            server_update_rx,
            shutting_down: AtomicBool::new(false),
            estimator_handle: None, // Initialize with None
        }
    }

    pub fn set_estimator_handle(mut self, handle: JoinHandle<()>) -> Self {
        self.estimator_handle = Some(handle);
        self
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
        let server_ids = if let Some(coordinator) = &self.coordinator {
            coordinator.get_servers()
        } else {
            panic!("Coordinator not available");
        };

        // Ensure server_ids is not empty
        if server_ids.is_empty() {
            panic!("No servers available");
        }

        // Use round-robin to determine the server ID
        let server_count = server_ids.len();
        let index = self.round_robin_index.fetch_add(1, Ordering::SeqCst) % server_count;
        let server_id = server_ids[index];

        // Assign the selected server to the customer
        customer_server_map.insert(customer_id, server_id);

        println!("Assigned server {} to customer {}", server_id, customer_id);

        server_id
    }

    /// Polls for server update messages.
    fn poll_server_updates(&self) {
        // Acquire the lock on the receiver.
        let receiver = self.server_update_rx.lock().unwrap();

        while let Ok(server_ids) = receiver.try_recv() {
            // Update server IDs in the balancer.
            let mut server_ids_lock = self.server_ids.write().unwrap();
            *server_ids_lock = server_ids;

            println!("Updated server list: {:?}", *server_ids_lock);
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

        // Poll for server updates before handling the request.
        self.poll_server_updates();

        match rq.kind() {
            // Handle the request for getting the number of servers
            RequestKind::GetNumServers => {
                let num_servers = self.server_ids.read().unwrap().len() as u32;
                rq.respond_with_int(num_servers);
            }

            // Handle the request for setting the number of servers
            RequestKind::SetNumServers => {
                let num_servers = rq.read_u32().unwrap_or(0) as usize;
                if let Some(coordinator) = &self.coordinator {
                    coordinator.set_num_servers(num_servers);
                    rq.respond_with_int(num_servers as u32);
                } else {
                    rq.respond_with_err("Coordinator not available.");
                }
            }

            // Handle the request for getting the list of servers
            RequestKind::GetServers => {
                let server_ids = self.server_ids.read().unwrap();
                rq.respond_with_server_list(server_ids.as_slice());
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

        if let Some(coordinator) = self.coordinator {
            coordinator.shutdown();
        }

        if let Some(handle) = self.estimator_handle {
            handle.join().unwrap(); // Wait for the Estimator to finish
        }

        println!("Balancer has been fully shut down");
    }
}
