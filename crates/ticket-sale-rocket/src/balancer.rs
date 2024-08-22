use std::collections::HashMap;
use std::sync::mpsc::{self};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, RwLock,
};
use std::thread::JoinHandle;

use ticket_sale_core::{Request, RequestHandler, RequestKind};
use uuid::Uuid;

use crate::coordinator::Coordinator;
use crate::messages::{CoordinatorMessage, ServerMessage, ServerOrRequestMessage};

/// Implementation of the load balancer
pub struct Balancer {
    /// Maps customer IDs to server IDs to ensure requests from the same customer
    /// are routed to the same server.
    customer_server_map: RwLock<HashMap<Uuid, Uuid>>,

    /// Index for the round-robin assignment
    round_robin_index: AtomicUsize,

    /// Optional `Coordinator` instance used for communication.
    coordinator: Option<Arc<Coordinator>>,

    /// Flag indicating if the balancer is currently shutting down.
    shutting_down: AtomicBool,

    /// Flag indicating if the balancer is currently terminating.
    terminating: AtomicBool,

    /// Field to store the Estimator's JoinHandle
    estimator_handle: Option<JoinHandle<()>>,
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
            round_robin_index: AtomicUsize::new(0),
            coordinator,
            shutting_down: AtomicBool::new(false),
            terminating: AtomicBool::new(false),
            estimator_handle: None,
        }
    }

    /// Sets the Estimator's JoinHandle for the balancer.
    ///
    /// This allows the balancer to wait for the Estimator thread to finish during
    /// shutdown.
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
    fn assign_server(&self, customer_id: Uuid) -> Option<Uuid> {
        // Acquire a write lock on the customer-to-server map.
        let mut customer_server_map = self.customer_server_map.write().unwrap();

        // If the customer is already mapped to a server, return the server ID.
        if let Some(&server_id) = customer_server_map.get(&customer_id) {
            return Some(server_id);
        }

        // Retrieve the list of server IDs from the coordinator.
        let server_ids = self.coordinator.as_ref()?.get_servers();

        if server_ids.is_empty() {
            return None;
        }

        // Perform round-robin assignment.
        let server_count = server_ids.len();
        let index = self.round_robin_index.fetch_add(1, Ordering::SeqCst) % server_count;
        let server_id = server_ids[index];

        // Map the customer to the selected server.
        customer_server_map.insert(customer_id, server_id);

        // Return the selected server ID.
        Some(server_id)
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

        // Check if the balancer is terminating.
        if self.terminating.load(Ordering::SeqCst) {
            rq.respond_with_err("Balancer is terminating.");
            return;
        }

        match rq.kind() {
            RequestKind::GetNumServers => {
                if let Some(coordinator) = &self.coordinator {
                    let (response_tx, response_rx) = mpsc::channel();
                    coordinator
                        .get_message_tx()
                        .send(CoordinatorMessage::GetNumServers(response_tx))
                        .unwrap();
                    if let Ok(num_servers) = response_rx.recv() {
                        rq.respond_with_int(num_servers);
                    }
                } else {
                    rq.respond_with_err("Coordinator not available.");
                }
            }

            RequestKind::SetNumServers => {
                let num_servers = rq.read_u32().unwrap_or(0) as usize;
                if let Some(coordinator) = &self.coordinator {
                    let (response_tx, response_rx) = mpsc::channel();
                    coordinator
                        .get_message_tx()
                        .send(CoordinatorMessage::SetNumServers(num_servers, response_tx))
                        .unwrap();
                    if let Ok(updated_servers) = response_rx.recv() {
                        rq.respond_with_int(updated_servers);
                    }
                } else {
                    rq.respond_with_err("Coordinator not available.");
                }
            }

            RequestKind::GetServers => {
                if let Some(coordinator) = &self.coordinator {
                    let (response_tx, response_rx) = mpsc::channel();
                    coordinator
                        .get_message_tx()
                        .send(CoordinatorMessage::GetServers(response_tx))
                        .unwrap();
                    if let Ok(server_ids) = response_rx.recv() {
                        rq.respond_with_server_list(&server_ids);
                    }
                } else {
                    rq.respond_with_err("Coordinator not available.");
                }
            }

            RequestKind::ReserveTicket => {
                // Handle reservation requests with server termination checks
                let customer_id = rq.customer_id();

                // Assign a server to the customer, considering potential server termination.
                if let Some(server_id) = self.assign_server(customer_id) {
                    if let Some(coordinator) = &self.coordinator {
                        if let Some(server_sender) = coordinator.get_server_sender(server_id) {
                            // Check if the server is terminating before sending the request.
                            let (response_tx, response_rx) = mpsc::channel();
                            server_sender
                                .send(ServerOrRequestMessage::ServerMessage(
                                    ServerMessage::CheckIfTerminating(response_tx),
                                ))
                                .unwrap();

                            if let Ok(is_terminating) = response_rx.recv() {
                                if is_terminating {
                                    // Server is terminating, remove the current mapping.
                                    println!(
                                        "Server {} is terminating. Reassigning request from customer {}.",
                                        server_id, customer_id
                                    );
                                    self.customer_server_map
                                        .write()
                                        .unwrap()
                                        .remove(&customer_id);

                                    // Reassign to another server.
                                    if let Some(new_server_id) = self.assign_server(customer_id) {
                                        if let Some(new_server_sender) =
                                            coordinator.get_server_sender(new_server_id)
                                        {
                                            // Send the request to the newly assigned server.
                                            new_server_sender
                                                .send(ServerOrRequestMessage::ClientRequest(rq))
                                                .unwrap();
                                        } else {
                                            rq.respond_with_err("No available servers.");
                                        }
                                    } else {
                                        rq.respond_with_err("No available servers.");
                                    }
                                } else {
                                    // Server is not terminating, proceed to send the request.
                                    server_sender
                                        .send(ServerOrRequestMessage::ClientRequest(rq))
                                        .unwrap();
                                }
                            } else {
                                rq.respond_with_err("Failed to check server state.");
                            }
                        }
                    }
                } else {
                    rq.respond_with_err("No available servers.");
                }
            }

            // Handle all other request types normally
            _ => {
                let customer_id = rq.customer_id();
                if let Some(server_id) = self.assign_server(customer_id) {
                    if let Some(coordinator) = &self.coordinator {
                        if let Some(server_sender) = coordinator.get_server_sender(server_id) {
                            server_sender
                                .send(ServerOrRequestMessage::ClientRequest(rq))
                                .unwrap();
                        } else {
                            rq.respond_with_err("Server not found.");
                        }
                    } else {
                        rq.respond_with_err("Coordinator not available.");
                    }
                } else {
                    rq.respond_with_err("No available servers.");
                }
            }
        }
    }

    /// Shut down the balancer
    ///
    /// This method ensures that the balancer is shut down gracefully, ensuring
    /// that any associated threads (like the Estimator) are properly joined.
    fn shutdown(self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        println!("Balancer is shutting down");

        if let Some(coordinator) = self.coordinator {
            coordinator.shutdown();
        }

        if let Some(handle) = self.estimator_handle {
            handle.join().unwrap();
        }

        println!("Balancer has been fully shut down");
    }
}
