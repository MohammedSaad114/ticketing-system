use std::sync::mpsc::{self};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::thread::JoinHandle;

use ticket_sale_core::{Request, RequestHandler, RequestKind};
use uuid::Uuid;

use crate::coordinator::Coordinator;
use crate::util::{CoordinatorMessage, ServerOrRequestMessage};

/// Implementation of the load balancer
pub struct Balancer {
    /// Index for the round-robin assignment
    round_robin_index: AtomicUsize,

    /// Optional `Coordinator` instance used for communication.
    coordinator: Option<Arc<Coordinator>>,

    /// Flag indicating if the balancer is currently shutting down.
    shutting_down: AtomicBool,

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
            round_robin_index: AtomicUsize::new(0),
            coordinator,
            shutting_down: AtomicBool::new(false),
            estimator_handle: None,
        }
    }

    /// Sets the Estimator's JoinHandle for the balancer.
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
    fn assign_server(&self, current_server_id: Option<Uuid>) -> Option<(Uuid, Option<Uuid>)> {
        if let Some(coordinator) = &self.coordinator {
            let (response_tx, response_rx) = mpsc::channel();
            coordinator
                .get_message_tx()
                .send(CoordinatorMessage::GetRunningAndTerminatingServers(
                    response_tx,
                ))
                .unwrap();

            if let Ok((running_servers, terminating_servers)) = response_rx.recv() {
                if running_servers.is_empty() {
                    return None;
                }

                if let Some(server_id) = current_server_id {
                    if running_servers.contains(&server_id) {
                        // Server is valid, return it with an available server from the running list.
                        let available_server =
                            running_servers.iter().cloned().find(|&id| id != server_id);
                        return Some((server_id, available_server));
                    } else if terminating_servers.contains(&server_id) {
                        // If the server is terminating, redirect to another running server
                        let server_count = running_servers.len();
                        let index =
                            self.round_robin_index.fetch_add(1, Ordering::SeqCst) % server_count;
                        let new_server_id = running_servers[index];
                        let available_server = running_servers
                            .iter()
                            .cloned()
                            .find(|&id| id != new_server_id);
                        return Some((new_server_id, available_server));
                    } else {
                        // Server is not valid, assign a new server ID from the running servers.
                        let server_count = running_servers.len();
                        let index =
                            self.round_robin_index.fetch_add(1, Ordering::SeqCst) % server_count;
                        let new_server_id = running_servers[index];
                        let available_server = running_servers
                            .iter()
                            .cloned()
                            .find(|&id| id != new_server_id);
                        return Some((new_server_id, available_server));
                    }
                } else {
                    // No server ID assigned yet, assign a new one using round-robin.
                    let server_count = running_servers.len();
                    let index =
                        self.round_robin_index.fetch_add(1, Ordering::SeqCst) % server_count;
                    let new_server_id = running_servers[index];
                    let available_server = running_servers
                        .iter()
                        .cloned()
                        .find(|&id| id != new_server_id);
                    return Some((new_server_id, available_server));
                }
            }
        }
        None
    }
}

impl RequestHandler for Balancer {
    /// Handle incoming requests
    ///
    /// * `rq` - The `Request` to be handled
    fn handle(&self, mut rq: Request) {
        if self.shutting_down.load(Ordering::SeqCst) {
            rq.respond_with_err("Balancer is shutting down.");
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

            _ => {
                let current_server_id = rq.server_id();
                if let Some((new_server_id, available_id)) = self.assign_server(current_server_id) {
                    if current_server_id != Some(new_server_id) {
                        rq.set_server_id(new_server_id);
                    }

                    if let Some(coordinator) = &self.coordinator {
                        if let Some(server_sender) = coordinator.get_server_sender(new_server_id) {
                            server_sender
                                .send(ServerOrRequestMessage::ClientRequest {
                                    request: rq,
                                    available_server: available_id,
                                })
                                .unwrap_or_else(|_e| {
                                });
                        } else {
                            rq.respond_with_err("Assigned server not found.");
                        }
                    } else {
                        rq.respond_with_err("Coordinator not available.");
                    }
                } else {
                    rq.respond_with_err("No server assigned to handle the request.");
                }
            }
        }
    }

    /// Shut down the balancer
    fn shutdown(self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        println!("Balancer is shutting down");

        if let Some(coordinator) = self.coordinator {
            let message_tx = coordinator.get_message_tx();

            // Send the shutdown message to the Coordinator
            if let Err(e) = message_tx.send(CoordinatorMessage::Shutdown) {
            }
        }

        if let Some(handle) = self.estimator_handle {
            handle.join().unwrap();
        }

    }
}
