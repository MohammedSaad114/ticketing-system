use std::sync::mpsc::{self}; /* Import the multiple producer, single consumer channel for
                               * message passing */
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering}, /* Import atomic types for safe concurrent
                                                  * programming */
    Arc, // Import Arc for thread-safe reference counting
};
use std::thread::JoinHandle; // Import JoinHandle for managing threads

use ticket_sale_core::{Request, RequestHandler, RequestKind}; /* Import relevant items from
                                                                * ticket_sale_core */
use uuid::Uuid; // Import Uuid for unique server identification

use crate::coordinator::Coordinator; /* Import the Coordinator struct from the coordinator
                                       * module */
use crate::util::{CoordinatorMessage, ServerOrRequestMessage}; // Import message types used for communication

/// Implementation of the load balancer
///
/// The Balancer is responsible for distributing incoming requests to available servers,
/// managing the communication with the `Coordinator` to ensure that requests are handled
/// by the appropriate server.
pub struct Balancer {
    /// Index for the round-robin assignment
    ///
    /// This index keeps track of which server should handle the next request in a
    /// round-robin fashion.
    round_robin_index: AtomicUsize,

    /// Optional `Coordinator` instance used for communication.
    ///
    /// The `Coordinator` is responsible for managing the overall system, including
    /// servers and load balancing. The Balancer interacts with it to assign requests
    /// to servers.
    coordinator: Option<Arc<Coordinator>>,

    /// Flag indicating if the balancer is currently shutting down.
    ///
    /// This flag is checked before processing each request to determine if the Balancer
    /// should continue operating or start rejecting requests due to shutdown.
    shutting_down: AtomicBool,

    /// Field to store the Estimator's JoinHandle
    ///
    /// This is used to manage the thread running the Estimator, which is responsible for
    /// estimating system performance metrics. It allows the Balancer to wait for the
    /// Estimator to finish when shutting down.
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
    ///
    /// This function initializes the Balancer with a round-robin index starting at 0,
    /// the provided `Coordinator`, and sets the `shutting_down` flag to false.
    pub fn new(coordinator: Option<Arc<Coordinator>>) -> Self {
        Self {
            round_robin_index: AtomicUsize::new(0),
            coordinator,
            shutting_down: AtomicBool::new(false),
            estimator_handle: None,
        }
    }

    /// Sets the Estimator's JoinHandle for the balancer.
    ///
    /// # Arguments
    ///
    /// * `handle` - A `JoinHandle` to the Estimator's thread.
    ///
    /// # Returns
    ///
    /// * `Self` - Returns the modified `Balancer` instance.
    ///
    /// This function allows the Balancer to store a handle to the Estimator's thread, so
    /// that it can join (i.e., wait for) this thread when shutting down.
    pub fn set_estimator_handle(mut self, handle: JoinHandle<()>) {
        self.estimator_handle = Some(handle);
    }

    /// Assigns a server to a customer based on their ID using the `Coordinator` to get
    /// server IDs.
    ///
    /// # Arguments
    ///
    /// * `current_server_id` - The ID of the server currently handling the customer, if
    ///   any.
    ///
    /// # Returns
    ///
    /// * `Option<(Uuid, Option<Uuid>)>` - A tuple containing the assigned server's ID and
    ///   an available server's ID (if different from the current server).
    ///
    /// This function decides which server should handle the next request. It first checks
    /// if a `Coordinator` is available. If it is, it requests the list of running and
    /// terminating servers from the `Coordinator`. Depending on the current server
    /// handling the request, it either continues with the same server or assigns a
    /// new one using a round-robin strategy.
    fn assign_server(&self, current_server_id: Option<Uuid>) -> Option<(Uuid, Option<Uuid>)> {
        if let Some(coordinator) = &self.coordinator {
            // Create a channel to receive the list of running and terminating servers
            let (response_tx, response_rx) = mpsc::channel();
            coordinator
                .get_message_tx()
                .send(CoordinatorMessage::GetRunningAndTerminatingServers(
                    response_tx,
                ))
                .unwrap();

            // Wait for the list from the Coordinator
            if let Ok((running_servers, terminating_servers)) = response_rx.recv() {
                // Return None only if both running_servers and terminating_servers are empty to
                // avoid leaving open requests unansewred
                if running_servers.is_empty() && terminating_servers.is_empty() {
                    return None; // No servers available in both lists
                }

                if let Some(server_id) = current_server_id {
                    // If the current server is still valid, use it
                    if running_servers.contains(&server_id)
                        || terminating_servers.contains(&server_id)
                    {
                        let available_server =
                            running_servers.iter().cloned().find(|&id| id != server_id);
                        return Some((server_id, available_server));
                    } else {
                        // I suspect that issue lies here,,,,we don't  account for the individual
                        // servers? Otherwise, assign a new server using
                        // round-robin
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
                    // No current server, so assign a new one using round-robin
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
    /// # Arguments
    ///
    /// * `rq` - The `Request` to be handled.
    ///
    /// This function is responsible for handling different types of requests, such as
    /// getting the number of servers, setting the number of servers, or routing requests
    /// to the appropriate server.
    fn handle(&self, mut rq: Request) {
        if self.shutting_down.load(Ordering::SeqCst) {
            // If the Balancer is shutting down, respond with an error
            rq.respond_with_err("Balancer is shutting down.");
            return;
        }

        match rq.kind() {
            RequestKind::GetNumServers => {
                // Handle request to get the number of servers
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
                // Handle request to set the number of servers
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
                // Handle request to get the list of servers
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
                // Handle other request types by assigning a server
                let current_server_id = rq.server_id();
                if let Some((new_server_id, available_id)) = self.assign_server(current_server_id) {
                    if current_server_id != Some(new_server_id) {
                        rq.set_server_id(new_server_id); // Set the new server ID for the
                                                         // request
                    }

                    if let Some(coordinator) = &self.coordinator {
                        if let Some(server_sender) = coordinator.get_server_sender(new_server_id) {
                            server_sender
                                .send(ServerOrRequestMessage::ClientRequest {
                                    request: rq,
                                    available_server: available_id,
                                })
                                .unwrap_or_else(|_e| {});
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
    ///
    /// This function safely shuts down the Balancer, ensuring that all threads are joined
    /// and any necessary cleanup is performed.
    fn shutdown(self) {
        // Set the shutting down flag to true
        self.shutting_down.store(true, Ordering::SeqCst);
        println!("Balancer is shutting down");

        if let Some(coordinator) = self.coordinator {
            let message_tx = coordinator.get_message_tx();

            // Send the shutdown message to the Coordinator
            if let Err(_e) = message_tx.send(CoordinatorMessage::Shutdown) {
                // Handle error if necessary
            }
        }

        // Join the Estimator thread if it was set
        if let Some(handle) = self.estimator_handle {
            handle.join().unwrap(); // Wait for the thread to finish
        }
    }
}
