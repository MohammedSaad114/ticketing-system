use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::{Coordinator, Database};

/// The `Estimator` is responsible for estimating and updating the resource availability
/// for servers based on the current state of the `Database` and server configuration.
pub struct Estimator {
    /// A reference to the `Coordinator`, used to access and manage servers
    coordinator: Arc<Coordinator>,

    /// A reference to the shared `Database`, used to get the number of available
    /// resources
    database: Arc<RwLock<Database>>,

    /// Time allocated for the estimation process in seconds
    roundtrip_secs: u32,

    /// Flag indicating whether the `Estimator` is currently running
    running: Arc<AtomicBool>,
}

impl Estimator {
    /// Creates a new `Estimator`.
    ///
    /// # Arguments
    ///
    /// * `coordinator` - The `Coordinator` instance used to access servers
    /// * `database` - The `Database` instance used to get the number of available
    ///   resources
    /// * `roundtrip_secs` - Time allocated for the round-trip estimation process in
    ///   seconds
    ///
    /// # Returns
    ///
    /// * `Self` - New instance of `Estimator`
    pub fn new(
        coordinator: Arc<Coordinator>,
        database: Arc<RwLock<Database>>,
        roundtrip_secs: u32,
    ) -> Self {
        Self {
            coordinator,
            database,
            roundtrip_secs,
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Starts the `Estimator` in a separate thread to periodically update server
    /// estimates.
    ///
    /// # Returns
    ///
    /// * `std::thread::JoinHandle<()>` - Handle to the spawned thread
    pub fn start(&self) -> std::thread::JoinHandle<()> {
        let coordinator = self.coordinator.clone();
        let database = self.database.clone();
        let roundtrip_secs = self.roundtrip_secs;
        let running = self.running.clone();

        std::thread::spawn(move || {
            let roundtrip_duration = Duration::from_secs(roundtrip_secs as u64);
            while running.load(Ordering::SeqCst) {
                let start_time = Instant::now();

                // Determine the set of servers currently in operation
                let servers = coordinator.get_servers();
                let num_servers = servers.len() as u32;

                // If no servers are available, sleep for a short time and continue
                if num_servers == 0 {
                    std::thread::sleep(Duration::from_secs(1));
                    continue;
                }

                // Query the database for the number of available tickets
                let db_available = {
                    let db = database.read().unwrap();
                    db.get_num_available()
                };

                // Calculate the total number of tickets allocated across all servers
                let mut total_allocated_tickets = 0;
                for server_id in &servers {
                    if let Some(server) = coordinator.get_server(*server_id) {
                        total_allocated_tickets +=
                            server.read().unwrap().get_allocated_ticket_count();
                    }
                }

                // Total available tickets including the tickets held by servers
                let total_available_tickets = db_available + total_allocated_tickets;

                // Update each server with the estimated availability
                for server_id in servers {
                    if let Some(server) = coordinator.get_server(server_id) {
                        server
                            .write()
                            .unwrap()
                            .update_estimate(total_available_tickets);
                    }
                }

                // Sleep until the roundtrip duration has passed
                let elapsed = Instant::now() - start_time;
                let remaining_time = roundtrip_duration
                    .checked_sub(elapsed)
                    .unwrap_or(Duration::new(0, 0));
                std::thread::sleep(remaining_time);
            }
        })
    }

    /// Stops the `Estimator` by setting the running flag to false.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
        println!("Estimator is shutting down");
    }
}
