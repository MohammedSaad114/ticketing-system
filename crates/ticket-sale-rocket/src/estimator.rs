use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc::channel, Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::messages::{ServerMessage, ServerOrRequestMessage};
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

    /// Condvar and mutex to wait for shutdown completion
    shutdown_cv: Arc<(Mutex<bool>, Condvar)>,
    handle: Mutex<Option<JoinHandle<()>>>, // Handle wrapped in a Mutex
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
            shutdown_cv: Arc::new((Mutex::new(false), Condvar::new())),
            handle: Mutex::new(None), // Initialize with None
        }
    }

    /// Starts the `Estimator` in a separate thread to periodically update server
    /// estimates.
    ///
    /// # Returns
    ///
    /// * `JoinHandle<()>` - Handle to the spawned thread
    pub fn start(&self) {
        let coordinator = self.coordinator.clone();
        let database = self.database.clone();
        let roundtrip_secs = self.roundtrip_secs;
        let running = self.running.clone();
        let shutdown_cv = self.shutdown_cv.clone();

        let handle = std::thread::spawn(move || {
            let roundtrip_duration = Duration::from_secs(roundtrip_secs as u64);
            while running.load(Ordering::SeqCst) {
                let start_time = Instant::now();

                let servers = coordinator.get_running_servers();
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

                let mut total_allocated_tickets = 0;
                for server_id in &servers {
                    if let Some(server_sender) = coordinator.get_server_sender(*server_id) {
                        let (ticket_count_tx, ticket_count_rx) = channel();
                        let _ = server_sender.send(ServerOrRequestMessage::ServerMessage(
                            ServerMessage::RequestTicketCount(ticket_count_tx),
                        ));

                        if let Ok(ticket_count) = ticket_count_rx.recv() {
                            total_allocated_tickets += ticket_count;
                        }
                    }
                }

                let total_available_tickets = db_available + total_allocated_tickets;

                for server_id in servers {
                    if let Some(server_sender) = coordinator.get_server_sender(server_id) {
                        let _ = server_sender.send(ServerOrRequestMessage::ServerMessage(
                            ServerMessage::UpdateTicketEstimate(total_available_tickets),
                        ));
                    }
                }

                let elapsed = Instant::now() - start_time;
                let remaining_time = roundtrip_duration
                    .checked_sub(elapsed)
                    .unwrap_or(Duration::new(0, 0));

                let (lock, cv) = &*shutdown_cv;
                let mut shutdown_complete = lock.lock().unwrap();
                if *shutdown_complete {
                    break;
                }
                let timeout_result = cv.wait_timeout(shutdown_complete, remaining_time).unwrap();
                shutdown_complete = timeout_result.0;

                if *shutdown_complete {
                    break;
                }
            }

            let (lock, cv) = &*shutdown_cv;
            let mut shutdown_complete = lock.lock().unwrap();
            *shutdown_complete = true;
            cv.notify_all();
        });

        *self.handle.lock().unwrap() = Some(handle);
    }

    /// Stops the `Estimator` by setting the running flag to false and waits for the
    /// current iteration to complete gracefully.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
        println!("Estimator is shutting down...");

        let (lock, cv) = &*self.shutdown_cv;
        let mut shutdown_complete = lock.lock().unwrap();
        *shutdown_complete = true;
        cv.notify_all(); // Signal the thread to wake up if it's sleeping

        if let Some(handle) = self.handle.lock().unwrap().take() {
            handle.join().unwrap();
        }

        println!("Estimator has shut down gracefully");
    }
}
