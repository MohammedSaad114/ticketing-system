#![allow(rustdoc::private_intra_doc_links)]

use std::sync::mpsc;
use std::sync::{Arc, Mutex, RwLock};

use estimator::Estimator;
use ticket_sale_core::Config;

mod balancer;
mod coordinator;
mod database;
mod estimator;
mod messages;
mod server;

pub use balancer::Balancer;
use coordinator::Coordinator;
use database::Database;
use messages::CoordinatorMessage;
/// Entrypoint of your implementation
///
/// 📌 Hint: The function must construct a balancer which is served requests by the
/// surrounding infrastructure.
///
/// ⚠️ This function must not be renamed and its signature must not be changed.
pub fn launch(config: &Config) -> Balancer {
    // Check if bonus functionality is requested in the config (not implemented)
    if config.bonus {
        todo!("Bonus not implemented!")
    }

    // Create a new Database instance wrapped in an RwLock for thread-safe read/write access
    let database: Arc<RwLock<Database>> = Arc::new(RwLock::new(Database::new(config.tickets)));

    // Create a communication channel for CoordinatorMessages
    let (message_tx, message_rx) = mpsc::channel::<CoordinatorMessage>();
    let message_rx = Arc::new(Mutex::new(message_rx));

    let coordinator = Arc::new(Coordinator::new(
        config,
        database.clone(),
        message_tx.clone(),
    ));

    // Spawn the messaging thread
    let coordinator_clone = Arc::clone(&coordinator);
    let message_rx_clone = Arc::clone(&message_rx); // Clone the Arc for the thread
    std::thread::spawn(move || {
        coordinator_clone.run(message_rx_clone);
    });

    let balancer = Balancer::new(Some(coordinator.clone()));

    let estimator = Arc::new(Estimator::new(
        coordinator.clone(),             // Pass the Coordinator instance
        database.clone(),                // Pass the Database instance
        config.estimator_roundtrip_time, // Configuration for roundtrip time estimation
    ));

    estimator.start(); // Start the estimator but do not return the handle

    balancer
}
