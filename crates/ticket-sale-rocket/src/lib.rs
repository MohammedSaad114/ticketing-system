#![allow(rustdoc::private_intra_doc_links)]

use std::sync::mpsc;
use std::sync::{Arc, RwLock};

use estimator::Estimator;
use ticket_sale_core::Config;

mod balancer;
mod coordinator;
mod database;
mod estimator;
mod server;
mod util;

pub use balancer::Balancer;
use coordinator::Coordinator;
use database::Database;
use util::CoordinatorMessage;

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

    let coordinator = Arc::new(Coordinator::new(
        config,
        database.clone(),
        message_tx.clone(),
    ));

    // Spawn the messaging thread
    let coordinator_clone = Arc::clone(&coordinator);
    std::thread::spawn(move || {
        coordinator_clone.run(message_rx); // Pass the receiver directly
    });

    // Initialize the Balancer with the Coordinator
    let balancer = Balancer::new(Some(coordinator.clone()));

    // Initialize and start the Estimator
    let estimator = Arc::new(Estimator::new(
        coordinator.clone(),             // Pass the Coordinator instance
        database.clone(),                // Pass the Database instance
        config.estimator_roundtrip_time, // Configuration for roundtrip time estimation
    ));

    estimator.start(); // Start the estimator thread

    // Return the initialized Balancer instance
    balancer
}
