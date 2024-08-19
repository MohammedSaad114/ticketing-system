#![allow(rustdoc::private_intra_doc_links)]

use std::sync::mpsc;
use std::sync::{Arc, Mutex, RwLock};

use estimator::Estimator;
use ticket_sale_core::Config;

mod balancer;
mod coordinator;
mod database;
mod estimator;
mod server;

pub use balancer::Balancer;
use coordinator::Coordinator;
use database::Database;

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
    let database = Arc::new(RwLock::new(Database::new(config.tickets)));

    // Create a communication channel for server updates between the Coordinator and the
    // Balancer
    let (server_update_tx, server_update_rx) = mpsc::channel();
    let server_update_rx = Arc::new(Mutex::new(server_update_rx));

    // Create a new Coordinator instance wrapped in an Arc for shared ownership
    let coordinator = Arc::new(Coordinator::new(config, database.clone(), server_update_tx));

    // Create a new Balancer instance, providing it with the Coordinator and the server update
    // receiver
    let balancer = Balancer::new(Some(coordinator.clone()), server_update_rx.clone());

    // Create a new Estimator instance, also wrapped in an Arc
    let estimator = Arc::new(Estimator::new(
        coordinator.clone(),             // Pass the Coordinator instance
        database.clone(),                // Pass the Database instance
        config.estimator_roundtrip_time, // Configuration for roundtrip time estimation
    ));

    // Start the Estimator to begin its operation
    estimator.start(); // Start the estimator but do not return the handle

    // Return the Balancer instance to handle incoming requests
    balancer
}
