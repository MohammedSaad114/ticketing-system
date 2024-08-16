//! 🚀 Your implementation of the ticket sales system must go here.
//! lib.rs
//!
//! We already provide you with a skeleton of classes for the components of the
//! system: The [database], [load balancer][balancer], [coordinator],
//! [estimator], and [server].
//!
//! While you are free to modify anything in this module, your implementation
//! must adhere to the project description regarding the components and their
//! communication.

#![allow(rustdoc::private_intra_doc_links)]

use std::sync::{Arc, RwLock};

use estimator::Estimator;
use ticket_sale_core::Config;

mod balancer;
mod coordinator;
mod database;
mod estimator;
mod priority;
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
/// added the tests
pub fn launch(config: &Config) -> Balancer {
    // Check if bonus functionality is requested in the config (not implemented)
    if config.bonus {
        todo!("Bonus not implemented!")
    }

    // The use of Arc is a ensures that multiple threads can own a reference to the same
    // data without risking data races or requiring additional synchronization
    // mechanisms. Create a new Database instance wrapped in an RwLock for
    // thread-safe read/write access (every thread can read, but exclusive writing.),
    let database = Arc::new(RwLock::new(Database::new(config.tickets)));

    // Create a new Coordinator instance wrapped in an Arc for shared ownership
    let coordinator = Arc::new(Coordinator::new(config.timeout, database.clone(), config));

    // Create a new Balancer instance, providing it with the Coordinator
    let balancer = Balancer::new(Some(coordinator.clone()));

    // Create a new Estimator instance, also wrapped in an Arc
    let estimator = Arc::new(Estimator::new(
        coordinator.clone(),             // Pass the Coordinator instance
        database.clone(),                // Pass the Database instance
        config.estimator_roundtrip_time, // Configuration for roundtrip time estimation
    ));

    // Start the Estimator to begin its operation
    estimator.start();

    // Return the Balancer instance to handle incoming requests
    balancer
}
