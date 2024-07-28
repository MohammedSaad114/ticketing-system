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
/// ⚠️ This functions must not be renamed and its signature must not be changed.
pub fn launch(config: &Config) -> Balancer {
    if config.bonus {
        todo!("Bonus not implemented!")
    }

    let database = Arc::new(RwLock::new(Database::new(config.tickets)));
    let coordinator = Arc::new(Coordinator::new(config.timeout, database.clone()));
    let estimator = Estimator::new(database.clone(), config.estimator_roundtrip_time);

    let estimator_handle = estimator.start(coordinator.clone());

    for _ in 0..config.initial_servers {
        coordinator.add_server(config);
    }

    Balancer::new(coordinator.clone(), estimator_handle)
}
