//! Implementation of the estimator
//! estimator.rs

use std::sync::{Arc, Mutex};
use std::thread;

use super::coordinator::Coordinator;
use super::database::Database;

/// Estimator that estimates the number of tickets available overall
pub struct Estimator {
    coordinator: Arc<Coordinator>,
    database: Arc<Mutex<Database>>,
    roundtrip_secs: u32,
}

impl Estimator {
    /// The estimator's main routine.
    ///
    /// `roundtrip_secs` is the time in seconds the estimator needs to contact all
    /// servers. If there are `N` servers, then the estimator should wait
    /// `roundtrip_secs / N` between each server when collecting statistics.
    pub fn new(
        coordinator: Arc<Coordinator>,
        database: Arc<Mutex<Database>>,
        roundtrip_secs: u32,
    ) -> Self {
        Self {
            coordinator,
            database,
            roundtrip_secs,
        }
    }

    /// Start the estimator
    pub fn start(&self) {
        let coordinator = self.coordinator.clone();
        let database = self.database.clone();
        let roundtrip_secs = self.roundtrip_secs;

        thread::spawn(move || {
            loop {
                let servers = coordinator.get_servers();
                let num_servers = servers.len() as u32;

                let db_available = {
                    let db = database.lock().unwrap();
                    db.get_num_available()
                };

                for server_id in servers {
                    if let Some(server) = coordinator.get_server(server_id) {
                        server.lock().unwrap().update_estimate(db_available);
                    }

                    thread::sleep(std::time::Duration::from_secs(
                        (roundtrip_secs / num_servers).max(1) as u64,
                    ));
                }
            }
        });
    }
}
