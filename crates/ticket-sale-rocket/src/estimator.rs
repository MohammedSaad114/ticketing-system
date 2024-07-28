//! Implementation of the estimator
//! estimator.rs

use std::sync::{atomic::Ordering, Arc, RwLock};
use std::time::Duration;

use super::coordinator::Coordinator;
use super::database::Database;
/// Estimator that estimates the number of tickets available overall
pub struct Estimator {
    database: Arc<RwLock<Database>>,
    roundtrip_secs: u32,
}

impl Estimator {
    /// The estimator's main routine.
    ///
    /// `roundtrip_secs` is the time in seconds the estimator needs to contact all
    /// servers. If there are `N` servers, then the estimator should wait
    /// `roundtrip_secs / N` between each server when collecting statistics.
    pub fn new(database: Arc<RwLock<Database>>, roundtrip_secs: u32) -> Self {
        Self {
            database,
            roundtrip_secs,
        }
    }

    pub fn start(&self, coordinator: Arc<Coordinator>) -> std::thread::JoinHandle<()> {
        let database = self.database.clone();
        let roundtrip_secs = self.roundtrip_secs;
        let running = coordinator.running();

        std::thread::spawn(move || {
            while running.load(Ordering::SeqCst) {
                let servers = coordinator.get_servers();
                let num_servers = servers.len() as u32;

                if num_servers == 0 {
                    std::thread::sleep(Duration::from_secs(1));
                    continue;
                }

                let db_available = {
                    let db = database.read().unwrap();
                    db.get_num_available()
                };

                for server_id in servers {
                    if let Some(server) = coordinator.get_server(server_id) {
                        server.write().unwrap().update_estimate(db_available);
                    }

                    std::thread::sleep(Duration::from_secs((roundtrip_secs / num_servers) as u64));
                }
            }
        })
    }
}
