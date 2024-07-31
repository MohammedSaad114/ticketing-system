use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::{Coordinator, Database};

pub struct Estimator {
    coordinator: Arc<Coordinator>,
    database: Arc<RwLock<Database>>,
    roundtrip_secs: u32,
    running: Arc<AtomicBool>,
}

impl Estimator {
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

    pub fn start(&self) -> std::thread::JoinHandle<()> {
        let coordinator = self.coordinator.clone();
        let database = self.database.clone();
        let roundtrip_secs = self.roundtrip_secs;
        let running = self.running.clone();

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
                        server
                            .write()
                            .unwrap()
                            .update_estimate(db_available.try_into().unwrap());
                    }

                    std::thread::sleep(Duration::from_secs((roundtrip_secs / num_servers) as u64));
                }
            }
        })
    }

    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
        println!("Estimator is shutting down");
    }
}
