use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};

use ticket_sale_core::{Config, Request, RequestKind};
use uuid::Uuid;

use crate::database::Database;
use crate::estimator::Estimator;
use crate::server::Server;

pub struct Coordinator {
    database: Arc<RwLock<Database>>,
    servers: RwLock<HashMap<Uuid, Arc<RwLock<Server>>>>,
    running: AtomicBool,
    estimator: Option<Arc<Estimator>>,
}

impl Coordinator {
    pub fn new(reservation_timeout: u32, database: Arc<RwLock<Database>>, config: &Config) -> Self {
        let mut servers = HashMap::new();

        for _ in 0..config.initial_servers {
            let server = Arc::new(RwLock::new(Server::new(
                Arc::clone(&database),
                config.tickets,
                reservation_timeout,
            )));
            let server_id = server.read().unwrap().id().clone();
            servers.insert(server_id, server);
        }

        Self {
            database,
            servers: RwLock::new(servers),
            running: AtomicBool::new(true),
            estimator: None,
        }
    }

    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
        if let Some(estimator) = &self.estimator {
            estimator.start();
        }
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);

        if let Some(estimator) = &self.estimator {
            estimator.shutdown();
        }

        let servers = self.servers.read().unwrap();
        for (_, server) in servers.iter() {
            server.read().unwrap().shutdown();
        }

        println!("Coordinator is shutting down");
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn get_servers(&self) -> Vec<Uuid> {
        self.servers.read().unwrap().keys().cloned().collect()
    }

    pub fn get_server(&self, id: Uuid) -> Option<Arc<RwLock<Server>>> {
        self.servers.read().unwrap().get(&id).cloned()
    }

    pub fn adjust_server_count(&self, target_count: u32, config: &Config) -> u32 {
        let mut servers = self.servers.write().unwrap();
        let current_count = servers.len() as u32;

        if target_count > current_count {
            for _ in 0..(target_count - current_count) {
                let server = Arc::new(RwLock::new(Server::new(
                    Arc::clone(&self.database),
                    config.tickets,
                    config.timeout,
                )));
                let server_id = server.read().unwrap().id().clone();
                servers.insert(server_id, server);
            }
        } else if target_count < current_count {
            let remove_count = current_count - target_count;
            let mut to_remove = Vec::with_capacity(remove_count as usize);

            for (id, _) in servers.iter().take(remove_count as usize) {
                to_remove.push(*id);
            }

            for id in to_remove {
                servers.remove(&id);
            }
        }

        target_count
    }

    pub fn handle_request(&self, rq: Request) {
        if !self.is_running() {
            rq.respond_with_err("Coordinator is shutting down.");
            return;
        }

        match rq.kind() {
            RequestKind::NumAvailableTickets => {
                let servers = self.servers.read().unwrap();
                if let Some(server) = servers.values().next() {
                    server.write().unwrap().handle_request(rq);
                } else {
                    rq.respond_with_err("No servers available.");
                }
            }
            _ => {
                rq.respond_with_err("Unsupported request kind.");
            }
        }
    }

    pub fn set_estimator(&mut self, estimator: Arc<Estimator>) {
        self.estimator = Some(estimator);
    }
}
