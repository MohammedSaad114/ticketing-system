use std::{
    collections::HashMap,
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex, RwLock,
    },
    thread::JoinHandle,
};

use ticket_sale_core::Request;
use uuid::Uuid;

use crate::server::Server;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerState {
    Running,
    Terminating,
    HasStopped,
}

/// Coordinator manages the servers and the database, handling server scaling and request
/// routing.
pub type ServerMap = Arc<
    RwLock<
        HashMap<
            Uuid,
            (
                Sender<ServerOrRequestMessage>,
                Arc<Mutex<ServerState>>,
                JoinHandle<()>,
            ),
        >,
    >,
>;

pub type ServerSpawn = (
    Arc<RwLock<Server>>,
    Sender<ServerOrRequestMessage>,
    Arc<Mutex<ServerState>>,
    JoinHandle<()>,
);
// Define the enum for messages that will be passed between the Balancer and Coordinator.
pub enum CoordinatorMessage {
    GetNumServers(Sender<u32>),
    SetNumServers(usize, Sender<u32>),
    GetServers(Sender<Vec<Uuid>>),
    GetTerminatingServers(Sender<Vec<Uuid>>),
    GetRunningAndTerminatingServers(Sender<(Vec<Uuid>, Vec<Uuid>)>),
    GetServerSender(Uuid, Sender<Sender<ServerOrRequestMessage>>),
    Shutdown,
}

pub enum Message<T> {
    HighPriority(T),
    NormalPriority(T),
}
#[derive(Debug)]

pub enum ServerOrRequestMessage {
    ServerMessage(ServerMessage),
    ClientRequest {
        request: Request,
        available_server: Option<Uuid>,
    },
}

// Define the enum for messages that the server can receive
#[derive(Debug, Clone)]
pub enum ServerMessage {
    ShutdownServer,  // Immediate shutdown
    TerminateServer, // Graceful termination
    UpdateTicketEstimate(u32),
    RequestTicketCount(Sender<u32>),
    CurrentState(Sender<ServerState>),
}

pub struct MessageQueue {
    high_priority_tx: Sender<ServerOrRequestMessage>,
    high_priority_rx: Receiver<ServerOrRequestMessage>,
    normal_priority_tx: Sender<ServerOrRequestMessage>,
    normal_priority_rx: Receiver<ServerOrRequestMessage>,
}

impl MessageQueue {
    pub fn new() -> Self {
        let (high_priority_tx, high_priority_rx) = channel();
        let (normal_priority_tx, normal_priority_rx) = channel();

        Self {
            high_priority_tx,
            high_priority_rx,
            normal_priority_tx,
            normal_priority_rx,
        }
    }

    pub fn send_high_priority(&self, msg: ServerOrRequestMessage) {
        self.high_priority_tx.send(msg).unwrap();
    }

    pub fn send_normal_priority(&self, msg: ServerOrRequestMessage) {
        self.normal_priority_tx.send(msg).unwrap();
    }

    pub fn receive(&self) -> Option<Message<ServerOrRequestMessage>> {
        // Try to receive from high-priority first
        if let Ok(msg) = self.high_priority_rx.try_recv() {
            Some(Message::HighPriority(msg))
        } else if let Ok(msg) = self.normal_priority_rx.try_recv() {
            // Otherwise, check normal-priority
            Some(Message::NormalPriority(msg))
        } else {
            None
        }
    }
}

