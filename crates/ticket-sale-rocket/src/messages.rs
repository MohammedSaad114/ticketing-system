use std::sync::mpsc::Sender;

use uuid::Uuid;

// Define the enum for messages that will be passed between the Balancer and Coordinator.
pub enum CoordinatorMessage {
    GetNumServers(Sender<u32>),
    SetNumServers(usize, Sender<u32>),
    GetServers(Sender<Vec<Uuid>>),
    ServerUpdate(Uuid, u32), // Add this variant
    Shutdown,
}
