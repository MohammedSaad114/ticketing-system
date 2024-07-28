use std::cmp::Ordering;
use std::collections::BinaryHeap;

use ticket_sale_core::{Request, RequestKind};

/// Struct to wrap a request with its priority
#[derive(Debug)]
pub struct PrioritizedRequest {
    pub priority: u8,
    pub request: Request,
}

impl PrioritizedRequest {
    pub fn new(priority: u8, request: Request) -> Self {
        Self { priority, request }
    }
}

impl PartialEq for PrioritizedRequest {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl Eq for PrioritizedRequest {}

impl PartialOrd for PrioritizedRequest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrioritizedRequest {
    fn cmp(&self, other: &Self) -> Ordering {
        other.priority.cmp(&self.priority)
    }
}

pub fn prioritize_request(request: &Request) -> u8 {
    match request.kind() {
        RequestKind::GetNumServers | RequestKind::SetNumServers | RequestKind::GetServers => 0,
        _ => 1,
    }
}

pub struct RequestQueue {
    queue: BinaryHeap<PrioritizedRequest>,
}

impl RequestQueue {
    pub fn new() -> Self {
        Self {
            queue: BinaryHeap::new(),
        }
    }

    pub fn push(&mut self, request: Request) {
        let priority = prioritize_request(&request);
        let prioritized_request = PrioritizedRequest::new(priority, request);
        self.queue.push(prioritized_request);
    }

    pub fn pop(&mut self) -> Option<Request> {
        self.queue.pop().map(|p| p.request)
    }
}
