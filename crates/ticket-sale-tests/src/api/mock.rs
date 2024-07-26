//! Mock API implementation directly using the `ticket-sale-rocket` crate

use std::sync::Arc;

use ticket_sale_core::{RawRequest, Request, RequestHandler, RequestKind};
use tokio::sync::oneshot;
use tokio::task::{self, JoinHandle};
use uuid::Uuid;

use super::{check_send_result, Api, RequestMsg, Response};

pub struct MockBalancer {
    balancer: Arc<ticket_sale_rocket::Balancer>,
    join_handles: Vec<JoinHandle<()>>,
}

struct MockRawRequest {
    payload: Option<u32>,
    kind: RequestKind,
    response_channel: oneshot::Sender<Response>,
}

pub async fn start(threads: u16, config: ticket_sale_core::Config) -> (MockBalancer, Api) {
    let balancer = Arc::new(
        tokio::task::spawn_blocking(move || ticket_sale_rocket::launch(&config))
            .await
            .unwrap(),
    );

    let it = (0..threads).map(|_| {
        let (sender, receiver) = flume::bounded::<RequestMsg>(65536);
        let balancer = balancer.clone();
        let handle = task::spawn_blocking(move || {
            let balancer = &*balancer;
            for msg in receiver.into_iter() {
                let raw = Box::new(MockRawRequest {
                    payload: msg.payload,
                    kind: msg.kind,
                    response_channel: msg.response_channel,
                });
                balancer.handle(Request::from_raw(
                    msg.kind,
                    msg.customer_id,
                    msg.server_id,
                    raw,
                ))
            }
        });
        (sender, handle)
    });
    let (senders, join_handles) = it.unzip();

    let mock_balancer = MockBalancer {
        balancer,
        join_handles,
    };
    (mock_balancer, Api::new(senders))
}

impl MockBalancer {
    pub async fn shutdown(self) {
        for handle in self.join_handles {
            handle.await.unwrap()
        }
        task::spawn_blocking(move || Arc::into_inner(self.balancer).unwrap().shutdown())
            .await
            .unwrap();
    }
}

impl RawRequest for MockRawRequest {
    fn url(&self) -> &str {
        use RequestKind::*;
        match self.kind {
            GetNumServers => "/api/admin/num_servers",
            SetNumServers => "/api/admin/num_servers",
            GetServers => "/api/admin/get_servers",
            NumAvailableTickets => "/api/num_available_tickets",
            ReserveTicket => "/api/reserve_ticket",
            BuyTicket => "/api/buy_ticket",
            AbortPurchase => "/api/abort_purchase",
            Debug => unreachable!(),
        }
    }

    fn method(&self) -> ticket_sale_core::RequestMethod {
        use ticket_sale_core::RequestMethod::*;
        use RequestKind::*;
        match self.kind {
            GetNumServers | GetServers | NumAvailableTickets => Get,
            _ => Post,
        }
    }

    fn read_bytes(&mut self) -> std::io::Result<Vec<u8>> {
        Ok(match self.payload.take() {
            None => Vec::new(),
            Some(i) => i.to_string().into_bytes(),
        })
    }
    fn read_string(&mut self) -> std::io::Result<String> {
        Ok(match self.payload.take() {
            None => String::new(),
            Some(i) => i.to_string(),
        })
    }
    fn read_u32(&mut self) -> Option<u32> {
        self.payload.take()
    }

    fn respond_with_err(self: Box<Self>, msg: String, customer_id: Uuid, server_id: Option<Uuid>) {
        let response = Response::Error {
            msg,
            server_id,
            customer_id,
        };
        check_send_result(self.response_channel.send(response))
    }

    fn respond_with_int(self: Box<Self>, i: u32, customer_id: Uuid, server_id: Option<Uuid>) {
        let response = Response::Int {
            i,
            server_id,
            customer_id,
        };
        check_send_result(self.response_channel.send(response))
    }

    fn respond_with_string(self: Box<Self>, s: String, customer_id: Uuid, server_id: Option<Uuid>) {
        panic!(
            "{:?} must not be answered with a string.\ncustomer: {customer_id:?}\nserver: {server_id:?}\nmessage: {s}",
            self.kind,
        )
    }

    fn respond_with_sold_out(self: Box<Self>, customer_id: Uuid, server_id: Option<Uuid>) {
        let response = Response::SoldOut {
            server_id,
            customer_id,
        };
        check_send_result(self.response_channel.send(response))
    }

    fn respond_with_server_list(self: Box<Self>, servers: &[Uuid]) {
        let response = Response::ServerList(servers.to_vec());
        check_send_result(self.response_channel.send(response))
    }
}
