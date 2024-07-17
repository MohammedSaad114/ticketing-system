use std::{str::FromStr, sync::Arc};

use eyre::Result;
use flume::Sender;
use nanorand::Rng;
use thiserror::Error;
use ticket_sale_core::RequestKind;
use tokio::sync::oneshot;
use uuid::Uuid;

pub mod jni;
pub mod mock;

#[derive(Debug, Error)]
#[error("Error 400: {0}")]
pub struct ApiError(String);

pub type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Debug)]
enum Response {
    Error {
        msg: String,
        server_id: Option<Uuid>,
        customer_id: Uuid,
    },
    Int {
        i: u32,
        server_id: Option<Uuid>,
        customer_id: Uuid,
    },
    SoldOut {
        server_id: Option<Uuid>,
        customer_id: Uuid,
    },
    ServerList(Vec<Uuid>),
}

impl Response {
    fn into_api_response_usize(self, rq_kind: RequestKind) -> ApiResponse<usize> {
        match self {
            Response::Error {
                msg,
                server_id,
                customer_id,
            } => ApiResponse {
                server_id,
                customer_id: Some(customer_id),
                result: Err(ApiError(msg)),
            },
            Response::Int {
                i,
                server_id,
                customer_id,
            } => ApiResponse {
                server_id,
                customer_id: Some(customer_id),
                result: Ok(i as usize),
            },
            resp => panic!("{rq_kind:?} must not be answered by {resp:?}"),
        }
    }

    fn into_api_response_u64(self, rq_kind: RequestKind) -> ApiResponse<u64> {
        match self {
            Response::Error {
                msg,
                server_id,
                customer_id,
            } => ApiResponse {
                server_id,
                customer_id: Some(customer_id),
                result: Err(ApiError(msg)),
            },
            Response::Int {
                i,
                server_id,
                customer_id,
            } => ApiResponse {
                server_id,
                customer_id: Some(customer_id),
                result: Ok(i as u64),
            },
            resp => panic!("{rq_kind:?} must not be answered by {resp:?}"),
        }
    }
}

struct RequestMsg {
    kind: RequestKind,
    payload: Option<u32>,
    customer_id: Uuid,
    server_id: Option<Uuid>,
    response_channel: oneshot::Sender<Response>,
}

pub struct Api {
    /// One channel per balancer thread
    channels: Arc<Vec<Sender<RequestMsg>>>,

    my_channel: Sender<RequestMsg>,
    my_index: usize,
}

impl Api {
    fn new(channels: Vec<Sender<RequestMsg>>) -> Self {
        let my_channel = channels[0].clone();
        Self {
            channels: Arc::new(channels),
            my_channel,
            my_index: 0,
        }
    }
}

impl Clone for Api {
    fn clone(&self) -> Self {
        let my_index = (self.my_index + 1) % self.channels.len();
        Self {
            channels: self.channels.clone(),
            my_channel: self.channels[my_index].clone(),
            my_index,
        }
    }
}

const NO_REQUEST_OPTIONS: RequestOptions = RequestOptions {
    server_id: None,
    customer_id: None,
};

impl Api {
    async fn make_request(
        &self,
        kind: RequestKind,
        payload: Option<u32>,
        options: &RequestOptions,
    ) -> Result<Response> {
        let (sender, receiver) = oneshot::channel();
        let msg = RequestMsg {
            kind,
            payload,
            customer_id: options.customer_id.unwrap_or_default(),
            server_id: options.server_id,
            response_channel: sender,
        };
        self.my_channel.send_async(msg).await?;
        Ok(receiver.await?)
    }

    pub async fn get_num_servers(&self) -> Result<ApiResponse<usize>> {
        let kind = RequestKind::GetNumServers;
        let response = self.make_request(kind, None, &NO_REQUEST_OPTIONS);
        Ok(response.await?.into_api_response_usize(kind))
    }

    pub async fn post_num_servers(&self, number: usize) -> Result<ApiResponse<usize>> {
        let kind = RequestKind::SetNumServers;
        let response = self.make_request(kind, Some(number as u32), &NO_REQUEST_OPTIONS);
        Ok(response.await?.into_api_response_usize(kind))
    }

    pub async fn get_servers(&self) -> Result<ApiResponse<Vec<Uuid>>> {
        let kind = RequestKind::GetServers;
        let response = self.make_request(kind, None, &NO_REQUEST_OPTIONS);
        Ok(match response.await? {
            Response::Error {
                msg,
                server_id,
                customer_id,
            } => ApiResponse {
                server_id,
                customer_id: Some(customer_id),
                result: Err(ApiError(msg)),
            },
            Response::ServerList(list) => ApiResponse {
                server_id: None,
                customer_id: None,
                result: Ok(list),
            },
            resp => panic!("{kind:?} must not be answered by {resp:?}"),
        })
    }

    pub async fn get_available_tickets(
        &self,
        options: &RequestOptions,
    ) -> Result<ApiResponse<u64>> {
        let kind = RequestKind::NumAvailableTickets;
        let response = self.make_request(kind, None, options);
        Ok(response.await?.into_api_response_u64(kind))
    }

    pub async fn reserve_ticket(
        &self,
        options: &RequestOptions,
    ) -> Result<ApiResponse<Reservation>> {
        let kind = RequestKind::ReserveTicket;
        let response = self.make_request(kind, None, options);
        Ok(match response.await? {
            Response::Error {
                msg,
                server_id,
                customer_id,
            } => ApiResponse {
                server_id,
                customer_id: Some(customer_id),
                result: Err(ApiError(msg)),
            },
            Response::Int {
                i,
                server_id,
                customer_id,
            } => ApiResponse {
                server_id,
                customer_id: Some(customer_id),
                result: Ok(Reservation::Reserved(i as u64)),
            },
            Response::SoldOut {
                server_id,
                customer_id,
            } => ApiResponse {
                server_id,
                customer_id: Some(customer_id),
                result: Ok(Reservation::SoldOut),
            },
            resp => panic!("{kind:?} must not be answered by {resp:?}"),
        })
    }

    pub async fn abort_purchase(
        &self,
        ticket_id: u64,
        options: &RequestOptions,
    ) -> Result<ApiResponse<u64>> {
        let kind = RequestKind::AbortPurchase;
        let response = self.make_request(kind, Some(ticket_id as u32), options);
        Ok(response.await?.into_api_response_u64(kind))
    }

    pub async fn buy_ticket(
        &self,
        ticket_id: u64,
        options: &RequestOptions,
    ) -> Result<ApiResponse<u64>> {
        let kind = RequestKind::BuyTicket;
        let response = self.make_request(kind, Some(ticket_id as u32), options);
        Ok(response.await?.into_api_response_u64(kind))
    }

    pub fn create_user_session(&self, server_id: Option<Uuid>) -> UserSession {
        let mut bytes = [0u8; 16];
        nanorand::tls_rng().fill(&mut bytes);
        UserSession {
            api: self,
            customer_id: uuid::Builder::from_random_bytes(bytes).into_uuid(),
            server_id,
            state: SessionState::None,
        }
    }
}

pub struct ApiResponse<T> {
    pub server_id: Option<Uuid>,
    pub customer_id: Option<Uuid>,
    pub result: ApiResult<T>,
}

impl<T> ApiResponse<T> {
    pub fn map_response<R, F: FnOnce(T) -> Result<R>>(self, func: F) -> Result<ApiResponse<R>> {
        let result = match self.result.map(func) {
            Ok(result) => Ok(result?),
            Err(err) => Err(err),
        };
        Ok(ApiResponse {
            server_id: self.server_id,
            customer_id: self.customer_id,
            result,
        })
    }
}

pub enum Reservation {
    SoldOut,
    Reserved(u64),
}

impl Reservation {
    pub fn reserved(&self) -> Result<u64> {
        match self {
            Reservation::SoldOut => Err(eyre::eyre!(
                "Reservation failed when it shall have succeeded."
            )),
            Reservation::Reserved(ticket_id) => Ok(*ticket_id),
        }
    }
}

impl FromStr for Reservation {
    type Err = eyre::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "SOLD OUT" => Ok(Self::SoldOut),
            s => Ok(Self::Reserved(s.parse()?)),
        }
    }
}

pub enum SessionState {
    None,
    Reserved(u64),
}

pub struct UserSession<'a> {
    pub api: &'a Api,
    pub customer_id: Uuid,
    pub server_id: Option<Uuid>,
    pub state: SessionState,
}

impl<'a> UserSession<'a> {
    fn request_options(&self) -> RequestOptions {
        RequestOptions {
            server_id: self.server_id,
            customer_id: Some(self.customer_id),
        }
    }

    fn process_response<T>(&mut self, response: ApiResponse<T>) -> ApiResponse<T> {
        self.server_id = response.server_id;
        response
    }

    pub async fn get_available_tickets(&mut self) -> Result<ApiResponse<u64>> {
        Ok(self.process_response(
            self.api
                .get_available_tickets(&self.request_options())
                .await?,
        ))
    }

    pub async fn reserve_ticket(&mut self) -> Result<ApiResponse<Reservation>> {
        let response: ApiResponse<Reservation> =
            self.process_response(self.api.reserve_ticket(&self.request_options()).await?);
        if let Ok(reservation) = &response.result {
            match reservation {
                Reservation::SoldOut => {
                    self.state = SessionState::None;
                }
                Reservation::Reserved(ticket_id) => {
                    self.state = SessionState::Reserved(*ticket_id);
                }
            }
        }
        Ok(response)
    }

    pub async fn abort_purchase(&mut self, ticket_id: u64) -> Result<ApiResponse<u64>> {
        Ok(self.process_response(
            self.api
                .abort_purchase(ticket_id, &self.request_options())
                .await?,
        ))
    }

    pub async fn buy_ticket(&mut self, ticket_id: u64) -> Result<ApiResponse<u64>> {
        Ok(self.process_response(
            self.api
                .buy_ticket(ticket_id, &self.request_options())
                .await?,
        ))
    }
}

#[derive(Copy, Clone, Default)]
pub struct RequestOptions {
    pub server_id: Option<Uuid>,
    pub customer_id: Option<Uuid>,
}

fn check_send_result<T: std::fmt::Debug>(result: Result<(), T>) {
    if let Err(err) = result {
        eprintln!("WARNING: Tester did not await response `{err:?}`");
    }
}
