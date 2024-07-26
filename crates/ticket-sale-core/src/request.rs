use std::io;

use uuid::Uuid;

/// Kind of the request
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(u8)]
pub enum RequestKind {
    /// Retrieve the number of active (i.e., non-terminating) servers for
    /// on-demand scaling
    ///
    /// 📌 Hint: Should be processed by the load balancer.
    GetNumServers,

    /// Scale the ticket sales system to the provided number of servers
    ///
    /// The response to this request includes the number of servers after the
    /// scaling is done (i.e., it should be the equal to the requested number of
    /// servers).
    ///
    /// 📌 Hint: Should be processed by the load balancer.
    SetNumServers,

    /// Retrieve a list of all servers which are active, i.e., not terminating
    ///
    /// 📌 Hint: Should be processed by the load balancer.
    GetServers,

    /// Retrieve an approximation of the number of available tickets
    ///
    /// 📌 Hint: Should be processed by a server.
    NumAvailableTickets,

    /// Reserve a ticket
    ///
    /// 📌 Hint: Should be processed by a server.
    ReserveTicket,

    /// Buy a previously reserved ticket
    ///
    /// 📌 Hint: Should be processed by a server.
    BuyTicket,

    /// Abort the purchase of a previously reserved ticket
    ///
    /// 📌 Hint: Should be processed by a server.
    AbortPurchase,

    /// Useful for sending information for debugging
    ///
    /// 📌 Hint: You can process this request however you like.
    Debug,
}

/// Request sent from a web browser
///
/// 📌 Hint: Your implementation primarily interacts with instances of this
/// class.
pub struct Request {
    kind: RequestKind,
    customer: Uuid,
    server: Option<Uuid>,
    raw: Box<dyn RawRequest + Send>,
}

impl std::fmt::Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Request")
            .field("kind", &self.kind)
            .field("customer", &self.customer)
            .field("server", &self.server)
            .field("raw", &format_args!(".."))
            .finish()
    }
}

/// HTTP request method
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum RequestMethod {
    /// GET request
    Get,
    /// POST request, may have a payload
    Post,
}

/// Interface for handling requests from a web browser
///
/// 📌 Hint: The load balancer must implement this trait.
pub trait RequestHandler {
    /// Handle a request from a web browser
    ///
    /// This method may be called concurrently from different threads.
    fn handle(&self, request: Request);

    /// Shut the ticket sales system down
    ///
    /// This method waits for all threads spawned for the ticket sales system
    /// (e.g., the servers and the estimator) to have terminated.
    fn shutdown(self);
}

/// A raw request, implemented by the HTTP server
///
/// 📌 Hint: You should not need to interact with this trait (unless you create
/// your own testing infrastructure).
pub trait RawRequest {
    /// Get the URL
    fn url(&self) -> &str;
    /// Get the request method
    fn method(&self) -> RequestMethod;

    /// Read the request body as bytes
    fn read_bytes(&mut self) -> io::Result<Vec<u8>>;
    /// Read the request body as string
    fn read_string(&mut self) -> io::Result<String>;
    /// Parse the request body as [`u32`] integer
    fn read_u32(&mut self) -> Option<u32>;

    /// Respond with an error message
    fn respond_with_err(self: Box<Self>, err: String, customer: Uuid, server: Option<Uuid>);
    /// Respond with a integer
    fn respond_with_int(self: Box<Self>, int: u32, customer: Uuid, server: Option<Uuid>);
    /// Respond with a string
    fn respond_with_string(self: Box<Self>, s: String, customer: Uuid, server: Option<Uuid>);
    /// Respond with “SOLD OUT”
    fn respond_with_sold_out(self: Box<Self>, customer: Uuid, server: Option<Uuid>);
    /// Respond with a server list
    fn respond_with_server_list(self: Box<Self>, servers: &[Uuid]);
}

impl Request {
    /// Get the request's kind
    #[inline]
    pub fn kind(&self) -> &RequestKind {
        &self.kind
    }

    /// Get the value of the server id header, if present
    #[inline]
    pub fn server_id(&self) -> Option<Uuid> {
        self.server
    }

    /// Set the server id for the response
    #[inline]
    pub fn set_server_id(&mut self, sid: Uuid) {
        self.server = Some(sid);
    }

    /// Get the customer's id
    ///
    /// If the customer did not send the corresponding HTTP header, it is
    /// randomly generated.
    #[inline]
    pub fn customer_id(&self) -> Uuid {
        self.customer
    }

    /// Get the request URL
    ///
    /// 📌 Hint: This method is only relevant if you want to implement custom
    /// debugging commands. Note that this returns the full URL, e.g.,
    /// `/api/debug/my-command`.
    #[inline]
    #[allow(unused)]
    pub fn url(&self) -> &str {
        self.raw.url()
    }

    /// Get the request method
    ///
    /// 📌 Hint: This method is only relevant if you want to implement custom
    /// debugging commands.
    #[inline]
    #[allow(unused)]
    pub fn method(&self) -> RequestMethod {
        self.raw.method()
    }

    /// Read an integer provided by the web browser (e.g., a ticket id or number
    /// of servers).
    ///
    /// In case the browser did not provide an integer (or some communication
    /// error happened), [`None`] is returned.
    ///
    /// 📌 Hint: This method has side effects and should be called only once per
    /// request.
    #[inline]
    pub fn read_u32(&mut self) -> Option<u32> {
        self.raw.read_u32()
    }

    /// Read the payload provided by the web browser as bytes
    ///
    /// Returns [`Err`] in case of a communication error. See
    /// [`std::io::Read::read_to_end()`] for more details.
    ///
    /// 📌 Hint: You should only use this method to implement custom debugging
    /// commands. For all standard requests, [`Self::read_u32()`] is more
    /// suitable. Like [`Self::read_u32()`], this method has side effects and
    /// should be called only once per request.
    #[inline]
    #[allow(unused)]
    pub fn read_bytes(&mut self) -> io::Result<Vec<u8>> {
        self.raw.read_bytes()
    }

    /// Read the payload provided by the web browser as a UTF-8 string
    ///
    /// Returns [`Err`] if the payload is invalid UTF-8 or in case of a
    /// communication error. See [`std::io::Read::read_to_string()`] for more
    /// details.
    ///
    /// 📌 Hint: You should only use this method to implement custom debugging
    /// commands. For all standard requests, [`Self::read_u32()`] is more
    /// suitable. Like [`Self::read_u32()`], this method has side effects and
    /// should be called only once per request.
    #[inline]
    #[allow(unused)]
    pub fn read_string(&mut self) -> io::Result<String> {
        self.raw.read_string()
    }

    /// Respond with an error indicating an invalid request to the client.
    ///
    /// This method blocks until the response has been sent.
    #[inline]
    pub fn respond_with_err(self, err: impl Into<String>) {
        self.raw
            .respond_with_err(err.into(), self.customer, self.server);
    }

    /// Respond with an integer, e.g., a ticket number or the number of servers.
    ///
    /// This method blocks until the response has been sent.
    #[inline]
    pub fn respond_with_int(self, int: u32) {
        self.raw.respond_with_int(int, self.customer, self.server);
    }

    /// Respond with an arbitrary string
    ///
    /// This method blocks until the response has been sent.
    #[inline]
    pub fn respond_with_string(self, s: impl Into<String>) {
        self.raw
            .respond_with_string(s.into(), self.customer, self.server);
    }

    /// Responds with the message `SOLD OUT`
    ///
    /// Use this method to respond to a reservation request when no tickets are
    /// available.
    ///
    /// This method blocks until the response has been sent.
    #[inline]
    pub fn respond_with_sold_out(self) {
        self.raw.respond_with_sold_out(self.customer, self.server);
    }

    /// Responds with a list of server ids
    ///
    /// Use this method to send a list of server ids to the client.
    #[inline]
    pub fn respond_with_server_list(self, servers: &[Uuid]) {
        self.raw.respond_with_server_list(servers)
    }

    /// Create a new request from a [`RawRequest`]
    ///
    /// 📌 Hint: Normally, there should not be a need to use this function
    /// (unless you create your own testing infrastructure).
    #[inline]
    pub fn from_raw(
        kind: RequestKind,
        customer: Uuid,
        server: Option<Uuid>,
        raw: Box<dyn RawRequest + Send>,
    ) -> Self {
        Self {
            kind,
            customer,
            server,
            raw,
        }
    }
}
