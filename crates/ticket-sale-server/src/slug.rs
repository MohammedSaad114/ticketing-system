//! 🐌 A sequential implementation for reference

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::time::Instant;

use parking_lot::Mutex;
use ticket_sale_core::Config;
use ticket_sale_core::{Request, RequestHandler, RequestKind};
use uuid::Uuid;

#[derive(Debug)]
struct Reservation {
    ticket: u32,
    reserved_at: Instant,
}

impl Reservation {
    #[inline]
    fn new(ticket: u32) -> Self {
        Self {
            ticket,
            reserved_at: Instant::now(),
        }
    }

    #[inline]
    fn age_secs(&self) -> u64 {
        self.reserved_at.elapsed().as_secs()
    }
}

struct ServerInner {
    /// The server's ID
    id: Uuid,

    /// List of available ticket IDs
    available_tickets: Vec<u32>,

    /// Reservations made by customers
    reservations: HashMap<Uuid, Reservation>,

    /// Reservation timeout in seconds
    reservation_timeout: u32,
}

impl ServerInner {
    /// Abort and remove expired reservations
    fn clear_reservations(&mut self) {
        self.reservations.retain(|_, res| {
            if res.age_secs() > self.reservation_timeout as u64 {
                self.available_tickets.push(res.ticket);
                false
            } else {
                true
            }
        });
    }
}

impl ServerInner {
    fn handle(&mut self, mut rq: Request) {
        self.clear_reservations();

        match rq.kind() {
            // In your implementation, the following requests will be handled by the coordinator.
            RequestKind::GetNumServers => {
                // In your implementation, you need to respond with the number
                // of servers.
                rq.respond_with_int(1);
            }
            RequestKind::SetNumServers => {
                if rq.read_u32().is_some() {
                    // In your implementation, you need to support this request
                    // for on-demand scaling. After scaling, you should respond
                    // with the number of servers.
                    rq.respond_with_err("Slug does not support on-demand scaling!");
                } else {
                    rq.respond_with_err("No number of servers provided!");
                }
            }
            RequestKind::GetServers => rq.respond_with_server_list(&[self.id]),

            // Handling the following requests will remain the Server's responsibility.
            RequestKind::NumAvailableTickets => {
                // This request requires us to respond with a server id
                rq.set_server_id(self.id);

                rq.respond_with_int(self.available_tickets.len() as u32)
            }
            RequestKind::ReserveTicket => {
                // This request requires us to respond with a server id
                rq.set_server_id(self.id);

                let customer = rq.customer_id();
                match self.reservations.entry(customer) {
                    Entry::Occupied(_) => {
                        // We do not allow a customer to reserve more than a
                        // ticket at a time.
                        rq.respond_with_err("A ticket has already been reserved!");
                    }
                    Entry::Vacant(entry) => {
                        // Try to take a ticket from the stack of available
                        // tickets and reserve it.
                        if let Some(ticket) = self.available_tickets.pop() {
                            entry.insert(Reservation::new(ticket));
                            // Respond with the id of the reserved ticket
                            rq.respond_with_int(ticket);
                        } else {
                            // Tell the client that no tickets are available.
                            rq.respond_with_sold_out();
                        }
                    }
                }
            }
            RequestKind::BuyTicket => {
                // This request requires us to respond with a server id
                rq.set_server_id(self.id);

                if let Some(ticket) = rq.read_u32() {
                    let cid = rq.customer_id();
                    if let Some(res) = self.reservations.get(&cid) {
                        if ticket != res.ticket {
                            // The id does not match the id of the reservation
                            rq.respond_with_err("Invalid ticket id provided!");
                        } else {
                            // Sell the ticket to the customer
                            self.reservations.remove(&cid);
                            rq.respond_with_int(ticket);
                        }
                    } else {
                        // Without a reservation there is nothing to buy.
                        rq.respond_with_err("No ticket has been reserved!")
                    }
                } else {
                    // The client is supposed to provide a ticket id.
                    rq.respond_with_err("No ticket id provided!");
                }
            }
            RequestKind::AbortPurchase => {
                // This request requires us to respond with a server id
                rq.set_server_id(self.id);

                if let Some(ticket) = rq.read_u32() {
                    let cid = rq.customer_id();
                    if let Some(res) = self.reservations.get(&cid) {
                        if ticket != res.ticket {
                            // The id does not match the id of the reservation.
                            rq.respond_with_err("Invalid ticket id provided!");
                        } else {
                            // Abort the reservation and put the ticket back on the stack.
                            self.reservations.remove(&cid);

                            self.available_tickets.push(ticket);
                            // Respond with the id of the formerly reserved ticket.
                            rq.respond_with_int(ticket);
                        }
                    } else {
                        // Without a reservation there is nothing to abort.
                        rq.respond_with_err("No ticket has been reserved!")
                    }
                } else {
                    // The client is supposed to provide a ticket id.
                    rq.respond_with_err("No ticket id provided!");
                }
            }

            // Debug requests you may freely use to debug your implementation.
            RequestKind::Debug => {
                // 📌 Hint: You can use `rq.url()` and `rq.method()` to
                // implement multiple debugging commands.
                rq.respond_with_string("This is 🐌.");
            }
        }
    }
}

/// A request handler processing requests sequentially
pub struct Server(Mutex<ServerInner>);

impl RequestHandler for Server {
    fn handle(&self, request: Request) {
        self.0.lock().handle(request)
    }

    fn shutdown(self) {
        // nothing to do
    }
}

impl Server {
    /// Create a new slug
    pub fn new(config: &Config) -> Self {
        let inner = ServerInner {
            id: Uuid::new_v4(), // random uuid
            available_tickets: (0..config.tickets).collect(),
            reservations: HashMap::new(),
            reservation_timeout: config.timeout,
        };
        Self(Mutex::new(inner))
    }
}
