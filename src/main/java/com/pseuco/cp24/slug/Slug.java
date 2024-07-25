package com.pseuco.cp24.slug;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import com.pseuco.cp24.Config;
import com.pseuco.cp24.request.CustomerId;
import com.pseuco.cp24.request.Request;
import com.pseuco.cp24.request.RequestHandler;
import com.pseuco.cp24.request.ServerId;

/**
 * A slow request handler processing requests sequentially.
 */
public class Slug implements RequestHandler {
    /**
     * Server ID of this single server implementation.
     */
    private final ServerId serverId = ServerId.generate();

    /**
     * Stack of tickets still available.
     */
    private final Stack<Ticket> available;

    /**
     * Timeout of reservations in seconds.
     */
    private final int timeout;

    /**
     * Reservations made by customers.
     */
    private Map<CustomerId, Reservation> reservations = new HashMap<>();

    /**
     * Constructs a new slug.
     *
     * @param config Options for the sales system.
     */
    public Slug(final Config config) {
        this.available = Ticket.generateStack(config.getNumTickets());
        this.timeout = config.getTimeout();
    }

    /**
     * Aborts and removes reservations with an expired timeout.
     */
    private void clearReservations() {
        this.reservations.values().removeIf(reservation -> {
            if (reservation.getAge() > this.timeout) {
                // Make the ticket available again.
                this.available.push(reservation.abort());
                return true;
            } else {
                return false;
            }
        });
    }

    @Override
    public synchronized void handle(final Request request) {
        // We first need to clear reservations with an expired timeout. In your
        // implementation, you need to do something equivalently locally on each
        // server.
        this.clearReservations();

        // Now, we can handle the request.
        switch (request.getKind()) {
            // This request is handled by the load balancer.
            case NUM_SERVERS -> {
                switch (request.getMethod()) {
                    case GET -> {
                        // In your implementation, you need to respond with the number of servers.
                        request.respondWithInt(1);
                    }
                    case POST -> {
                        final var numServers = request.readInt();
                        if (numServers.isEmpty()) {
                            // The client is supposed to provide a number of servers.
                            request.respondWithError("No number of servers provided!");
                        } else {
                            // In your implementation, you need to support this request for on-demand
                            // scaling. After
                            // scaling, you should respond with the number of servers.
                            request.respondWithError("Slug does not support on-demand scaling!");
                        }
                    }
                }
            }
            // This request is handled by the load balancer.
            case GET_SERVERS -> {
                // In your implementation, you need to respond with the IDs of the active
                // servers.
                final ServerId[] ids = { this.serverId };
                request.respondWithServerIds(Arrays.asList(ids));
            }

            // In your implementation, this request need to be redirected to a server.
            case NUM_AVAILABLE_TICKETS -> {
                // This request requires us to respond with a server ID.
                request.setServerId(this.serverId);
                // In your implementation, you respond with an approximation of the actual
                // number.
                request.respondWithInt(this.available.size());
            }

            // In your implementation, this request need to be redirected to a server.
            case RESERVE_TICKET -> {
                // This request requires us to respond with a server ID.
                request.setServerId(this.serverId);

                final var customer = request.getCustomerId();
                if (this.reservations.containsKey(customer)) {
                    // We do not allow a customer to reserve more than a ticket at a time.
                    request.respondWithError("A ticket has already been reserved!");
                } else if (!this.available.isEmpty()) {
                    // Take a ticket from the stack of available tickets and reserve it.
                    final var ticket = this.available.pop();
                    this.reservations.put(customer, new Reservation(ticket));
                    // Respond with the ID of the reserved ticket.
                    request.respondWithInt(ticket.getId());
                } else {
                    // Tell the client that no tickets are available.
                    request.respondWithSoldOut();
                }
            }
            // In your implementation, this request need to be redirected to a server.
            case ABORT_PURCHASE -> {
                // This request requires us to respond with a server ID.
                request.setServerId(this.serverId);

                final var customer = request.getCustomerId();
                if (!this.reservations.containsKey(customer)) {
                    // Without a reservation there is nothing to abort.
                    request.respondWithError("No ticket has been reserved!");
                } else {
                    final var reservation = this.reservations.get(customer);
                    final var ticketId = request.readInt();
                    if (ticketId.isEmpty()) {
                        // The client is supposed to provide a ticket ID.
                        request.respondWithError("No ticket ID provided!");
                    } else if (ticketId.get() == reservation.getTicketId()) {
                        // Abort the reservation and put the ticket back on the stack.
                        final var ticket = reservation.abort();
                        this.available.push(ticket);
                        this.reservations.remove(customer);
                        // Respond with the ID of the formerly reserved ticket.
                        request.respondWithInt(ticket.getId());
                    } else {
                        // ID does not match the ID of the reservation.
                        request.respondWithError("Invalid ticket ID provided!");
                    }
                }
            }
            // In your implementation, this request need to be redirected to a server.
            case BUY_TICKET -> {
                // This request requires us to respond with a server ID.
                request.setServerId(this.serverId);

                final var customer = request.getCustomerId();
                if (!this.reservations.containsKey(customer)) {
                    // Without a reservation there is nothing to buy.
                    request.respondWithError("No ticket has been reserved!");
                } else {
                    final var reservation = this.reservations.get(customer);
                    final var ticketId = request.readInt();
                    if (ticketId.isEmpty()) {
                        // The client is supposed to provide a ticket ID.
                        request.respondWithError("No ticket ID provided!");
                    } else if (ticketId.get() == reservation.getTicketId()) {
                        // Sell the ticket to the customer.
                        final var ticket = reservation.sell();
                        this.reservations.remove(customer);
                        // Respond with the ID of the sold ticket.
                        request.respondWithInt(ticket.getId());
                    } else {
                        // ID does not match the ID of the reservation.
                        request.respondWithError("Invalid ticket ID provided!");
                    }
                }
            }

            // Use this request for sending debug information of your choosing.
            case DEBUG -> {
                request.respondWithString("This is 🐌.");
            }
        }
    }

    @Override
    public void shutdown() {
        // nothing to do
    }
}
