package com.pseuco.cp24.request;

import java.util.Optional;

/**
 * <p>
 * Represents a request from a web browser.
 * </p>
 *
 * <p>
 * 📌 Hint: Your implementation primarily interacts with instances of this
 * class.
 * </p>
 *
 * <p>
 * Every request has a {@link Method} and a {@link Kind}. The {@link Method}
 * indicates whether a request is merely for retrieving information or may have
 * side effects like scaling the number of servers or reserving a ticket.
 * </p>
 */
public abstract class Request {

    /**
     * The <em>method</em> of the request.
     */
    public enum Method {
        /**
         * <p>
         * The GET method is used to retrieve some information from the system.
         * </p>
         *
         * <p>
         * A GET request must not have any side effects.
         * </p>
         */
        GET,
        /**
         * <p>
         * The POST method is used to send some information to the system.
         * </p>
         *
         * <p>
         * A POST request may have side effects (e.g., reserving a ticket).
         * </p>
         */
        POST;

        /**
         * Returns a {@link Method} based on its name.
         *
         * @param method Name of the method.
         *
         * @return An optional {@link Method} which is empty in case the string
         *         is invalid.
         */
        public static Optional<Method> fromName(final String method) {
            return switch (method) {
                case "GET" ->
                    Optional.of(Method.GET);
                case "POST" ->
                    Optional.of(Method.POST);
                default ->
                    Optional.empty();
            };
        }
    }

    /**
     * There are seven kinds of requests.
     */
    public enum Kind {
        /**
         * <p>
         * Retrieves or sets the number of active (i.e., non-terminating)
         * servers for on-demand scaling.
         * </p>
         *
         * <p>
         * 📌 Hint: Should be processed by the load balancer.
         * </p>
         */
        NUM_SERVERS,
        /**
         * <p>
         * Retrieves a list of all servers which are active, i.e., not
         * terminating.
         * </p>
         *
         * <p>
         * 📌 Hint: Should be processed by the load balancer.
         * </p>
         */
        GET_SERVERS,
        /**
         * <p>
         * Retrieves an approximation of the number of available tickets.
         * </p>
         *
         * <p>
         * 📌 Hint: Should be processed by a server.
         * </p>
         */
        NUM_AVAILABLE_TICKETS,
        /**
         * <p>
         * Reserves a ticket.
         * </p>
         *
         * <p>
         * 📌 Hint: Should be processed by a server.
         * </p>
         */
        RESERVE_TICKET,
        /**
         * <p>
         * Buys a previously reserved ticket.
         * </p>
         *
         * <p>
         * 📌 Hint: Should be processed by a server.
         * </p>
         */
        BUY_TICKET,
        /**
         * <p>
         * Aborts the purchase of a previously reserved ticket.
         * </p>
         *
         * <p>
         * 📌 Hint: Should be processed by a server.
         * </p>
         */
        ABORT_PURCHASE,
        /**
         * <p>
         * Useful for sending information for debugging.
         * </p>
         *
         * <p>
         * 📌 Hint: You can process this request however you like.
         * </p>
         */
        DEBUG;

        /**
         * Returns a {@link Kind} based on its path.
         *
         * @param path The path.
         *
         * @return An optional {@link Kind} which is empty in case the path is
         *         invalid.
         */
        public static Optional<Kind> fromPath(final String path) {
            return switch (path) {
                case "/api/admin/num_servers" -> Optional.of(Kind.NUM_SERVERS);
                case "/api/admin/get_servers" -> Optional.of(Kind.GET_SERVERS);

                case "/api/num_available_tickets" -> Optional.of(Kind.NUM_AVAILABLE_TICKETS);

                case "/api/reserve_ticket" -> Optional.of(Kind.RESERVE_TICKET);
                case "/api/buy_ticket" -> Optional.of(Kind.BUY_TICKET);
                case "/api/abort_purchase" -> Optional.of(Kind.ABORT_PURCHASE);

                default -> {
                    if (path.startsWith("/api/debug")) {
                        yield Optional.of(Kind.DEBUG);
                    }
                    yield Optional.empty();
                }
            };
        }
    }

    /**
     * Method of the request.
     */
    protected final Method method;
    /**
     * Kind of the request.
     */
    protected final Kind kind;

    /**
     * Customer ID associated with the request.
     */
    protected final CustomerId customerId;

    /**
     * An optional server ID associated with the request.
     */
    protected Optional<ServerId> serverId;

    /**
     * Constructs a new request from the provided parameters.
     *
     * @param method     Method of the request.
     * @param kind       Kind of the request.
     * @param customerId {@link CustomerId} for the request.
     * @param serverId   {@link ServerId} for the request.
     */
    public Request(final Method method, final Kind kind, final CustomerId customerId,
            final Optional<ServerId> serverId) {
        this.method = method;
        this.kind = kind;
        this.customerId = customerId;
        this.serverId = serverId;
    }

    /**
     * Returns the {@link Method} of the request.
     *
     * @return {@link Method} of the request.
     */
    public Method getMethod() {
        return this.method;
    }

    /**
     * Returns the {@link Kind} of the request.
     *
     * @return {@link Kind} of the request.
     */
    public Kind getKind() {
        return this.kind;
    }

    /**
     * Returns the path of the request.
     *
     * @return Path of the request.
     */
    public String getPath() {
        return switch (this.kind) {
            case NUM_SERVERS -> "/api/admin/num_servers";
            case GET_SERVERS -> "/api/admin/get_servers";
            case NUM_AVAILABLE_TICKETS -> "/api/num_available_tickets";
            case RESERVE_TICKET -> "/api/reserve_ticket";
            case BUY_TICKET -> "/api/buy_ticket";
            case ABORT_PURCHASE -> "/api/abort_purchase";
            case DEBUG -> "/api/debug";
        };
    }

    /**
     * Returns the {@link CustomerId} associated with the request.
     *
     * @return {@link CustomerId} associated with the request.
     */
    public CustomerId getCustomerId() {
        return this.customerId;
    }

    /**
     * Returns the {@link ServerId} associated with the request if there is any.
     *
     * @return {@link ServerId} associated with the request if there is any.
     */
    public Optional<ServerId> getServerId() {
        return this.serverId;
    }

    /**
     * <p>
     * Sets the server ID.
     * </p>
     *
     * <p>
     * This method should be called before responding to the request.
     * </p>
     *
     * <p>
     * 📌 Hint: Call this method to assign a server to a client.
     * </p>
     *
     * @param serverId {@link ServerId} to set.
     */
    public void setServerId(final ServerId serverId) {
        this.serverId = Optional.of(serverId);
    }

    /**
     * <p>
     * Reads an integer provided by the web browser (e.g., a ticket ID or number
     * of servers).
     * </p>
     *
     * <p>
     * In case the browser did not provide an integer, an empty {@link Optional}
     * is returned.
     * </p>
     *
     * <p>
     * 📌 Hint: This method has side effects and should be called only once on
     * each request.
     * </p>
     *
     * @return Integer if there is any.
     */
    public abstract Optional<Integer> readInt();

    /**
     * <p>
     * Responds with an error indicating an invalid request to the client.
     * </p>
     *
     * <p>
     * This method blocks until the response has been sent.
     * </p>
     *
     * @param message An optional error message to be sent to the client.
     */
    public abstract void respondWithError(final String message);

    /**
     * <p>
     * Responds with an integer, e.g., a ticket number or the number of servers.
     * </p>
     *
     * <p>
     * This method blocks until the response has been sent.
     * </p>
     *
     * @param integer Integer to be sent to the client.
     */
    public abstract void respondWithInt(final int integer);

    /**
     * <p>
     * Responds with an arbitrary string.
     * </p>
     *
     * <p>
     * This method blocks until the response has been sent.
     * </p>
     *
     * @param string String to respond with.
     */
    public abstract void respondWithString(final String string);

    /**
     * <p>
     * Responds with the message `SOLD OUT`.
     * </p>
     *
     * <p>
     * Use this method to respond to a reservation request when no tickets
     * available.
     * </p>
     *
     * <p>
     * This method blocks until the response has been sent.
     * </p>
     */
    public abstract void respondWithSoldOut();

    /**
     * <p>
     * Responds with a list of server IDs.
     * </p>
     *
     * <p>
     * Use this method to send a list of server IDs to the client.
     * </p>
     *
     * <p>
     * This method blocks until the response has been sent.
     * </p>
     *
     * @param ids An {@link Iterable} of server IDs.
     */
    public abstract void respondWithServerIds(final Iterable<ServerId> ids);
}
