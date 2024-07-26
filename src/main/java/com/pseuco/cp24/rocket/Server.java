package com.pseuco.cp24.rocket;

import com.pseuco.cp24.request.Request;
import com.pseuco.cp24.request.ServerId;

/**
 * Implements the server.
 */
public class Server implements Runnable {
    /**
     * The server's ID.
     */
    protected final ServerId id;

    /**
     * {@link Coordinator} of the ticket sales system.
     */
    protected final Coordinator coordinator;

    /**
     * Mailbox of the {@link Server}.
     */
    private final Mailbox<Command<Server>> mailbox = new Mailbox<>();

    /**
     * Constructs a new {@link Server}.
     *
     * @param id          ID of the server.
     * @param coordinator {@link Coordinator} of the ticket sales system.
     */
    public Server(final ServerId id, final Coordinator coordinator) {
        this.id = id;
        this.coordinator = coordinator;
    }

    /**
     * Returns the {@link Mailbox} of the server.
     *
     * @return The server's {@link Mailbox}.
     */
    public Mailbox<Command<Server>> getMailbox() {
        return this.mailbox;
    }

    @Override
    public void run() {
        /*
         * TODO: Implement the server as described in the project description. The
         * server will process the messages sent to its mailbox.
         */
        throw new RuntimeException("Not implemented!");
    }

    /**
     * A message containing a {@link Request} that should be processed by the
     * server.
     */
    public static class MsgProcessRequest implements Command<Server> {
        /**
         * Request that should be processed.
         */
        private final Request request;

        /**
         * Constructs a new {@link MsgProcessRequest} message.
         *
         * @param request The {@link Request} to process.
         */
        public MsgProcessRequest(final Request request) {
            this.request = request;
        }

        @Override
        public void execute(final Server srv) {
            /*
             * 📌 Hint: Use the 🐌 implementation as a basis.
             */
            throw new RuntimeException("Not implemented!");
        }
    }

    /**
     * This message is sent by the coordinator to shut down the server.
     */
    public static class MsgShutdown implements Command<Server> {
        /**
         * Explicit default constructor.
         */
        public MsgShutdown() {
        }

        @Override
        public void execute(final Server srv) {
            throw new RuntimeException("Not implemented!");
        }
    }

    /**
     * This message is periodically sent by the {@link Estimator} to every server to
     * inform each server about the number of available tickets excluding those
     * allocated to the respective server itself.
     */
    public static class MsgTicketsAvailable implements Command<Server> {
        private final int numAvailable;

        /**
         * Constructs a new {@link MsgTicketsAvailable} message.
         *
         * @param numAvailable Number of available tickets.
         */
        public MsgTicketsAvailable(final int numAvailable) {
            this.numAvailable = numAvailable;
        }

        @Override
        public void execute(final Server srv) {
            /**
             * TODO: Update the number of available tickets and respond to the estimator
             * with the tickets currently available but allocated to this server.
             */
            // To inform the estimator, you must sent a `MsgAvailableServer` to its
            // mailbox. You can obtain this mailbox as follows:
            // final var mailbox = obj.coordinator.getEstimatorMailbox();
            throw new RuntimeException("Not implemented!");

        }
    }
}
