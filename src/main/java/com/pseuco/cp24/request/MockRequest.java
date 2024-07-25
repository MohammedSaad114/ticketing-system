package com.pseuco.cp24.request;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import com.pseuco.cp24.Config;
import com.pseuco.cp24.rocket.Rocket;

/**
 * <p>
 * Represents a request from the tester (via the Java Native Interface).
 * </p>
 */
public class MockRequest extends Request {

    /**
     * Pointer to the FFI response object.
     */
    private long responsePtr;

    /**
     * Mock request body.
     */
    private Optional<Integer> payload;

    private static Method rawKindToMethod(final int kind) {
        return switch (kind) {
            case 0 -> Method.GET; // GetNumServers
            case 1 -> Method.POST; // SetNumServers
            case 2 -> Method.GET; // GetServers
            case 3 -> Method.GET; // NumAvailableTickets
            case 4 -> Method.POST; // ReserveTicket
            case 5 -> Method.POST; // BuyTicket
            case 6 -> Method.POST; // AbortPurchase
            case 7 -> Method.POST; // Debug
            default -> throw new RuntimeException("Invalid raw request kind");
        };
    }

    private static Kind rawKindToKind(final int kind) {
        return switch (kind) {
            case 0 -> Kind.NUM_SERVERS; // get
            case 1 -> Kind.NUM_SERVERS; // set
            case 2 -> Kind.GET_SERVERS;
            case 3 -> Kind.NUM_AVAILABLE_TICKETS;
            case 4 -> Kind.RESERVE_TICKET;
            case 5 -> Kind.BUY_TICKET;
            case 6 -> Kind.ABORT_PURCHASE;
            case 7 -> Kind.DEBUG;
            default -> throw new RuntimeException("Invalid raw request kind");
        };
    }

    /**
     * Constructs a new mock request from the provided parameters.
     *
     * @param responsePtr Pointer to the FFI response object.
     * @param kind        Raw request kind.
     * @param customerL   Least significant bits of the {@link CustomerId}.
     * @param customerM   Most significant bits of the {@link CustomerId}.
     * @param hasServerId Whether the request contains a {@link ServerId}.
     * @param serverL     Least significant bits of the {@link ServerId}.
     * @param serverM     Most significant bits of the {@link ServerId}.
     * @param payload     Request body.
     */
    private MockRequest(final long responsePtr, final int kind, final long customerL, final long customerM,
            final boolean hasServerId, final long serverL, final long serverM, final int payload) {
        super(rawKindToMethod(kind), rawKindToKind(kind), new CustomerId(new UUID(customerM, customerL)),
                hasServerId ? Optional.of(new ServerId(new UUID(serverM, serverL))) : Optional.empty());
        this.payload = payload >= 0 ? Optional.of(payload) : Optional.empty();
        this.responsePtr = responsePtr;
    }

    @Override
    public Optional<Integer> readInt() {
        final var res = this.payload;
        this.payload = Optional.empty();
        return res;
    }

    @Override
    public synchronized void respondWithError(final String message) {
        if (this.responsePtr == 0)
            throw new RuntimeException("A request must not be answered twice");

        long serverL = 0;
        long serverM = 0;
        if (this.serverId.isPresent()) {
            final UUID sid = this.serverId.get().getUUID();
            serverL = sid.getLeastSignificantBits();
            serverM = sid.getMostSignificantBits();
        }
        final UUID cid = this.customerId.getUUID();
        final long customerL = cid.getLeastSignificantBits();
        final long customerM = cid.getMostSignificantBits();
        respondWithError(this.responsePtr, message, this.serverId.isPresent(), serverL, serverM, customerL, customerM);

        this.responsePtr = 0;
    }

    private static native void respondWithError(long responsePtr, String msg, boolean hasServerId, long serverL,
            long serverM, long customerL, long customerM);

    @Override
    public synchronized void respondWithInt(final int integer) {
        if (this.responsePtr == 0)
            throw new RuntimeException("A request must not be answered twice");

        long serverL = 0;
        long serverM = 0;
        if (this.serverId.isPresent()) {
            final UUID sid = this.serverId.get().getUUID();
            serverL = sid.getLeastSignificantBits();
            serverM = sid.getMostSignificantBits();
        }
        final UUID cid = this.customerId.getUUID();
        final long customerL = cid.getLeastSignificantBits();
        final long customerM = cid.getMostSignificantBits();
        respondWithInt(this.responsePtr, integer, this.serverId.isPresent(), serverL, serverM, customerL, customerM);

        this.responsePtr = 0;
    }

    private static native void respondWithInt(long responsePtr, int i, boolean hasServerId, long serverL, long serverM,
            long customerL, long customerM);

    @Override
    public void respondWithString(final String string) {
        throw new RuntimeException("Request must not be answered with a string");
    }

    @Override
    public synchronized void respondWithSoldOut() {
        if (this.responsePtr == 0)
            throw new RuntimeException("A request must not be answered twice");

        final UUID sid = this.serverId.get().getUUID();
        final long serverL = sid.getLeastSignificantBits();
        final long serverM = sid.getMostSignificantBits();
        final UUID cid = this.customerId.getUUID();
        final long customerL = cid.getLeastSignificantBits();
        final long customerM = cid.getMostSignificantBits();
        respondWithSoldOut(this.responsePtr, serverL, serverM, customerL, customerM);

        this.responsePtr = 0;
    }

    private static native void respondWithSoldOut(long responsePtr, long serverL, long serverM, long customerL,
            long customerM);

    @Override
    public synchronized void respondWithServerIds(final Iterable<ServerId> ids) {
        if (this.responsePtr == 0)
            throw new RuntimeException("A request must not be answered twice");

        int length = 0;
        for (@SuppressWarnings("unused")
        final var id : ids) {
            length++;
        }
        long sids[] = new long[length * 2];
        int i = 0;
        for (final var id : ids) {
            final var uuid = id.getUUID();
            sids[i] = uuid.getLeastSignificantBits();
            sids[i + 1] = uuid.getMostSignificantBits();
            i += 2;
        }
        respondWithServerIds(this.responsePtr, sids);

        this.responsePtr = 0;
    }

    private static native void respondWithServerIds(long responsePtr, long serverIds[]);

}

class RocketLauncher {

    /**
     * Mapping from thread groups to print channels
     */
    private static final Map<ThreadGroup, Long> printChannels = new HashMap<>();
    /**
     * Whether <code>System.setOut()</code> and <code>System.setErr()</code>
     * were called accordingly.
     *
     * Guarded by {@link printChannels}’ object lock
     */
    private static boolean setupDone = false;

    private RequestHandler balancer;
    private final ThreadGroup threadGroup;
    private final Thread[] balancerThreads;

    private RocketLauncher(final Config config, final long[] balancerContexts, long printChannel)
            throws InterruptedException {
        this.threadGroup = new ThreadGroup("test-" + Long.toString(printChannel, 16));
        this.balancerThreads = new Thread[balancerContexts.length];
        synchronized (RocketLauncher.printChannels) {
            if (!RocketLauncher.setupDone) {
                System.setOut(new PrintStream(new Output(false)));
                System.setErr(new PrintStream(new Output(true)));
                RocketLauncher.setupDone = true;
            }
            RocketLauncher.printChannels.put(this.threadGroup, printChannel);
        }

        // Launch the rocket from within the test’s `ThreadGroup`
        final var launchThread = new Thread(this.threadGroup, () -> {
            this.balancer = Rocket.launch(config);
        }, "launcher");
        launchThread.start();
        launchThread.join();

        /// Create balancer threads that are part of the test’s `ThreadGroup`
        for (int i = 0; i < balancerContexts.length; i++) {
            final var balancerContext = balancerContexts[i];
            final var balancerThread = new Thread(this.threadGroup, () -> {
                balancerMain(balancerContext, this.balancer);
            }, "balancer-" + i);
            balancerThread.start();
            this.balancerThreads[i] = balancerThread;
        }
    }

    private class Output extends OutputStream {
        private final boolean isError;

        Output(final boolean isError) {
            this.isError = isError;
        }

        private long channel() {
            var group = Thread.currentThread().getThreadGroup();
            while (group != null) {
                final var res = printChannels.get(group);
                if (res != null)
                    return res;
                group = group.getParent();
            }
            return 0;
        }

        @Override
        public void write(int b) throws IOException {
            printByte(channel(), this.isError, b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            Objects.checkFromIndexSize(off, len, b.length);
            print(channel(), this.isError, b, off, len);
        }
    }

    private boolean land() throws InterruptedException {
        for (final var thread : this.balancerThreads) {
            thread.join();
        }

        this.balancer.shutdown();

        final var current = Thread.currentThread();
        final int activeCount = this.threadGroup.activeCount();
        final boolean success = activeCount <= 1;

        if (!success) {
            System.out.println(
                "RequestHandler.shutdown() did not terminate all other threads. The following are still running:");
            final var threads = new Thread[activeCount];
            this.threadGroup.enumerate(threads);
            for (final var thread : threads) {
                if (thread == null || thread == current)
                    continue;
                System.out.println("Thread: " + thread.getName());
                System.out.println("----------- Stack Trace -----------");
                for (final var element : thread.getStackTrace()) {
                    System.out.println(element);
                }
                System.out.println("-----------------------------------");
            }
        }

        synchronized (RocketLauncher.printChannels) {
            RocketLauncher.printChannels.remove(this.threadGroup);
        }

        return success;
    }

    private static native void balancerMain(long context, RequestHandler handler);

    private static native void printByte(long printChannel, boolean isError, int b);

    private static native void print(long printChannel, boolean isError, byte[] b, int off, int len);

}
