package com.pseuco.cp24;

/**
 * Configuration of the ticket sales system.
 */
public class Config {
    /**
     * Number of tickets initially available.
     */
    private final int numTickets;
    /**
     * Timeout of reservations in seconds.
     */
    private final int timeout;
    /**
     * Number of initial servers.
     */
    private final int initialServers;
    /**
     * Time in seconds the estimator takes to contact all servers.
     */
    private final int estimatorRoundtripTime;

    /**
     * Indicates whether to use the bonus implementation.
     */
    private final boolean bonus;

    /**
     * Constructs a new instance from the provided parameters.
     *
     * @param numTickets             Number of tickets initially available.
     * @param timeout                Timeout of reservations in seconds.
     * @param estimatorRoundtripTime Estimator roundtrip time in seconds.
     * @param bonus                  Flag indicating whether to use the bonus implementation.
     */
    public Config(final int numTickets, final int timeout, final int estimatorRoundtripTime, final boolean bonus) {
        this.numTickets = numTickets;
        this.timeout = timeout;
        // We just set this to two for now.
        this.initialServers = 2;
        this.estimatorRoundtripTime = estimatorRoundtripTime;
        this.bonus = bonus;
    }

    /**
     * Returns the number of tickets initially available.
     *
     * @return Number of tickets initially available.
     */
    public int getNumTickets() {
        return this.numTickets;
    }

    /**
     * Returns the timeout of reservations in seconds.
     *
     * @return Timeout of reservations in seconds.
     */
    public int getTimeout() {
        return this.timeout;
    }

    /**
     * Returns the number of initial servers.
     *
     * @return Number of initial servers.
     */
    public int getInitialServers() {
        return this.initialServers;
    }

    /**
     * Returns the time in seconds the estimator takes to contact all servers.
     *
     * @return Time in seconds the estimator takes to contact all servers.
     */
    public int getEstimatorRoundtripTime() {
        return this.estimatorRoundtripTime;
    }

    /**
     * Returns whether to use the Bonus implementation.
     *
     * @return Boolean indicating whether to use the bonus implementation.
     */
    public boolean getUseBonus() {
        return this.bonus;
    }
}
