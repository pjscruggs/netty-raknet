package network.ycc.raknet.packet;

import io.netty.buffer.ByteBuf;

/** Echoes a connected ping and optionally carries the responding peer's monotonic timestamp. */
public class Pong extends SimpleFramedPacket {

    /** Sender clock value echoed from the ping for local round-trip calculation. */
    private long pingTimestamp;
    /** Responder timestamp in nanoseconds, or zero when absent from a newly decoded pong. */
    private long pongTimestamp;

    /** Creates an unreliable decoder target with zero timestamps. */
    public Pong() {
        reliability = Reliability.UNRELIABLE;
    }

    /**
     * Creates a timestamped response using the delivery policy selected by the caller.
     *
     * @param pingTimestamp sender clock value copied unchanged from the ping
     * @param reliability transport delivery policy for the response
     */
    public Pong(final long pingTimestamp, final Reliability reliability) {
        this(pingTimestamp);
        this.reliability = reliability;
    }

    /**
     * Creates an unreliable response stamped with the responder's monotonic clock.
     *
     * @param pingTimestamp sender clock value copied unchanged from the ping
     */
    public Pong(final long pingTimestamp) {
        this();
        this.pingTimestamp = pingTimestamp;
        this.pongTimestamp = System.nanoTime();
    }

    /**
     * Appends the echoed ping timestamp followed by the responder timestamp.
     *
     * @param buf caller-owned destination positioned after the packet ID
     */
    @Override
    public void encode(final ByteBuf buf) {
        buf.writeLong(pingTimestamp);
        buf.writeLong(pongTimestamp);
    }

    /**
     * Reads the echoed timestamp and, if any bytes remain, reads a second 64-bit timestamp.
     *
     * @param buf caller-owned source containing one or two timestamps
     */
    @Override
    public void decode(final ByteBuf buf) {
        pingTimestamp = buf.readLong();
        if (buf.isReadable()) {
            pongTimestamp = buf.readLong();
        }
    }

    /**
     * Returns the original sender clock value used for round-trip measurement.
     *
     * @return unchanged timestamp echoed from the ping
     */
    public long getPingTimestamp() {
        return pingTimestamp;
    }

    /**
     * Returns the responder's clock sample without converting between peer clock domains.
     *
     * @return responder timestamp, initially zero if no second timestamp was decoded
     */
    public long getPongTimestamp() {
        return pongTimestamp;
    }

    /**
     * Subtracts the echoed ping timestamp from the current local monotonic clock.
     * This is meaningful only on the endpoint that created the original timestamp.
     *
     * @return elapsed nanoseconds since the original local ping
     */
    public long getRTT() {
        return System.nanoTime() - pingTimestamp;
    }

}