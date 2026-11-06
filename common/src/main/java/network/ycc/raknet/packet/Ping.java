package network.ycc.raknet.packet;

import io.netty.buffer.ByteBuf;

/** Carries a sender-clock sample that a connected peer echoes for round-trip measurement. */
public class Ping extends SimpleFramedPacket {

    /** Opaque sender clock value, normally a local monotonic timestamp in nanoseconds. */
    protected long timestamp;

    /** Creates an unreliable probe stamped with the local monotonic clock. */
    public Ping() {
        timestamp = System.nanoTime();
        reliability = Reliability.UNRELIABLE;
    }

    /**
     * Creates a probe with an explicit timestamp and the base class's reliable ordered delivery.
     *
     * @param timestamp sender clock value to be echoed unchanged
     */
    public Ping(final long timestamp) {
        super();
        this.timestamp = timestamp;
    }

    /**
     * Creates a reliable unordered probe stamped with the local monotonic clock.
     *
     * @return new ping whose delivery policy requests retransmission
     */
    public static Ping newReliablePing() {
        final Ping out = new Ping();
        out.reliability = Reliability.RELIABLE;
        return out;
    }

    /**
     * Appends the 64-bit sender timestamp without changing the packet's clock value.
     *
     * @param buf caller-owned destination positioned after the packet ID
     */
    @Override
    public void encode(final ByteBuf buf) {
        buf.writeLong(timestamp);
    }

    /**
     * Reads the sender timestamp as an opaque 64-bit value.
     *
     * @param buf caller-owned source positioned after the packet ID
     */
    @Override
    public void decode(final ByteBuf buf) {
        timestamp = buf.readLong();
    }

    /**
     * Returns the clock value that the responding peer must echo unchanged.
     *
     * @return sender timestamp, normally measured in nanoseconds
     */
    public long getTimestamp() {
        return timestamp;
    }

}