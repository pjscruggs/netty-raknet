package network.ycc.raknet.packet;

import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;

/** Completes session establishment with endpoint information and the server timestamp being acknowledged. */
public class ClientHandshake extends SimpleFramedPacket {

    /** Timestamp copied from the server handshake. */
    private long pongTimestamp;
    /** Client monotonic-clock timestamp in nanoseconds when constructed locally. */
    private long timestamp;
    /** Resolved endpoint encoded at the start of the handshake. */
    private InetSocketAddress address;
    /** Number of wildcard address slots written after the endpoint. */
    private int nExtraAddresses;

    /** Creates a decoder target using reliable ordered delivery. */
    public ClientHandshake() {
        reliability = Reliability.RELIABLE_ORDERED;
    }

    /**
     * Creates a handshake response stamped with the local monotonic clock.
     *
     * @param pongTimestamp timestamp copied from the server handshake
     * @param address resolved endpoint to encode
     * @param nExtraAddresses number of wildcard address slots to append
     */
    public ClientHandshake(final long pongTimestamp, final InetSocketAddress address, final int nExtraAddresses) {
        this(pongTimestamp, System.nanoTime(), address, nExtraAddresses);
    }

    /**
     * Creates a response with explicit timestamps for protocol decoding or deterministic construction.
     *
     * @param pongTimestamp timestamp copied from the server handshake
     * @param timestamp client timestamp to encode unchanged
     * @param address resolved endpoint to encode
     * @param nExtraAddresses number of wildcard address slots to append
     */
    public ClientHandshake(final long pongTimestamp, final long timestamp, final InetSocketAddress address,
            final int nExtraAddresses) {
        this();
        this.pongTimestamp = pongTimestamp;
        this.timestamp = timestamp;
        this.address = address;
        this.nExtraAddresses = nExtraAddresses;
    }

    /**
     * Writes the endpoint, wildcard address slots, echoed server timestamp and client timestamp.
     *
     * @param buf caller-owned destination positioned after the packet ID
     */
    @Override
    public void encode(final ByteBuf buf) {
        writeAddress(buf, address);
        for (int i = 0; i < nExtraAddresses; i++) {
            writeAddress(buf);
        }
        buf.writeLong(pongTimestamp);
        buf.writeLong(timestamp);
    }

    /**
     * Reads the endpoint and counts address slots until the final two timestamps remain.
     * The intermediate addresses are consumed but not retained.
     *
     * @param buf caller-owned source containing the handshake body
     */
    @Override
    public void decode(final ByteBuf buf) {
        address = readAddress(buf);
        for (nExtraAddresses = 0; buf.readableBytes() > 16; nExtraAddresses++) {
            readAddress(buf);
        }
        pongTimestamp = buf.readLong();
        timestamp = buf.readLong();
    }

    /**
     * Exposes the server timestamp echoed by this client acknowledgement.
     *
     * @return unchanged timestamp copied from the received server handshake
     */
    public long getPongTimestamp() {
        return pongTimestamp;
    }

    /**
     * Selects the server timestamp echoed before the client timestamp.
     *
     * @param pongTimestamp value copied unchanged from the server handshake
     */
    public void setPongTimestamp(final long pongTimestamp) {
        this.pongTimestamp = pongTimestamp;
    }

    /**
     * Exposes the client timestamp appended after the echoed server timestamp.
     *
     * @return client clock value, using monotonic nanoseconds when constructed locally
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Selects the client timestamp written as the final handshake field.
     *
     * @param timestamp client clock value copied to the wire without conversion
     */
    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Reports how many intermediate address slots were decoded or will be written as wildcards.
     *
     * @return stored count of extra address slots
     */
    public int getnExtraAddresses() {
        return nExtraAddresses;
    }

    /**
     * Sets the number of wildcard slots to emit without retaining any original intermediate addresses.
     *
     * @param nExtraAddresses nonnegative count of extra address slots
     */
    public void setnExtraAddresses(final int nExtraAddresses) {
        this.nExtraAddresses = nExtraAddresses;
    }

}
