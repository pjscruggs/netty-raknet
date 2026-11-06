package network.ycc.raknet.packet;

import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;

/** Announces the client endpoint and timing data during the reliable server handshake. */
public class ServerHandshake extends SimpleFramedPacket {

    /** Client endpoint observed by the server. */
    private InetSocketAddress clientAddr;
    /** Timestamp written first when encoding and overwritten by the second wire timestamp when decoding. */
    private long timestamp;
    /** Number of wildcard address slots included in the handshake. */
    private int nExtraAddresses;

    /** Creates a decoder target using reliable unordered delivery. */
    public ServerHandshake() {
        reliability = Reliability.RELIABLE;
    }

    /**
     * Creates a reliable handshake containing twenty wildcard address slots.
     *
     * @param clientAddr resolved client endpoint observed by the server
     * @param timestamp request timestamp to echo as the first wire timestamp
     */
    public ServerHandshake(final InetSocketAddress clientAddr, final long timestamp) {
        this(clientAddr, timestamp, 20);
    }

    /**
     * Creates a reliable handshake with an explicit wildcard-address count.
     *
     * @param clientAddr resolved client endpoint observed by the server
     * @param timestamp request timestamp to echo as the first wire timestamp
     * @param nExtraAddresses number of wildcard address slots to encode
     */
    public ServerHandshake(final InetSocketAddress clientAddr, final long timestamp, final int nExtraAddresses) {
        this();
        this.clientAddr = clientAddr;
        this.timestamp = timestamp;
        this.nExtraAddresses = nExtraAddresses;
    }

    /**
     * Writes client address, a zero system index, wildcard slots, request timestamp and current wall-clock milliseconds.
     *
     * @param buf caller-owned destination positioned after the packet ID
     */
    @Override
    public void encode(final ByteBuf buf) {
        writeAddress(buf, clientAddr);
        buf.writeShort(0);
        for (int i = 0; i < nExtraAddresses; i++) {
            writeAddress(buf);
        }
        buf.writeLong(timestamp);
        buf.writeLong(System.currentTimeMillis());
    }

    /**
     * Consumes the client address, system index, address slots and two timestamps.
     * The current decoder retains the second timestamp, replacing the first timestamp read.
     *
     * @param buf caller-owned source containing the complete handshake body
     */
    @Override
    public void decode(final ByteBuf buf) {
        clientAddr = readAddress(buf);
        buf.readShort();
        for (nExtraAddresses = 0; buf.readableBytes() > 16; nExtraAddresses++) {
            readAddress(buf);
        }
        timestamp = buf.readLong();
        timestamp = buf.readLong();
    }

    /**
     * Exposes the endpoint that the server observed for the connecting client.
     *
     * @return resolved client address, or null before initialization
     */
    public InetSocketAddress getClientAddr() {
        return clientAddr;
    }

    /**
     * Selects the observed client endpoint written at the start of the server handshake.
     *
     * @param clientAddr resolved client address and UDP port
     */
    public void setClientAddr(final InetSocketAddress clientAddr) {
        this.clientAddr = clientAddr;
    }

    /**
     * Exposes the retained handshake timestamp. Locally constructed packets hold the request timestamp.
     * Decoded packets hold the second wire timestamp, which this implementation writes as wall-clock milliseconds.
     *
     * @return retained timestamp in the clock domain of the corresponding wire field
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Selects the request timestamp written before the freshly sampled server timestamp.
     *
     * @param timestamp original request clock value to echo unchanged
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
