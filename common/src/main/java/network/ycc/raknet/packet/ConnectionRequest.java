package network.ycc.raknet.packet;

import io.netty.buffer.ByteBuf;

/** Starts the reliable phase of connection establishment with a client identifier and timestamp. */
public class ConnectionRequest extends SimpleFramedPacket implements Packet.ClientIdConnection {

    /** Identifier used to bind this request to the initiating client. */
    protected long clientId;
    /** Client timestamp echoed during the handshake, locally generated in nanoseconds. */
    protected long timestamp;

    /** Creates a decoder target using reliable unordered delivery. */
    public ConnectionRequest() {
        reliability = Reliability.RELIABLE;
    }

    /**
     * Creates a reliable request stamped with the local monotonic clock.
     *
     * @param clientId identifier of the initiating client
     */
    public ConnectionRequest(final long clientId) {
        this();
        this.clientId = clientId;
        this.timestamp = System.nanoTime();
    }

    /**
     * Appends client identity, timestamp and a false legacy-security flag. The codec writes the packet ID separately.
     *
     * @param buf destination whose writer index advances, retained by the caller
     */
    @Override
    public void encode(final ByteBuf buf) {
        buf.writeLong(clientId);
        buf.writeLong(timestamp);
        buf.writeBoolean(false);
    }

    /**
     * Reads client identity and timestamp, then consumes the legacy-security flag without enforcing it. The packet ID must already have been consumed.
     *
     * @param buf source whose reader index advances, retained by the caller
     */
    @Override
    public void decode(final ByteBuf buf) {
        clientId = buf.readLong(); //client id
        timestamp = buf.readLong();
        buf.readBoolean(); //use security
    }

    /**
     * Exposes the client identity supplied with this request. The value alone does not authenticate the client.
     *
     * @return opaque 64-bit client identifier
     */
    @Override
    public long getClientId() {
        return clientId;
    }

    /**
     * Stores the client identity to announce when this request is encoded.
     *
     * @param clientId opaque identifier matching the other messages in this connection attempt
     */
    public void setClientId(final long clientId) {
        this.clientId = clientId;
    }

    /**
     * Exposes the request timestamp that the server handshake is expected to echo.
     *
     * @return opaque peer timestamp, or local monotonic nanoseconds for a locally constructed request
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Replaces the request timestamp without converting between clock domains.
     *
     * @param timestamp value to echo through the handshake
     */
    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

}
