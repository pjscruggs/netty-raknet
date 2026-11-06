package network.ycc.raknet.packet;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultMagic;

import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;

/** Confirms the probed MTU and identifies the client and destination for session establishment. */
public class ConnectionRequest2 extends SimplePacket implements Packet, Packet.ClientIdConnection {

    /** Offline-message marker carried by the request. */
    private RakNet.Magic magic;
    /** Negotiated MTU encoded as an unsigned 16-bit value. */
    private int mtu;
    /** Identifier used to bind subsequent handshake messages to this client. */
    private long clientId;
    /** Resolved server endpoint the client is requesting. */
    private InetSocketAddress address;

    /** Creates an uninitialized second request for the decoder. */
    public ConnectionRequest2() {
    }

    /**
     * Creates the second offline request with its negotiated endpoint settings.
     *
     * @param magic offline marker to encode
     * @param mtu negotiated path MTU in bytes
     * @param clientId identifier of the initiating client
     * @param address resolved server endpoint
     */
    public ConnectionRequest2(final RakNet.Magic magic, final int mtu, final long clientId,
            final InetSocketAddress address) {
        this.magic = magic;
        this.mtu = mtu;
        this.clientId = clientId;
        this.address = address;
    }

    /**
     * Appends magic, server address, MTU and client identifier in that order. The codec writes the packet ID separately.
     *
     * @param buf destination whose writer index advances, retained by the caller
     */
    @Override
    public void encode(final ByteBuf buf) {
        magic.write(buf);
        writeAddress(buf, address);
        buf.writeShort(mtu);
        buf.writeLong(clientId);
    }

    /**
     * Reads the marker, server address, unsigned MTU and client identifier. The packet ID must already have been consumed.
     *
     * @param buf source whose reader index advances, retained by the caller
     */
    @Override
    public void decode(final ByteBuf buf) {
        magic = DefaultMagic.decode(buf);
        address = readAddress(buf);
        mtu = buf.readUnsignedShort();
        clientId = buf.readLong();
    }

    /**
     * Exposes the offline marker carried by this packet without validating it against local configuration.
     *
     * @return stored marker, or null before initialization
     */
    public RakNet.Magic getMagic() {
        return magic;
    }

    /**
     * Selects the marker written by subsequent encodes, retaining the supplied object by reference.
     *
     * @param magic non-null offline marker whose contents remain stable while encoding
     */
    public void setMagic(final RakNet.Magic magic) {
        this.magic = magic;
    }

    /**
     * Reports the datagram size proposed in the second negotiation request.
     *
     * @return stored MTU in bytes, decoded from an unsigned 16-bit field
     */
    public int getMtu() {
        return mtu;
    }

    /**
     * Sets the negotiated size announced to the server without enforcing a transport limit.
     *
     * @param mtu datagram size in bytes that must fit the unsigned 16-bit wire field
     */
    public void setMtu(final int mtu) {
        this.mtu = mtu;
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
     * Exposes the server endpoint announced in the second negotiation request.
     *
     * @return destination endpoint, or null before initialization
     */
    public InetSocketAddress getAddress() {
        return address;
    }

    /**
     * Selects the server endpoint serialized after the offline marker.
     *
     * @param address resolved server address and UDP port
     */
    public void setAddress(final InetSocketAddress address) {
        this.address = address;
    }

}
