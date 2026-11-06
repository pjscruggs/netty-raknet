package network.ycc.raknet.packet;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultMagic;

import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;

/** Returns the accepted endpoint and MTU in the second offline server reply. */
public class ConnectionReply2 extends AbstractConnectionReply implements Packet {

    /** Client endpoint observed by the server, or null when encoding a wildcard address. */
    private InetSocketAddress address;

    /** Creates an uninitialized reply for the packet decoder. */
    public ConnectionReply2() {
    }

    /**
     * Creates an acceptance reply with the negotiated transport settings.
     *
     * @param magic offline marker to encode
     * @param mtu negotiated maximum datagram size in bytes
     * @param serverId identity of the accepting server
     * @param address client endpoint observed by the server, or null for a wildcard address
     */
    public ConnectionReply2(final RakNet.Magic magic, final int mtu, final long serverId, final InetSocketAddress address) {
        super(magic, mtu, serverId);
        this.address = address;
    }

    /**
     * Appends magic, server identity, client address, MTU and a false legacy-security flag.
     * A null client address is encoded as the wildcard endpoint. The packet ID is written by the codec.
     *
     * @param buf destination whose writer index advances, retained by the caller
     */
    @Override
    public void encode(final ByteBuf buf) {
        magic.write(buf);
        buf.writeLong(serverId);
        if (address == null) {
            writeAddress(buf);
        } else {
            writeAddress(buf, address);
        }
        buf.writeShort(mtu);
        buf.writeBoolean(NEEDS_SECURITY);
    }

    /**
     * Reads magic, server identity, client address and a signed-short MTU, then rejects legacy security.
     *
     * @param buf source positioned after the packet ID, retained by the caller
     * @throws IllegalArgumentException if the peer requires the unsupported legacy RakNet security handshake
     */
    @Override
    public void decode(final ByteBuf buf) {
        magic = DefaultMagic.decode(buf);
        serverId = buf.readLong();
        address = readAddress(buf);
        mtu = buf.readShort();
        if (buf.readBoolean()) {
            throw new IllegalArgumentException("No security support yet"); //TODO: security i guess?
        }
    }

    /**
     * Exposes the client endpoint returned by the server after negotiation.
     *
     * @return stored client endpoint, or null when a wildcard will be encoded
     */
    public InetSocketAddress getAddress() {
        return address;
    }

    /**
     * Selects the client endpoint returned in this reply.
     *
     * @param address resolved endpoint, or null to encode a wildcard address with port zero
     */
    public void setAddress(final InetSocketAddress address) {
        this.address = address;
    }

}
