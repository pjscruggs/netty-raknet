package network.ycc.raknet.packet;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultMagic;

import io.netty.buffer.ByteBuf;

/** Returns the probed path MTU in the first offline server reply. */
public class ConnectionReply1 extends AbstractConnectionReply implements Packet {

    /** Creates an uninitialized reply for the packet decoder. */
    public ConnectionReply1() {
    }

    /**
     * Creates an acceptance reply with the negotiated transport settings.
     *
     * @param magic offline marker to encode
     * @param mtu negotiated maximum datagram size in bytes
     * @param serverId identity of the accepting server
     */
    public ConnectionReply1(final RakNet.Magic magic, final int mtu, final long serverId) {
        super(magic, mtu, serverId);
    }

    /**
     * Appends magic, server identity, a false legacy-security flag and the negotiated MTU. The packet ID is written by the codec.
     *
     * @param buf destination whose writer index advances, retained by the caller
     */
    @Override
    public void encode(final ByteBuf buf) {
        magic.write(buf);
        buf.writeLong(serverId);
        buf.writeBoolean(NEEDS_SECURITY);
        buf.writeShort(mtu);
    }

    /**
     * Reads magic and server identity, rejects legacy security, and reads the MTU as a signed short.
     *
     * @param buf source positioned after the packet ID, retained by the caller
     * @throws IllegalArgumentException if the peer requires the unsupported legacy RakNet security handshake
     */
    @Override
    public void decode(final ByteBuf buf) {
        magic = DefaultMagic.decode(buf);
        serverId = buf.readLong();
        if (buf.readBoolean()) {
            throw new IllegalArgumentException("No security support yet");
        }
        mtu = buf.readShort();
    }

}
