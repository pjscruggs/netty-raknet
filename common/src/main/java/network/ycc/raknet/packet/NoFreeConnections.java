package network.ycc.raknet.packet;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultMagic;

import io.netty.buffer.ByteBuf;

public class NoFreeConnections extends SimplePacket implements Packet {

    private RakNet.Magic magic;
    private long serverId;

    public NoFreeConnections() {

    }

    public NoFreeConnections(final RakNet.Magic magic, final long serverId) {
        this.magic = magic;
        this.serverId = serverId;
    }

    @Override
    public void encode(final ByteBuf buf) {
        magic.write(buf);
        buf.writeLong(serverId);
    }

    @Override
    public void decode(final ByteBuf buf) {
        magic = DefaultMagic.decode(buf);
        serverId = buf.readLong();
    }

    public RakNet.Magic getMagic() {
        return magic;
    }

    public void setMagic(final RakNet.Magic magic) {
        this.magic = magic;
    }

    public long getServerId() {
        return serverId;
    }

    public void setServerId(final long serverId) {
        this.serverId = serverId;
    }

}
