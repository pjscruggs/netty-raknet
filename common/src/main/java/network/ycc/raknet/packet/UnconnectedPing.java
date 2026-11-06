package network.ycc.raknet.packet;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultMagic;

import io.netty.buffer.ByteBuf;

public class UnconnectedPing extends SimplePacket implements Packet {

    private RakNet.Magic magic;
    private long clientTime;
    private long clientId;

    @Override
    public void encode(final ByteBuf buf) {
        buf.writeLong(clientTime);
        magic.write(buf);
        buf.writeLong(clientId);
    }

    @Override
    public void decode(final ByteBuf buf) {
        clientTime = buf.readLong();
        magic = DefaultMagic.decode(buf);
        clientId = buf.readLong();
    }

    public RakNet.Magic getMagic() {
        return magic;
    }

    public void setMagic(final RakNet.Magic magic) {
        this.magic = magic;
    }

    public long getClientTime() {
        return clientTime;
    }

    public void setClientTime(final long clientTime) {
        this.clientTime = clientTime;
    }

    public long getClientId() {
        return clientId;
    }

    public void setClientId(final long clientId) {
        this.clientId = clientId;
    }

}
