package network.ycc.raknet.packet;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultMagic;

import io.netty.buffer.ByteBuf;

public class UnconnectedPong extends SimplePacket implements Packet {

    private long clientTime = 0L;
    private long serverId = 0L;
    private RakNet.Magic magic;
    private String info = "";

    public UnconnectedPong() {
    }

    public UnconnectedPong(final long clientTime, final long serverId, final RakNet.Magic magic, final String info) {
        this.clientTime = clientTime;
        this.serverId = serverId;
        this.magic = magic;
        this.info = info;
    }

    @Override
    public void encode(final ByteBuf buf) {
        buf.writeLong(clientTime);
        buf.writeLong(serverId);
        magic.write(buf);
        writeString(buf, info);
    }

    @Override
    public void decode(final ByteBuf buf) {
        clientTime = buf.readLong();
        serverId = buf.readLong();
        magic = DefaultMagic.decode(buf);
        info = readString(buf);
    }

    public long getClientTime() {
        return clientTime;
    }

    public void setClientTime(final long clientTime) {
        this.clientTime = clientTime;
    }

    public long getServerId() {
        return serverId;
    }

    public void setServerId(final long serverId) {
        this.serverId = serverId;
    }

    public RakNet.Magic getMagic() {
        return magic;
    }

    public void setMagic(final RakNet.Magic magic) {
        this.magic = magic;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(final String info) {
        this.info = info;
    }

}
