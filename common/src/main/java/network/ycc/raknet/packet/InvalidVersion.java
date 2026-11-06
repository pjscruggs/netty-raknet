package network.ycc.raknet.packet;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultMagic;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

public class InvalidVersion extends SimplePacket implements Packet {

    private RakNet.Magic magic;
    private int version;
    private long serverId;
    public InvalidVersion() {

    }

    public InvalidVersion(final RakNet.Magic magic, final long serverId) {
        this.magic = magic;
        this.serverId = serverId;
    }

    @Override
    public void encode(final ByteBuf buf) {
        buf.writeByte(version);
        magic.write(buf);
        buf.writeLong(serverId);
    }

    @Override
    public void decode(final ByteBuf buf) {
        version = buf.readUnsignedByte();
        magic = DefaultMagic.decode(buf);
        serverId = buf.readLong();
    }

    public RakNet.Magic getMagic() {
        return magic;
    }

    public void setMagic(final RakNet.Magic magic) {
        this.magic = magic;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(final int version) {
        this.version = version;
    }

    public long getServerId() {
        return serverId;
    }

    public void setServerId(final long serverId) {
        this.serverId = serverId;
    }

    public static class InvalidVersionException extends DecoderException {
        public static final long serialVersionUID = 590681756L;

        public InvalidVersionException() {
            super("Incorrect RakNet version");
        }
    }

}
