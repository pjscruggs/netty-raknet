package network.ycc.raknet.config;

import network.ycc.raknet.RakNet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class DefaultMagic implements RakNet.Magic {

    //TODO: static cache?

    protected final byte[] magicData;

    public DefaultMagic(final byte[] magicData) {
        this.magicData = magicData;
    }

    public static DefaultMagic decode(final ByteBuf buf) {
        final byte[] magicData = new byte[16];
        buf.readBytes(magicData);
        return new DefaultMagic(magicData);
    }

    @Override
    public void write(final ByteBuf buf) {
        buf.writeBytes(magicData);
    }

    @Override
    public void read(final ByteBuf buf) {
        for (byte b : magicData) {
            if (buf.readByte() != b) {
                throw new MagicMismatchException();
            }
        }
    }

    @Override
    public void verify(final RakNet.Magic other) {
        final ByteBuf tmp = Unpooled.buffer(16);
        write(tmp);
        other.read(tmp);
    }

}
