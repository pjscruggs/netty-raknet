package network.ycc.raknet.packet;

import network.ycc.raknet.RakNet;

public abstract class AbstractConnectionReply extends SimplePacket {

    protected static final boolean NEEDS_SECURITY = false;

    protected RakNet.Magic magic;
    protected int mtu;
    protected long serverId;

    public AbstractConnectionReply() {
    }

    public AbstractConnectionReply(final RakNet.Magic magic, final int mtu, final long serverId) {
        this.magic = magic;
        this.mtu = mtu;
        this.serverId = serverId;
    }

    public RakNet.Magic getMagic() {
        return magic;
    }

    public void setMagic(final RakNet.Magic magic) {
        this.magic = magic;
    }

    public int getMtu() {
        return mtu;
    }

    public void setMtu(final int mtu) {
        this.mtu = mtu;
    }

    public long getServerId() {
        return serverId;
    }

    public void setServerId(final long serverId) {
        this.serverId = serverId;
    }

}
