package network.ycc.raknet.utils;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

/** Satisfies bootstrap handler requirements in tests that exercise only transport establishment. */
public class EmptyInit extends ChannelInitializer<Channel> {
    /**
     * Leaves the initialized transport pipeline intact without adding an application protocol.
     *
     * @param ch test channel whose handshake is the subject of the test
     */
    @Override
    protected void initChannel(final Channel ch) {
    }
}
