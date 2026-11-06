package network.ycc.raknet.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.packet.Pong;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/** Feeds round-trip samples from unreliable packets into the connection's retransmission timing estimator. */
@ChannelHandler.Sharable
public class PongHandler extends SimpleChannelInboundHandler<Pong> {

    /** Pipeline name for consuming pong packets after framed packet decoding. */
    public static final String NAME = "rn-pong";
    /** Shared stateless pong consumer whose measurements remain connection-local. */
    public static final PongHandler INSTANCE = new PongHandler();

    /**
     * Updates round-trip estimates only from unreliable pongs, excluding retransmission delay from the sample.
     *
     * @param ctx connection holding the timing estimator
     * @param pong reply consumed by this handler whether or not it contributes a timing sample
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final Pong pong) {
        if (!pong.getReliability().isReliable) {
            final RakNet.Config config = RakNet.config(ctx);
            config.updateRTTNanos(pong.getRTT());
        }
    }

}
