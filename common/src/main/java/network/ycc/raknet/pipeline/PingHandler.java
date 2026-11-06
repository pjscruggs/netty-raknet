package network.ycc.raknet.pipeline;

import network.ycc.raknet.packet.Ping;
import network.ycc.raknet.packet.Pong;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/** Answers transport pings using the sender's timestamp and delivery mode without maintaining connection state. */
@ChannelHandler.Sharable
public class PingHandler extends SimpleChannelInboundHandler<Ping> {

    /** Pipeline name for consuming ping packets after framed packet decoding. */
    public static final String NAME = "rn-ping";
    /** Shared stateless ping responder. */
    public static final PingHandler INSTANCE = new PingHandler();

    /**
     * Queues a pong preserving the received ping's timestamp and reliability policy.
     * The normal transport flush cycle sends the response, avoiding a separate flush per ping.
     *
     * @param ctx connection on which the pong is queued
     * @param ping ping consumed and released by the inbound handler framework
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final Ping ping) {
        ctx.write(new Pong(ping.getTimestamp(), ping.getReliability()));
    }

}
