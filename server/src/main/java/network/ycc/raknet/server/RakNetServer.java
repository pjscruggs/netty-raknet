package network.ycc.raknet.server;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.pipeline.FlushTickHandler;
import network.ycc.raknet.pipeline.RawPacketCodec;
import network.ycc.raknet.server.channel.RakNetServerChannel;
import network.ycc.raknet.server.pipeline.DatagramConsumer;
import network.ycc.raknet.server.pipeline.ConnectionInitializer;
import network.ycc.raknet.server.pipeline.ConnectionListener;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;

/**
 * Supplies the server channel and separate initializers for listening sockets and accepted peers.
 */
public final class RakNetServer extends RakNet {

    /**
     * ServerBootstrap channel type that multiplexes peer connections over a UDP listener.
     */
    public static final Class<RakNetServerChannel> CHANNEL = RakNetServerChannel.class;

    /**
     * Installs offline connection admission and discards datagrams that no earlier handler accepts.
     */
    public static class DefaultDatagramInitializer extends ChannelInitializer<Channel> {
        /**
         * Reusable listener initializer that creates a fresh admission handler for each listening channel.
         */
        public static final ChannelInitializer<Channel> INSTANCE = new DefaultDatagramInitializer();

        /**
         * Installs connection admission now and defers the trailing datagram consumer to the event loop.
         * Deferral lets ServerBootstrap insert its acceptor before unmatched datagrams are discarded.
         *
         * @param channel listening server channel whose bootstrap registration is in progress
         */
        @Override
        protected void initChannel(final Channel channel) {
            channel.pipeline().addLast(ConnectionListener.NAME, new ConnectionListener());
            //must defer so we can add it before the ServerBootstrap acceptor
            channel.eventLoop().execute(() ->
                    channel.pipeline().addLast(DatagramConsumer.NAME, DatagramConsumer.INSTANCE));
        }
    }

    /**
     * Builds the reliable transport pipeline for one peer before installing its server handshake.
     */
    public static class DefaultChildInitializer extends ChannelInitializer<Channel> {
        /**
         * Reusable child initializer whose stateful framing and flush handlers are created per connection.
         */
        public static final ChannelInitializer<Channel> INSTANCE = new DefaultChildInitializer();

        /**
         * Adds wire decoding, reliability and packet handling around the reserved handshake slot.
         * The child channel replaces that slot with its connection-specific initializer during registration.
         *
         * @param channel peer transport channel receiving its initial pipeline
         */
        @Override
        protected void initChannel(final Channel channel) {
            channel.pipeline()
                    .addLast(FlushTickHandler.NAME, new FlushTickHandler())
                    .addLast(RawPacketCodec.NAME, RawPacketCodec.INSTANCE)
                    .addLast(ReliableFrameHandling.INSTANCE)
                    .addLast(PacketHandling.INSTANCE)
                    .addLast(ConnectionInitializer.NAME,
                            new ChannelInboundHandlerAdapter()); //will be replaced
        }
    }

}
