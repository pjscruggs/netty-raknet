package network.ycc.raknet;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import network.ycc.raknet.client.channel.RakNetClientChannel;
import network.ycc.raknet.channel.DatagramChannelProxy;
import network.ycc.raknet.server.channel.RakNetServerChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Protects subclass initialization and combined connection completion using owned loopback transport resources. */
@Timeout(15)
class ChannelConstructionTest {
    /**
     * Constructor-owned pipeline setup must not call accessors overridden by an uninitialized subclass.
     *
     * @throws InterruptedException if registration or bounded cleanup is interrupted
     */
    @Test
    void constructorDoesNotInvokeSubclassAccessors() throws InterruptedException {
        final NioEventLoopGroup group = new NioEventLoopGroup(1);
        final GuardedClient client = new GuardedClient();
        final GuardedServer server = new GuardedServer();
        try {
            assertEquals(0, client.installations.get());
            assertEquals(0, server.installations.get());
            group.register(client).sync();
            group.register(server).sync();
            assertSame(client.pipeline(), client.pipeline());
            assertTrue(client.installations.get() > 0);
            assertSame(server.pipeline(), server.pipeline());
            assertTrue(server.installations.get() > 0);
        } finally {
            close(client);
            close(server);
            assertTrue(group.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS));
        }
    }

    /**
     * A failed UDP connect must fail the public connect future without waiting for a handshake that cannot start.
     *
     * @throws InterruptedException if registration, connection completion or cleanup is interrupted
     */
    @Test
    void socketConnectFailureCompletesHandshakeAndClosesSocket() throws InterruptedException {
        final NioEventLoopGroup group = new NioEventLoopGroup(1);
        final RakNetClientChannel client = new RakNetClientChannel();
        final IllegalStateException failure = new IllegalStateException("Injected socket failure");
        try {
            client.parent().pipeline().addLast(new ChannelOutboundHandlerAdapter() {
                /**
                 * Rejects the socket operation before any UDP packets leave the fixture.
                 *
                 * @param context owned UDP socket context
                 * @param remote requested destination that will not be contacted
                 * @param local optional bind address left unused
                 * @param promise completed with the injected failure
                 */
                @Override
                public void connect(final ChannelHandlerContext context, final SocketAddress remote,
                        final SocketAddress local, final ChannelPromise promise) {
                    promise.setFailure(failure);
                }
            });
            group.register(client).sync();
            final ChannelFuture connected = client.connect(new InetSocketAddress("127.0.0.1", 9));
            assertTrue(connected.await(2, TimeUnit.SECONDS));
            assertFalse(connected.isSuccess());
            assertSame(failure, connected.cause());
            assertSame(failure, client.connectFuture().cause());
            assertTrue(client.parent().closeFuture().await(2, TimeUnit.SECONDS));
        } finally {
            close(client);
            assertTrue(group.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS));
        }
    }

    /**
     * Runs registered pipeline cleanup and closes a raw socket if registration never succeeded.
     *
     * @param channel fixture-owned transport whose lifetime has ended
     * @throws InterruptedException if the bounded normal close is interrupted
     */
    private static void close(final DatagramChannelProxy channel) throws InterruptedException {
        if (!channel.isOpen()) {
            return;
        }
        if (channel.isRegistered()) {
            assertTrue(channel.close().await(5, TimeUnit.SECONDS));
        } else {
            channel.parent().unsafe().closeForcibly();
        }
    }

    /** Detects premature client accessor dispatch using a field initialized after superclass construction. */
    private static final class GuardedClient extends RakNetClientChannel {
        /** Counts accessor calls that must occur only after subclass fields have been initialized. */
        private final AtomicInteger installations = new AtomicInteger();

        /**
         * Reads subclass state before returning the normal protocol pipeline.
         *
         * @return initialized client pipeline
         */
        @Override
        public ChannelPipeline pipeline() {
            installations.incrementAndGet();
            return super.pipeline();
        }
    }

    /** Detects premature server accessor dispatch using a field initialized after superclass construction. */
    private static final class GuardedServer extends RakNetServerChannel {
        /** Counts accessor calls that must occur only after subclass fields have been initialized. */
        private final AtomicInteger installations = new AtomicInteger();

        /**
         * Reads subclass state before returning the normal listener pipeline.
         *
         * @return initialized server pipeline
         */
        @Override
        public ChannelPipeline pipeline() {
            installations.incrementAndGet();
            return super.pipeline();
        }
    }
}
