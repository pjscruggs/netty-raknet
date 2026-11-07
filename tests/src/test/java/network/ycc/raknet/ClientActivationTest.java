package network.ycc.raknet;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import network.ycc.raknet.client.channel.RakNetClientThreadedChannel;
import network.ycc.raknet.utils.EmptyInit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Controls application activation independently of negotiation to expose premature connect completion. */
@Timeout(20)
class ClientActivationTest {
    /**
     * A successful parent handshake cannot complete the client's connect promise before application activity.
     * @throws InterruptedException if a bounded handshake or shutdown wait is interrupted
     */
    @Test
    void connectionWaitsForApplicationActivity() throws InterruptedException {
        verifyActivation(false);
    }

    /**
     * Closing after negotiation but before activity fails the pending connect instead of leaving it unresolved.
     * @throws InterruptedException if a bounded handshake or shutdown wait is interrupted
     */
    @Test
    void closingBeforeActivationFailsConnection() throws InterruptedException {
        verifyActivation(true);
    }

    /**
     * Holds the real parent's active event while observing the application connection result.
     * @param closeBeforeActivation whether to close instead of delivering the held active event
     * @throws InterruptedException if a bounded handshake or shutdown wait is interrupted
     */
    private static void verifyActivation(final boolean closeBeforeActivation) throws InterruptedException {
        final EndToEndTest fixture = new EndToEndTest();
        final ActivationGate gate = new ActivationGate();
        try {
            final Channel server = fixture.newServer(null, null);
            final ChannelFuture connection = new Bootstrap().group(fixture.ioGroup)
                    .channelFactory(() -> createClient(gate)).handler(new EmptyInit())
                    .connect(fixture.localhost);
            assertTrue(gate.arrived.await(5, TimeUnit.SECONDS));
            assertFalse(connection.isDone(), "Negotiation alone must not publish application success");
            final ChannelHandlerContext context = Objects.requireNonNull(gate.context);
            if (closeBeforeActivation) {
                context.close().sync();
                assertTrue(connection.await(5, TimeUnit.SECONDS));
                assertFalse(connection.isSuccess());
            } else {
                context.executor().submit(context::fireChannelActive).sync();
                assertTrue(connection.await(5, TimeUnit.SECONDS));
                assertTrue(connection.isSuccess());
                assertTrue(connection.channel().isActive());
            }
            connection.channel().close().sync();
            server.close().sync();
        } finally {
            fixture.stopEventLoops();
        }
    }

    /**
     * Installs a gate immediately before the real parent-to-application bridge after parent registration.
     * @param gate test-owned handler that delays only the active event
     * @return unregistered client with ordinary handshake and transport handlers intact
     */
    private static RakNetClientThreadedChannel createClient(final ActivationGate gate) {
        final RakNetClientThreadedChannel client = new RakNetClientThreadedChannel();
        client.parent().pipeline().addLast(new ChannelInitializer<Channel>() {
            /**
             * Adds the gate once the parent initializer has installed the application bridge.
             * @param parent registered client transport whose active event is being controlled
             */
            @Override
            protected void initChannel(final Channel parent) {
                parent.pipeline().addBefore(RakNetClientThreadedChannel.NAME_CLIENT_PARENT_THREADED_READ_HANDLER,
                        "activation-gate", gate);
            }
        });
        return client;
    }

    /** Holds one negotiated active event without changing packets, promises, or parent activity state. */
    private static final class ActivationGate extends ChannelInboundHandlerAdapter {
        /** Signals that the negotiated parent event reached the gate. */
        private final CountDownLatch arrived = new CountDownLatch(1);
        /** Context published before the latch is released, used to deliver or abandon the held event. */
        private ChannelHandlerContext context;

        /**
         * Records the event without forwarding it until the test explicitly releases the gate.
         * @param ctx parent context immediately before the application bridge
         */
        @Override
        public void channelActive(final ChannelHandlerContext ctx) {
            context = ctx;
            arrived.countDown();
        }
    }
}
