package network.ycc.raknet;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.bootstrap.ServerBootstrap;
import java.net.SocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import network.ycc.raknet.server.channel.RakNetServerChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies listener shutdown waits for peer transport cleanup rather than only application closure. */
@Timeout(20)
class ServerShutdownTest {
    /**
     * A failed peer socket connection must close that socket without publishing application activity.
     *
     * @throws InterruptedException if setup or the bounded socket-cleanup wait is interrupted
     */
    @Test
    void failedPeerSocketSetupDoesNotActivateApplication() throws InterruptedException {
        final EndToEndTest fixture = new EndToEndTest();
        final CountDownLatch attempted = new CountDownLatch(1);
        final AtomicReference<Channel> failedSocket = new AtomicReference<>();
        final AtomicInteger activations = new AtomicInteger();
        try {
            final Channel server = new ServerBootstrap().group(fixture.ioGroup, fixture.childGroup)
                    .channelFactory(() -> new RakNetServerChannel(() -> {
                        final NioDatagramChannel socket = new NioDatagramChannel();
                        socket.pipeline().addLast(new ChannelOutboundHandlerAdapter() {
                            /**
                             * Fails the dedicated peer connect while leaving the shared listener bind available.
                             *
                             * @param ctx socket whose failed setup is observed by the test
                             * @param remoteAddress peer endpoint that will not be connected
                             * @param localAddress requested local bind endpoint
                             * @param promise failed with the injected socket error
                             */
                            @Override
                            public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress,
                                    final SocketAddress localAddress, final ChannelPromise promise) {
                                failedSocket.set(ctx.channel());
                                promise.setFailure(new IllegalStateException("Injected UDP connect failure"));
                                attempted.countDown();
                            }
                        });
                        return socket;
                    }))
                    .childHandler(new ChannelInboundHandlerAdapter() {
                        /**
                         * Records an application activation that would be incorrect after failed socket setup.
                         *
                         * @param ctx application context whose active event is forwarded unchanged
                         */
                        @Override
                        public void channelActive(final ChannelHandlerContext ctx) {
                            activations.incrementAndGet();
                            ctx.fireChannelActive();
                        }
                    }).bind(fixture.localhost).sync().channel();
            final Channel client = fixture.newClient(null);
            assertTrue(attempted.await(5, TimeUnit.SECONDS));
            assertTrue(failedSocket.get().closeFuture().await(5, TimeUnit.SECONDS));
            org.junit.jupiter.api.Assertions.assertEquals(0, activations.get());
            client.close().sync();
            server.close().sync();
        } finally {
            fixture.stopEventLoops();
        }
    }

    /**
     * Holds a peer's transport close and verifies that the server cannot report completion ahead of it.
     *
     * @throws InterruptedException if fixture setup or a bounded shutdown wait is interrupted
     * @throws java.util.concurrent.ExecutionException if the listener cannot locate its accepted peer
     */
    @Test
    void listenerWaitsForPeerTransportClose() throws InterruptedException,
            java.util.concurrent.ExecutionException {
        final EndToEndTest fixture = new EndToEndTest();
        ChannelPromise release = null;
        try {
            final Channel server = fixture.newServer(null, null);
            final Channel client = fixture.newClient(null);
            final Channel peer = server.eventLoop().submit(() ->
                    ((RakNetServerChannel) server).getChildChannel(client.localAddress())).get();
            final CountDownLatch requested = new CountDownLatch(1);
            release = peer.newPromise();
            final ChannelPromise barrier = release;
            peer.eventLoop().submit(() -> peer.pipeline().addLast(new ChannelOutboundHandlerAdapter() {
                /**
                 * Defers the transport close until the test releases its barrier, retaining no message buffers.
                 *
                 * @param ctx peer pipeline context used to resume the close operation
                 * @param promise original caller's close completion
                 */
                @Override
                public void close(final ChannelHandlerContext ctx, final ChannelPromise promise) {
                    requested.countDown();
                    barrier.addListener(ignored -> ctx.close(promise));
                }
            })).sync();
            final ChannelFuture closing = server.close();
            assertTrue(requested.await(5, TimeUnit.SECONDS));
            assertFalse(closing.await(500, TimeUnit.MILLISECONDS));
            release.trySuccess();
            assertTrue(closing.await(5, TimeUnit.SECONDS));
            assertTrue(closing.isSuccess());
            assertTrue(peer.closeFuture().isDone());
            client.close().sync();
        } finally {
            if (release != null) {
                release.trySuccess();
            }
            fixture.stopEventLoops();
        }
    }
}
