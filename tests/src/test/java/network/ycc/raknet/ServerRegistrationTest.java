package network.ycc.raknet;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.socket.nio.NioDatagramChannel;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import network.ycc.raknet.server.channel.RakNetChildChannel;
import network.ycc.raknet.server.channel.RakNetServerChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies failed application-loop registration cannot leave a negotiated peer transport open. */
@Timeout(20)
class ServerRegistrationTest {
    /**
     * A terminated application loop rejects registration and the parent connection must close promptly.
     *
     * @throws InterruptedException if server setup or a bounded cleanup wait is interrupted
     */
    @Test
    void rejectedApplicationRegistrationClosesTransport() throws InterruptedException {
        final EndToEndTest fixture = new EndToEndTest();
        final DefaultEventLoop unavailable = new DefaultEventLoop();
        final CountDownLatch peerClosed = new CountDownLatch(1);
        try {
            unavailable.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
            final RakNetServerChannel server = (RakNetServerChannel) new ServerBootstrap()
                    .group(fixture.ioGroup, fixture.childGroup)
                    .channelFactory(() -> new ObservedServer(peerClosed))
                    .childHandler(new ChannelInboundHandlerAdapter())
                    .bind(fixture.localhost).sync().channel();
            server.setProvidedApplicationEventLoop(unavailable);
            final Channel client = fixture.newClient(null);
            assertTrue(peerClosed.await(5, TimeUnit.SECONDS));
            client.close().sync();
            server.close().sync();
        } finally {
            unavailable.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            fixture.stopEventLoops();
        }
    }

    /** Records peer transport closure independently of the application's registration result. */
    private static final class ObservedServer extends RakNetServerChannel {
        /** Signals when the accepted peer has finished closing its transport. */
        private final CountDownLatch peerClosed;

        /**
         * Creates a real UDP listener with a completion signal for its accepted peer.
         *
         * @param peerClosed latch released by the peer's close future
         */
        private ObservedServer(final CountDownLatch peerClosed) {
            super(NioDatagramChannel::new);
            this.peerClosed = peerClosed;
        }

        /**
         * Uses normal peer creation and observes its transport lifetime without changing registration.
         *
         * @param remoteAddress connecting client's endpoint
         * @param localAddress server's bound endpoint
         * @param registerChannel normal application-registration callback under test
         * @return ordinary child with a close-completion observer installed
         */
        @Override
        protected RakNetChildChannel newChild(final InetSocketAddress remoteAddress,
                final InetSocketAddress localAddress, final Consumer<Channel> registerChannel) {
            final RakNetChildChannel child = super.newChild(remoteAddress, localAddress, registerChannel);
            child.closeFuture().addListener(closed -> peerClosed.countDown());
            return child;
        }
    }
}
