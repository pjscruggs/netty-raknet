package network.ycc.raknet.client.pipeline;

import io.netty.channel.ChannelException;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultConfig;
import network.ycc.raknet.packet.ConnectionFailed;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks that a remote rejection resolves connection ownership without waiting for another write. */
class ConnectionRejectionTest {
    /** A refusal in any handshake phase fails the connect promise and closes without an error echo. */
    @Test
    void remoteRejectionCompletesEveryHandshakePhase() {
        for (int phase = 0; phase < 3; phase++) {
            TestChannel channel = new TestChannel();
            ChannelPromise connected = channel.newPromise();
            TestInitializer initializer = new TestInitializer(connected, phase);
            try {
                assertDoesNotThrow(() -> initializer.channelRead0(channel.pipeline().firstContext(),
                        new ConnectionFailed(DefaultConfig.DEFAULT_MAGIC)));
                assertTrue(connected.isDone());
                assertFalse(connected.isSuccess());
                assertTrue(connected.cause() instanceof ChannelException);
                assertFalse(channel.isOpen());
                assertNull(channel.readOutbound());
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }

    /** Provides the real RakNet configuration contract without opening a UDP socket. */
    private static final class TestChannel extends EmbeddedChannel {
        /** Lazily initialized because EmbeddedChannel may ask for configuration in its constructor. */
        private DefaultConfig raknetConfig;

        /** Installs one context used to exercise the initializer's real close and failure path. */
        private TestChannel() {
            super(new ChannelInboundHandlerAdapter());
        }

        /**
         * Returns connection-local transport settings for the initializer under test.
         *
         * @return stable RakNet configuration for this embedded connection
         */
        @Override
        public RakNet.Config config() {
            if (raknetConfig == null) {
                raknetConfig = new DefaultConfig(this);
            }
            return raknetConfig;
        }
    }

    /** Selects a handshake phase without starting timers or synthesizing a network exchange. */
    private static final class TestInitializer extends ConnectionInitializer {
        /**
         * Selects the exact state at which the peer's refusal arrives.
         *
         * @param connected promise whose failure must be observable immediately
         * @param phase zero-based handshake phase
         */
        private TestInitializer(final ChannelPromise connected, final int phase) {
            super(connected);
            state = State.values()[phase];
        }
    }
}
