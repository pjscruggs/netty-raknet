package network.ycc.raknet.config;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import network.ycc.raknet.packet.FrameSet;
import network.ycc.raknet.packet.Ping;
import network.ycc.raknet.packet.SimplePacket;
import network.ycc.raknet.packet.UnconnectedPing;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies construction and configuration do not expose partially initialized or externally mutable state. */
class ConfigurationOwnershipTest {
    /** Constructor defaults must not call a subclass RTT hook before its fields exist. */
    @Test
    void rttDefaultsDoNotInvokeSubclassHooks() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        try {
            final GuardedConfig config = new GuardedConfig(channel);
            assertEquals(100_000_000, config.getRTTNanos());
            assertEquals(0, config.updates.get());
            config.setRTTNanos(50);
            assertEquals(1, config.updates.get());
            assertEquals(50, config.getRTTNanos());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    /** Neither a setter's argument nor a getter's result may silently change protocol admission policy. */
    @Test
    void acceptedVersionsAreOwnedSnapshots() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        try {
            final DefaultConfig config = new DefaultConfig(channel);
            final int[] accepted = {9};
            config.setprotocolVersions(accepted);
            accepted[0] = 17;
            config.getProtocolVersions()[0] = 42;
            assertTrue(config.containsProtocolVersion(9));
            assertFalse(config.containsProtocolVersion(17));
            assertFalse(config.containsProtocolVersion(42));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    /** Built-ins bypass premature subclass dispatch while explicit later registrations still invoke the hook. */
    @Test
    void registryConstructionPreservesPostConstructionCustomization() {
        final GuardedCodec codec = new GuardedCodec();
        assertEquals(DefaultCodec.PING, codec.packetIdFor(Ping.class));
        assertEquals(0, codec.registrations.get());
        codec.replace(DefaultCodec.PING);
        assertEquals(1, codec.registrations.get());
        assertEquals(-1, codec.packetIdFor(Ping.class));
        assertEquals(DefaultCodec.PING, codec.packetIdFor(UnconnectedPing.class));
        assertFalse(codec.framedPacketIds.contains(DefaultCodec.PING));
    }

    /** Replacing one frame-set decode alias must preserve its independent canonical encoding ID. */
    @Test
    void replacingDecodeAliasPreservesCanonicalEncoder() {
        final GuardedCodec codec = new GuardedCodec();
        codec.replace(DefaultCodec.FRAME_DATA_START);
        assertEquals(DefaultCodec.FRAME_DATA_END, codec.packetIdFor(FrameSet.class));
    }

    /** Uses initialized subclass state in an ordinary override to detect illegal constructor dispatch. */
    private static final class GuardedConfig extends DefaultConfig {
        /** Counts explicit RTT changes after subclass initialization. */
        private final AtomicInteger updates = new AtomicInteger();

        /**
         * Creates a configuration whose override is usable only after the superclass constructor finishes.
         *
         * @param channel fixture channel owning the configuration
         */
        private GuardedConfig(final Channel channel) {
            super(channel);
        }

        /**
         * Records a post-construction RTT change before applying the real statistics update.
         *
         * @param rtt replacement RTT sample in nanoseconds
         */
        @Override
        public void setRTTNanos(final long rtt) {
            updates.incrementAndGet();
            super.setRTTNanos(rtt);
        }
    }

    /** Records subclass registry hooks, which must run only after its counter has been initialized. */
    private static final class GuardedCodec extends DefaultCodec {
        /** Counts explicit customization calls after construction. */
        private final AtomicInteger registrations = new AtomicInteger();

        /**
         * Replaces one wire identifier with an offline packet to exercise framed-state and alias cleanup.
         *
         * @param id identifier whose previous decoder is replaced
         */
        private void replace(final int id) {
            register(id, UnconnectedPing.class, UnconnectedPing::new);
        }

        /**
         * Uses subclass state before delegating a deliberately explicit registration to the base registry.
         *
         * @param <T> packet type produced by the factory
         * @param id wire identifier to register
         * @param type exact runtime class used for encoding
         * @param constructor factory invoked when decoding a packet body
         */
        @Override
        protected <T extends SimplePacket> void register(final int id, final Class<T> type,
                final Supplier<T> constructor) {
            registrations.incrementAndGet();
            super.register(id, type, constructor);
        }
    }
}
