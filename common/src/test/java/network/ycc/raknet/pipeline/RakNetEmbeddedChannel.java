package network.ycc.raknet.pipeline;

import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import network.ycc.raknet.config.DefaultConfig;

/** Gives isolated pipeline tests real per-connection transport settings without sockets or timers. */
final class RakNetEmbeddedChannel extends EmbeddedChannel {
    /** Lazily initialized because the superclass can request configuration during construction. */
    private DefaultConfig transportConfig;

    /**
     * Installs the tested production stages on an embedded event loop.
     *
     * @param handlers ordered pipeline stages owned by this fixture channel
     */
    RakNetEmbeddedChannel(final ChannelHandler... handlers) {
        super(handlers);
    }

    /**
     * Returns persistent RakNet settings that tests can configure before sending frames.
     *
     * @return this channel's transport configuration
     */
    @Override
    public DefaultConfig config() {
        if (transportConfig == null) {
            transportConfig = new DefaultConfig(this);
        }
        return transportConfig;
    }
}
