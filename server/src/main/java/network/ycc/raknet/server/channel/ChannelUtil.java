package network.ycc.raknet.server.channel;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.util.AttributeKey;

import java.util.Map;

/**
 * Applies ServerBootstrap's captured child options and attributes to separately registered channels.
 */
public class ChannelUtil {

    /**
     * Copies captured bootstrap options and attributes before an application channel is registered.
     * Option failures are reported individually so later options and attributes can still be applied.
     *
     * @param channel new application channel receiving bootstrap configuration
     * @param parameters snapshot of the server acceptor's child settings
     */
    static void applyChannelParameters(final Channel channel, final RakNetServerChannel.ChannelParameters parameters) {
        setChannelOptions(channel, parameters.childOptions);
        for (Map.Entry<AttributeKey<?>, Object> e: parameters.childAttrs) {
            channel.attr((AttributeKey<Object>) e.getKey()).set(e.getValue());
        }
    }

    /**
     * Applies each captured option independently using the channel's configuration implementation.
     * Unsupported options and rejected values are logged by the per-option helper.
     *
     * @param channel channel receiving the options
     * @param options option/value pairs captured from ServerBootstrap
     */
    static void setChannelOptions(
            final Channel channel, final Map.Entry<ChannelOption<?>, Object>[] options) {
        for (Map.Entry<ChannelOption<?>, Object> e: options) {
            setChannelOption(channel, e.getKey(), e.getValue());
        }
    }

    /**
     * Attempts one bootstrap option and reports unsupported or failing assignments without aborting setup.
     * The unchecked cast bridges a heterogeneous option table whose values must match their declared keys.
     *
     * @param channel target whose configuration interprets the option
     * @param option typed option key captured by the bootstrap
     * @param value value originally associated with that key
     */
    @SuppressWarnings("unchecked")
    private static void setChannelOption(
            final Channel channel, final ChannelOption<?> option, final Object value) {
        try {
            if (!channel.config().setOption((ChannelOption<Object>) option, value)) {
                System.out.println(String.format("Unknown channel option '%s' for channel '%s'", option, channel));
            }
        } catch (Throwable t) {
            System.err.println(String.format("Failed to set channel option '%s' with value '%s' for channel '%s'", option, value, channel));
            t.printStackTrace();
        }
    }

}
