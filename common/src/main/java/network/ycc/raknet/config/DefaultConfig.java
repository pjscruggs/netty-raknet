package network.ycc.raknet.config;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultChannelConfig;
import network.ycc.raknet.RakNet;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Stores per-channel negotiation settings, queue limits and recent round-trip samples.
 * Volatile option fields provide visibility but do not make multi-option updates atomic. Callers must
 * serialize access to the RTT statistics and preserve valid transport settings when using raw setters.
 */
public class DefaultConfig extends DefaultChannelConfig implements RakNet.Config {

    /** Standard sixteen-byte marker used to recognize RakNet offline packets. */
    public static final RakNet.Magic DEFAULT_MAGIC = new DefaultMagic(new byte[]{
            (byte) 0x00, (byte) 0xff, (byte) 0xff, (byte) 0x00, (byte) 0xfe, (byte) 0xfe,
            (byte) 0xfe, (byte) 0xfe, (byte) 0xfd, (byte) 0xfd, (byte) 0xfd, (byte) 0xfd,
            (byte) 0x12, (byte) 0x34, (byte) 0x56, (byte) 0x78});

    /** Initial maximum datagram size in bytes before peer negotiation. */
    public static final int DEFAULT_MTU = 1500;

    /** No-op metrics sink used when the application has not installed an observer. */
    private static final RakNet.MetricsLogger DEFAULT_METRICS = new RakNet.MetricsLogger() {};
    /** Non-cryptographic source for default server and client identifiers, which are not credentials. */
    private static final Random rnd = new Random();

    /** Rolling window of sixteen RTT samples in nanoseconds, accessed by the owning connection. */
    protected final DescriptiveStatistics rttStats = new DescriptiveStatistics(16);
    //TODO: add rest of ChannelOptions
    /** Server identity advertised during discovery and connection establishment. */
    private volatile long serverId = rnd.nextLong();
    /** Client identity announced or bound during connection establishment. */
    private volatile long clientId = rnd.nextLong();
    /** Observer receiving transport counters and timing measurements. */
    private volatile RakNet.MetricsLogger metrics = DEFAULT_METRICS;
    /** Negotiated maximum datagram size in bytes, initially the default MTU. */
    private volatile int mtu = DEFAULT_MTU;
    /** Extra resend grace period added to mean RTT and twice its standard deviation. */
    private volatile long retryDelayNanos = TimeUnit.NANOSECONDS.convert(15, TimeUnit.MILLISECONDS);
    /** Ceiling for the dynamic burst allowance added to the baseline pending-frame limit. */
    private volatile int maxPendingFrameSets = 1024;
    /** Baseline number of datagrams permitted to remain unacknowledged before burst allowance. */
    private volatile int defaultPendingFrameSets = 32;
    /** Byte limit used by transport queue and fragment-size checks. */
    private volatile int maxQueuedBytes = 3 * 1024 * 1024;
    /** Offline packet marker shared with the connection handshake. */
    private volatile RakNet.Magic magic = DEFAULT_MAGIC;
    /** Packet registry used for encoding and decoding this connection's transport messages. */
    private volatile RakNet.Codec codec = DefaultCodec.INSTANCE;
    /** Live array of transport versions accepted by the server, initially nine and ten. */
    private volatile int[] protocolVersions = new int[]{9, 10};
    /** Maximum number of child connections admitted by a server channel. */
    private volatile int maxConnections = 2048;
    /** Transport version selected for this connection, initially nine. */
    private volatile int protocolVersion = 9;
    /** Whether loss feedback is replaced with a positive gauge when adjusting burst allowance. */
    private volatile boolean ignoreResendGauge = false;
    /** Whether missing datagram sequences produce negative acknowledgements and incoming NACKs are honored. */
    private volatile boolean NACKEnabled = false;
    /** Whether flush updates immediately select the maximum configured burst allowance. */
    private volatile boolean noDelay = false;

    /**
     * Initializes Netty options and seeds the RTT window with a 100-millisecond sample.
     *
     * @param channel channel owning these transport options and RTT measurements
     */
    public DefaultConfig(final Channel channel) {
        super(channel);
        setRTTNanos(TimeUnit.NANOSECONDS.convert(100, TimeUnit.MILLISECONDS));
    }

    /**
     * Adds the advertised RakNet identity, MTU, timing, protocol and marker options to Netty's option map.
     *
     * @return new option map assembled from the channel defaults and advertised transport values
     */
    @Override
    @SuppressWarnings("deprecation")
    public Map<ChannelOption<?>, Object> getOptions() {
        return getOptions(
                super.getOptions(),
                RakNet.SERVER_ID, RakNet.CLIENT_ID, RakNet.METRICS, RakNet.MTU,
                RakNet.RTT, RakNet.PROTOCOL_VERSION, RakNet.MAGIC, RakNet.RETRY_DELAY_NANOS);
    }

    /**
     * Resolves recognized RakNet options directly and delegates other keys to Netty's configuration.
     *
     * @param <T> value type carried by the option key
     * @param option typed option to query
     * @return current option value, or the result of the inherited Netty lookup
     */
    @Override
    @SuppressWarnings({"unchecked", "deprecation"})
    public <T> T getOption(final ChannelOption<T> option) {
        if (option == RakNet.SERVER_ID) {
            return (T) (Long) serverId;
        } else if (option == RakNet.CLIENT_ID) {
            return (T) (Long) clientId;
        } else if (option == RakNet.METRICS) {
            return (T) metrics;
        } else if (option == RakNet.MTU) {
            return (T) (Integer) mtu;
        } else if (option == RakNet.RTT) {
            return (T) (Long) getRTTNanos();
        } else if (option == RakNet.PROTOCOL_VERSION) {
            return (T) (Integer) protocolVersion;
        } else if (option == RakNet.MAGIC) {
            return (T) magic;
        } else if (option == RakNet.RETRY_DELAY_NANOS) {
            return (T) (Long) retryDelayNanos;
        } else if (option == RakNet.MAX_CONNECTIONS) {
            return (T) (Integer) maxConnections;
        }
        return super.getOption(option);
    }

    /**
     * Stores recognized RakNet options and delegates other keys to Netty.
     * The RTT option resets the sampling window rather than adding an observation.
     *
     * @param <T> value type carried by the option key
     * @param option typed option to replace
     * @param value new value whose type and transport constraints must match the option
     * @return true for handled transport options, otherwise the inherited setter result
     */
    @Override
    @SuppressWarnings("deprecation")
    public <T> boolean setOption(final ChannelOption<T> option, final T value) {
        if (option == RakNet.SERVER_ID) {
            serverId = (Long) value;
        } else if (option == RakNet.CLIENT_ID) {
            clientId = (Long) value;
        } else if (option == RakNet.METRICS) {
            metrics = (RakNet.MetricsLogger) value;
        } else if (option == RakNet.MTU) {
            mtu = (Integer) value;
        } else if (option == RakNet.RTT) {
            setRTTNanos((Long) value);
        } else if (option == RakNet.PROTOCOL_VERSION) {
            protocolVersion = (Integer) value;
        } else if (option == RakNet.MAGIC) {
            magic = (RakNet.Magic) value;
        } else if (option == RakNet.RETRY_DELAY_NANOS) {
            retryDelayNanos = (Long) value;
        } else if (option == RakNet.MAX_CONNECTIONS) {
            maxConnections = (Integer) value;
        } else {
            return super.setOption(option, value);
        }
        return true;
    }

    /**
     * Checks exact membership in the current accepted-version array without interpreting release names.
     *
     * @param protocolVersion RakNet transport version proposed by a connecting peer
     * @return true if an accepted-version entry equals the proposed value
     */
    @Override
    public boolean containsProtocolVersion(final int protocolVersion) {
        for (int version : this.protocolVersions) {
            if (version == protocolVersion) {
                return true;
            }
        }

        return false;
    }
    /**
     * Returns the transport observer that receives counters and timing measurements.
     *
     * @return transport observer that receives counters and timing measurements
     */
    @Override
    public RakNet.MetricsLogger getMetrics() {
        return metrics;
    }

    /**
     * Installs the observer used by subsequent transport measurements.
     *
     * @param metrics non-null observer, or the existing no-op observer to disable collection
     */
    @Override
    public void setMetrics(final RakNet.MetricsLogger metrics) {
        this.metrics = metrics;
    }

    /**
     * Returns the server identifier announced to peers.
     *
     * @return server identifier announced to peers
     */
    @Override
    public long getServerId() {
        return serverId;
    }

    /**
     * Selects the server identifier used in future handshake and discovery replies.
     *
     * @param serverId opaque server identifier, not an authentication secret
     */
    @Override
    public void setServerId(final long serverId) {
        this.serverId = serverId;
    }

    /**
     * Returns the client identifier bound to this connection.
     *
     * @return client identifier bound to this connection
     */
    @Override
    public long getClientId() {
        return clientId;
    }

    /**
     * Stores the client identifier announced or accepted during the handshake.
     *
     * @param clientId opaque identifier that must remain consistent across handshake phases
     */
    @Override
    public void setClientId(final long clientId) {
        this.clientId = clientId;
    }

    /**
     * Returns the negotiated maximum datagram size in bytes.
     *
     * @return negotiated maximum datagram size in bytes
     */
    @Override
    public int getMTU() {
        return mtu;
    }

    /**
     * Sets the datagram size used for fragmentation and packet packing.
     * This setter stores the supplied value without checking whether frame headers fit.
     *
     * @param mtu usable maximum datagram size in bytes
     */
    @Override
    public void setMTU(final int mtu) {
        this.mtu = mtu;
    }

    /**
     * Returns the extra retransmission grace period in nanoseconds.
     *
     * @return extra retransmission grace period in nanoseconds
     */
    @Override
    public long getRetryDelayNanos() {
        return retryDelayNanos;
    }

    /**
     * Sets the grace period added to mean RTT and twice its standard deviation before a resend.
     *
     * @param retryDelayNanos additional nonnegative delay in nanoseconds
     */
    @Override
    public void setRetryDelayNanos(final long retryDelayNanos) {
        this.retryDelayNanos = retryDelayNanos;
    }

    /**
     * Returns the mean of the retained RTT samples, clamped to at least one nanosecond.
     *
     * @return positive mean RTT in nanoseconds
     */
    @Override
    public long getRTTNanos() {
        final long rtt = (long) rttStats.getMean();
        return Math.max(rtt, 1);
    }

    /**
     * Clears previous RTT history and seeds the rolling window with one supplied measurement.
     * This does not emit metrics callbacks.
     *
     * @param rtt replacement RTT estimate in nanoseconds
     */
    @Override
    public void setRTTNanos(final long rtt) {
        rttStats.clear();
        rttStats.addValue(rtt);
    }

    /**
     * Returns the sample standard deviation of the current RTT window, converted to whole nanoseconds.
     *
     * @return RTT standard deviation in nanoseconds
     */
    @Override
    public long getRTTStdDevNanos() {
        return (long) rttStats.getStandardDeviation(); //ns
    }

    /**
     * Appends a sample to the sixteen-value window and reports its resulting mean and standard deviation.
     * Call this on the connection's owning event loop to serialize access to the statistics.
     *
     * @param rttSample new local round-trip measurement in nanoseconds
     */
    @Override
    public void updateRTTNanos(final long rttSample) {
        rttStats.addValue(rttSample);
        metrics.measureRTTns(getRTTNanos());
        metrics.measureRTTnsStdDev(getRTTStdDevNanos());
    }

    /**
     * Returns the ceiling for dynamic burst tokens above the baseline pending limit.
     *
     * @return ceiling for dynamic burst tokens above the baseline pending limit
     */
    @Override
    public int getMaxPendingFrameSets() {
        return maxPendingFrameSets;
    }

    /**
     * Sets the largest dynamic burst allowance that can augment the baseline pending-frame limit.
     *
     * @param maxPendingFrameSets nonnegative ceiling for additional pending datagrams
     */
    @Override
    public void setMaxPendingFrameSets(final int maxPendingFrameSets) {
        this.maxPendingFrameSets = maxPendingFrameSets;
    }

    /**
     * Returns the baseline outstanding-datagram allowance before burst tokens.
     *
     * @return baseline outstanding-datagram allowance before burst tokens
     */
    @Override
    public int getDefaultPendingFrameSets() {
        return defaultPendingFrameSets;
    }

    /**
     * Sets the baseline send window and the batch threshold used for acknowledgement replies.
     *
     * @param defaultPendingFrameSets positive baseline number of outstanding frame sets
     */
    @Override
    public void setDefaultPendingFrameSets(final int defaultPendingFrameSets) {
        this.defaultPendingFrameSets = defaultPendingFrameSets;
    }

    /**
     * Returns the byte threshold consulted by transport queue and fragment-size checks.
     *
     * @return byte threshold consulted by transport queue and fragment-size checks
     */
    @Override
    public int getMaxQueuedBytes() {
        return maxQueuedBytes;
    }

    /**
     * Sets the transport byte threshold used to reject excessive queued work.
     * This stores a threshold rather than allocating or reserving that amount of memory.
     *
     * @param maxQueuedBytes positive limit in bytes
     */
    @Override
    public void setMaxQueuedBytes(final int maxQueuedBytes) {
        this.maxQueuedBytes = maxQueuedBytes;
    }

    /**
     * Returns the shared offline-message marker used during negotiation.
     *
     * @return shared offline-message marker used during negotiation
     */
    @Override
    public RakNet.Magic getMagic() {
        return magic;
    }

    /**
     * Selects the offline marker used in subsequent negotiation packets and comparisons.
     *
     * @param magic non-null marker shared by reference rather than copied
     */
    @Override
    public void setMagic(final RakNet.Magic magic) {
        this.magic = magic;
    }

    /**
     * Returns the packet codec used by this connection.
     *
     * @return packet codec used by this connection
     */
    @Override
    public RakNet.Codec getCodec() {
        return codec;
    }

    /**
     * Selects the registry used to translate transport packets and framed payloads.
     *
     * @param codec non-null codec whose registry is ready before concurrent channel use
     */
    @Override
    public void setCodec(final RakNet.Codec codec) {
        this.codec = codec;
    }

    /**
     * Returns the live accepted-version array without copying it.
     * External mutation changes subsequent membership checks and must not race with channel use.
     *
     * @return shared array of accepted RakNet transport versions
     */
    @Override
    public int[] getProtocolVersions() {
        return protocolVersions;
    }

    /**
     * Replaces the accepted-version array by reference without copying or validating its entries.
     *
     * @param protocolVersions non-null version array that callers must not mutate concurrently
     */
    @Override
    public void setprotocolVersions(final int[] protocolVersions) {
        this.protocolVersions = protocolVersions;
    }

    /**
     * Returns the server admission limit for child connections.
     *
     * @return server admission limit for child connections
     */
    @Override
    public int getMaxConnections() {
        return maxConnections;
    }

    /**
     * Sets the child-connection count at which a server refuses new connection attempts.
     *
     * @param maxConnections nonnegative maximum number of simultaneous child connections
     */
    @Override
    public void setMaxConnections(final int maxConnections) {
        this.maxConnections = maxConnections;
    }

    /**
     * Returns the RakNet transport version currently selected for this connection.
     *
     * @return RakNet transport version currently selected for this connection
     */
    @Override
    public int getProtocolVersion() {
        return protocolVersion;
    }

    /**
     * Records the transport version offered by the client or accepted during negotiation.
     *
     * @param protocolVersion selected transport version, distinct from the accepted-version array
     */
    @Override
    public void setProtocolVersion(final int protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    /**
     * Reports whether resend feedback is replaced with a positive gauge during burst adjustment.
     *
     * @return true when retransmission events must not reduce the gauge
     */
    @Override
    public boolean isIgnoreResendGauge() {
        return ignoreResendGauge;
    }

    /**
     * Controls whether reliability handling forces a positive resend gauge instead of applying loss feedback.
     *
     * @param ignoreResendGauge true to force the positive gauge during subsequent adjustments
     */
    @Override
    public void setIgnoreResendGauge(final boolean ignoreResendGauge) {
        this.ignoreResendGauge = ignoreResendGauge;
    }

    /**
     * Reports whether missing sequences are tracked for NACK replies and received NACKs are acted on.
     *
     * @return true when negative acknowledgement handling is enabled
     */
    @Override
    public boolean isNACKEnabled() {
        return NACKEnabled;
    }

    /**
     * Enables or disables missing-sequence tracking and negative acknowledgement processing.
     *
     * @param NACKEnabled true to send and honor NACKs on subsequent transport activity
     */
    @Override
    public void setNACKEnabled(final boolean NACKEnabled) {
        this.NACKEnabled = NACKEnabled;
    }

    /**
     * Reports whether each burst update uses the maximum burst allowance immediately.
     *
     * @return true for maximum burst allowance rather than gradual adjustment
     */
    @Override
    public boolean isNoDelayEnabled() {
        return noDelay;
    }

    /**
     * Selects immediate maximum RakNet burst allowance. This option does not configure TCP_NODELAY.
     *
     * @param value true to skip gradual burst-token adjustment
     */
    @Override
    public void setNoDelayEnabled(final boolean value) {
        this.noDelay = value;
    }
}
