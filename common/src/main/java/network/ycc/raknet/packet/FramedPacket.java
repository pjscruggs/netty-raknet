package network.ycc.raknet.packet;

/** Carries delivery metadata independently of the packet body so framing can assign sequence numbers. */
public interface FramedPacket extends Packet {

    /**
     * Returns the delivery guarantees requested before the packet is fragmented or queued.
     *
     * @return retransmission, ordering and sequencing policy
     */
    Reliability getReliability();

    /**
     * Selects the wire reliability mode for subsequent transport framing.
     *
     * @param reliability non-null delivery policy
     */
    void setReliability(Reliability reliability);

    /**
     * Returns the ordering lane used by ordered and sequenced modes.
     *
     * @return lane index supported by the transport
     */
    int getOrderChannel();

    /**
     * Selects which independently ordered stream receives this packet.
     * The lane has no ordering effect when the selected reliability mode is unordered.
     *
     * @param orderChannel supported ordering lane, currently zero through seven
     */
    void setOrderChannel(int orderChannel);

    /**
     * Maps the eight three-bit RakNet reliability codes to retransmission and sequencing requirements.
     * Declaration order is the wire code and must not be changed.
     */
    enum Reliability {
        //						  REL    ORD    SEQ    ACK
        /** May be lost and carries no ordering or reliable-delivery index. */
        UNRELIABLE(               false, false, false, false),
        /** May be lost and allows a newer sequence on its lane to supersede older data. */
        UNRELIABLE_SEQUENCED(     false, true,  true,  false),
        /** Requests retransmission without imposing delivery order. */
        RELIABLE(                 true,  false, false, false),
        /** Requests retransmission and in-order delivery within the chosen lane. */
        RELIABLE_ORDERED(         true,  true,  false, false),
        /** Requests retransmission while permitting newer sequenced data to supersede older data. */
        RELIABLE_SEQUENCED(       true,  true,  true,  false),
        /** Uses the unreliable wire mode with the receipt flag set. */
        UNRELIABLE_ACK(           false, false, false, true),
        /** Uses the reliable unordered wire mode with the receipt flag set. */
        RELIABLE_ACK(             true,  false, false, true),
        /** Uses the reliable ordered wire mode with the receipt flag set. */
        RELIABLE_ORDERED_ACK(     true,  true,  false, true);

        /** Whether fragments need reliable indexes and retransmission until acknowledged. */
        public final boolean isReliable;
        /** Whether the wire frame includes an ordering index and channel. */
        public final boolean isOrdered;
        /** Whether the wire frame also includes a sequence index for superseding older data. */
        public final boolean isSequenced;
        /** Whether this wire mode requests a receipt. This flag alone does not implement a receipt callback. */
        public final boolean isAckd;

        /**
         * Defines the frame-header requirements of one fixed wire reliability code.
         *
         * @param isReliable whether reliable delivery indexes and retransmission apply
         * @param isOrdered whether ordering index and channel are encoded
         * @param isSequenced whether a sequence index is also encoded
         * @param isAckd whether the wire mode requests a receipt
         */
        Reliability(final boolean isReliable, final boolean isOrdered, final boolean isSequenced, final boolean isAckd) {
            this.isReliable = isReliable;
            this.isOrdered = isOrdered;
            this.isSequenced = isSequenced;
            this.isAckd = isAckd;
        }

        /**
         * Resolves a three-bit wire value to its declared reliability mode.
         *
         * @param code wire reliability value from zero through seven
         * @return matching reliability mode
         */
        public static Reliability get(final int code) {
            assert code >= 0 && code < values().length;
            return values()[code];
        }

        /**
         * Returns the mode's declaration ordinal, which is its three-bit wire code.
         *
         * @return wire value from zero through seven
         */
        public int code() {
            return ordinal();
        }

        /**
         * Upgrades an unreliable mode before fragmentation while preserving sequencing and receipt flags.
         * Already reliable modes are returned unchanged.
         *
         * @return corresponding reliable mode
         * @throws IllegalArgumentException if an unsupported future mode has no reliable counterpart
         */
        public Reliability makeReliable() {
            if (isReliable) {
                return this;
            }
            switch (this) {
                case UNRELIABLE:
                    return RELIABLE;
                case UNRELIABLE_SEQUENCED:
                    return RELIABLE_SEQUENCED;
                case UNRELIABLE_ACK:
                    return RELIABLE_ACK;
                default:
                    throw new IllegalArgumentException("No reliable form of " + this);
            }
        }
    }

}
