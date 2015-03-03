package com.hazelcast.core;

/**
 * Used for providing information about the lost partition for a map
 *
 * @see com.hazelcast.map.listener.MapPartitionLostListener
 */
public class MapPartitionLostEvent extends AbstractIMapEvent {

    private static final long serialVersionUID = -7445734640964238109L;


    private final int lostPartitionId;

    public MapPartitionLostEvent(Object source, Member member, int eventType, int lostPartitionId) {
        super(source, member, eventType);
        this.lostPartitionId = lostPartitionId;
    }

    /**
     * Returns the partition id that has been lost for the given map
     *
     * @return the partition id that has been lost for the given map
     */
    public int getLostPartitionId() {
        return lostPartitionId;
    }

    @Override
    public String toString() {
        return "MapPartitionLostEvent{"
                + super.toString()
                + ", lostPartitionId=" + lostPartitionId
                + '}';
    }
}
