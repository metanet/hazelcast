package com.hazelcast.core;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * An event fired when a partition lost its owner and all backups.
 *
 * @see Partition
 * @see PartitionService
 * @see PartitionLostListener
 */
public class PartitionLostEvent
        implements DataSerializable, PartitionEvent {

    private int partitionId;

    private int lostReplicaIndex;

    private Address eventSource;

    public PartitionLostEvent() {
    }

    public PartitionLostEvent(int partitionId, int lostReplicaIndex, Address eventSource) {
        this.partitionId = partitionId;
        this.lostReplicaIndex = lostReplicaIndex;
        this.eventSource = eventSource;
    }

    /**
     * Returns the lost partition id.
     *
     * @return the lost partition id.
     */
    @Override
    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Returns the replica index that is lost. O: the owner, 1: first backup, 2: second backup ...
     *
     * @return the number of lost backups for the partition
     */
    public int getLostReplicaIndex() {
        return lostReplicaIndex;
    }

    /**
     * Returns the address of the node that dispatches the event
     * @return the address of the node that dispatches the event
     */
    public Address getEventSource() {
        return eventSource;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeInt(partitionId);
        out.writeInt(lostReplicaIndex);
        eventSource.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        partitionId = in.readInt();
        lostReplicaIndex = in.readInt();
        eventSource = new Address();
        eventSource.readData(in);
    }

    @Override
    public String toString() {
        return "PartitionLostEvent{" +
                "partitionId=" + partitionId +
                ", lostReplicaIndex=" + lostReplicaIndex +
                ", eventSource=" + eventSource +
                '}';
    }
}
