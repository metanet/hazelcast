package com.hazelcast.core;

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

    public PartitionLostEvent() {
    }

    public PartitionLostEvent(int partitionId, int lostReplicaIndex) {
        this.partitionId = partitionId;
        this.lostReplicaIndex = lostReplicaIndex;
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

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeInt(partitionId);
        out.writeInt(lostReplicaIndex);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        partitionId = in.readInt();
        lostReplicaIndex = in.readInt();
    }

    @Override
    public String toString() {
        return "PartitionLostEvent{" +
                "partitionId=" + partitionId +
                ", lostReplicaIndex=" + lostReplicaIndex +
                '}';
    }
}
