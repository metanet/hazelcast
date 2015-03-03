package com.hazelcast.partition;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Internal event that is dispatched to @see com.hazelcast.spi.PartitionAwareService#onPartitionLostEvent()
 * It contains the id and number of backups of the lost partition
 */
public class InternalPartitionLostEvent implements DataSerializable {

    private int partitionId;

    private int lostBackupCount;

    public InternalPartitionLostEvent() {
    }

    public InternalPartitionLostEvent(int partitionId, int lostBackupCount) {
        this.partitionId = partitionId;
        this.lostBackupCount = lostBackupCount;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getLostBackupCount() {
        return lostBackupCount;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeInt(partitionId);
        out.writeInt(lostBackupCount);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        this.partitionId = in.readInt();
        this.lostBackupCount = in.readInt();
    }

    @Override
    public String toString() {
        return "InternalPartitionLostEvent{" +
                "partitionId=" + partitionId +
                ", lostBackupCount=" + lostBackupCount +
                '}';
    }
}
