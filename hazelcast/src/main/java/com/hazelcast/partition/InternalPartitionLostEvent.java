package com.hazelcast.partition;

import com.hazelcast.nio.Address;
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

    private int lostReplicaIndex;

    private Address eventSource;

    public InternalPartitionLostEvent() {
    }

    public InternalPartitionLostEvent(int partitionId, int lostReplicaIndex, Address eventSource) {
        this.partitionId = partitionId;
        this.lostReplicaIndex = lostReplicaIndex;
        this.eventSource = eventSource;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getLostReplicaIndex() {
        return lostReplicaIndex;
    }

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
        this.partitionId = in.readInt();
        this.lostReplicaIndex = in.readInt();
        this.eventSource = new Address();
        this.eventSource.readData(in);
    }

    @Override
    public String toString() {
        return "InternalPartitionLostEvent{" +
                "partitionId=" + partitionId +
                ", lostReplicaIndex=" + lostReplicaIndex +
                ", eventSource=" + eventSource +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InternalPartitionLostEvent that = (InternalPartitionLostEvent) o;

        if (lostReplicaIndex != that.lostReplicaIndex) {
            return false;
        }
        if (partitionId != that.partitionId) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = partitionId;
        result = 31 * result + lostReplicaIndex;
        return result;
    }
}
