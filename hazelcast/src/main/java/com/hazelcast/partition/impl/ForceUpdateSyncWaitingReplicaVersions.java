package com.hazelcast.partition.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionLostEvent;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationCycleOperation;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// Runs locally when the node becomes owner of a partition upon a node failure
// Finds the replicaIndices on the sync-waiting state. Those indices represents the lost backups of the partition.
// Therefore, it publishes InternalPartitionLostEvent objects to notify related services
// It also updates the version for lost replicas to the first available version value after the lost backups, or 0 if N/A
final class ForceUpdateSyncWaitingReplicaVersions
        extends AbstractOperation
        implements PartitionAwareOperation, MigrationCycleOperation {

    @Override
    public void run()
            throws Exception {
        final int partitionId = getPartitionId();
        final InternalPartitionService partitionService = getService();

        final long[] versions = partitionService
                .getPartitionReplicaVersions(partitionId); // returns the internal array itself, not the copy
        final List<Integer> lostReplicaIndices = getSyncWaitingReplicaIndices(versions);
        forceUpdateLostReplicaIndices(partitionId, versions, lostReplicaIndices);
        publishPartitionLostEvents(partitionId, lostReplicaIndices);
    }

    private List<Integer> getSyncWaitingReplicaIndices(final long[] versions) {
        final List<Integer> lostReplicaIndices = new ArrayList<Integer>();
        for (int replicaIndex = 1; replicaIndex <= versions.length; replicaIndex++) {
            if (versions[replicaIndex - 1] == InternalPartition.WAITING_SYNC) {
                lostReplicaIndices.add(replicaIndex);
            }
        }
        return lostReplicaIndices;
    }

    private void publishPartitionLostEvents(final int partitionId, final List<Integer> lostReplicaIndices) {
        for (Integer lostReplicaIndex : lostReplicaIndices) {
            // TODO Publish this event
            final InternalPartitionLostEvent partitionLostEvent = new InternalPartitionLostEvent(partitionId, lostReplicaIndex);
        }
    }

    private void forceUpdateLostReplicaIndices(final int partitionId, final long[] versions, final List<Integer> lostReplicaIndices) {
        final int lostReplicaIndicesSize = lostReplicaIndices.size();
        if (lostReplicaIndicesSize > 0) {
            getLogger().warning("Partition " + partitionId + " is lost for replica indices: " + lostReplicaIndices);

            final int biggestLostReplicaIndex = lostReplicaIndices.get(lostReplicaIndicesSize - 1);
            final long forcedVersion = biggestLostReplicaIndex <= versions.length ? versions[biggestLostReplicaIndex] : 0;
            for (int replicaIndex = biggestLostReplicaIndex; replicaIndex > 0; replicaIndex--) {
                versions[replicaIndex - 1] = forcedVersion;
            }
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        throw new UnsupportedOperationException();
    }
}
