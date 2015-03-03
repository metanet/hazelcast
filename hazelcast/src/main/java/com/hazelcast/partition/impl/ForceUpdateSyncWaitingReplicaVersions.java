package com.hazelcast.partition.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionLostEvent;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationCycleOperation;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.PartitionAwareService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;

// Runs locally when the node becomes owner of a partition upon a node failure
// Finds the replicaIndices that are on the sync-waiting state. Those indices represents the lost backups of the partition.
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
        final int biggestLostReplicaIndex = getBiggestLostReplicaIndex(versions);

        if (biggestLostReplicaIndex > 0) {
            forceUpdateLostReplicaIndices(partitionId, versions, biggestLostReplicaIndex);
            sendPartitionLostEvent(partitionId, biggestLostReplicaIndex);
        }
    }

    private int getBiggestLostReplicaIndex(final long[] versions) {
        int biggestLostReplicaIndex = 0;

        for (int replicaIndex = 1; replicaIndex <= versions.length; replicaIndex++) {
            if (versions[replicaIndex - 1] == InternalPartition.WAITING_SYNC) {
                biggestLostReplicaIndex = replicaIndex;
            }
        }
        return biggestLostReplicaIndex;
    }

    private void forceUpdateLostReplicaIndices(final int partitionId, final long[] versions, final int lostReplicaIndex) {
        getLogger().warning("Partition " + partitionId + " is lost for biggest replica index: " + lostReplicaIndex);

        final long forcedVersion = lostReplicaIndex <= versions.length ? versions[lostReplicaIndex] : 0;
        for (int replicaIndex = lostReplicaIndex; replicaIndex > 0; replicaIndex--) {
            versions[replicaIndex - 1] = forcedVersion;
        }
    }

    private void sendPartitionLostEvent(int partitionId, int lostReplicaIndex) {
        final InternalPartitionLostEvent partitionLostEvent = new InternalPartitionLostEvent(partitionId, lostReplicaIndex);
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        for (PartitionAwareService service : nodeEngine.getServices(PartitionAwareService.class)) {
            try {
                service.onPartitionLostEvent(partitionLostEvent);
            } catch (Throwable e) {
                getLogger().warning(
                        "Handling partitionLostEvent failed. Service: " + service.getClass() + " Event: " + partitionLostEvent,
                        e);
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
