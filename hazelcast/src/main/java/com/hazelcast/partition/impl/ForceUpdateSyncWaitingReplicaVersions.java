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
import java.util.Arrays;

// Runs locally when the node becomes owner of a partition upon a node failure
// Finds the replicaIndices that are on the sync-waiting state. Those indices represents the lost backups of the partition.
// Therefore, it publishes InternalPartitionLostEvent objects to notify related services
// It also updates the version for lost replicas to the first available version value after the lost backups, or 0 if N/A
final class ForceUpdateSyncWaitingReplicaVersions
        extends AbstractOperation
        implements PartitionAwareOperation, MigrationCycleOperation {

    private final PartitionReplicaAssignmentReason reason;

    public ForceUpdateSyncWaitingReplicaVersions(PartitionReplicaAssignmentReason reason) {
        this.reason = reason;
    }

    @Override
    public void run()
            throws Exception {
        final int partitionId = getPartitionId();
        final InternalPartitionService partitionService = getService();
        // returns the internal array itself, not the copy
        final long[] versions = partitionService.getPartitionReplicaVersions(partitionId);

        if (reason == PartitionReplicaAssignmentReason.MEMBER_REMOVED) {
            final int lostReplicaIndex = getLostReplicaIndex(versions);
            if (lostReplicaIndex > 0) {
                if (getLogger().isFinestEnabled()) {
                    getLogger().finest("Partition replica is lost! partitionId=" + partitionId + " lostReplicaIndex=" + lostReplicaIndex + " replicaVersions=" + Arrays.toString(versions));
                }

                forceUpdateLostReplicaIndices(versions, lostReplicaIndex);
            }

            sendPartitionLostEvent(partitionId, lostReplicaIndex);
        } else {
            if (getLogger().isFinestEnabled()) {
                getLogger().finest("Resetting all SYNC_WAITING Versions. partitionId=" + partitionId
                        + " versionsBeforeReset=" + Arrays.toString(versions));
            }

            resetSyncWaitingVersions(versions);
        }
    }

    private void resetSyncWaitingVersions(long[] versions) {
        for (int replicaIndex = 1; replicaIndex < versions.length; replicaIndex++) {
            if (versions[replicaIndex - 1] == InternalPartition.SYNC_WAITING) {
                versions[replicaIndex - 1] = 0;
            }
        }
    }

    private int getLostReplicaIndex(final long[] versions) {
        int biggestLostReplicaIndex = 0;

        for (int replicaIndex = 1; replicaIndex <= versions.length; replicaIndex++) {
            if (versions[replicaIndex - 1] == InternalPartition.SYNC_WAITING) {
                biggestLostReplicaIndex = replicaIndex;
            }
        }

        return biggestLostReplicaIndex;
    }

    private void forceUpdateLostReplicaIndices(final long[] versions, final int lostReplicaIndex) {
        final long forcedVersion = lostReplicaIndex <= versions.length ? versions[lostReplicaIndex] : 0;
        for (int replicaIndex = lostReplicaIndex; replicaIndex > 0; replicaIndex--) {
            versions[replicaIndex - 1] = forcedVersion;
        }
    }

    private void sendPartitionLostEvent(int partitionId, int lostReplicaIndex) {
        final InternalPartitionLostEvent partitionLostEvent = new InternalPartitionLostEvent(partitionId, lostReplicaIndex, getNodeEngine().getThisAddress());
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
