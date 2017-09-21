package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ServiceNamespace;

import java.io.IOException;

import static com.hazelcast.spi.partition.IPartition.MAX_BACKUP_COUNT;
import static com.hazelcast.util.Preconditions.checkTrue;

public class CheckNamespaceReplicaVersionsOperation extends Operation implements MigrationCycleOperation,
                                                                                 PartitionAwareOperation,
                                                                                 BackupAwareOperation,
                                                                                 IdentifiedDataSerializable {

    private int maxReplicaIndex;

    private ServiceNamespace namespace;

    private long[] versions;

    public CheckNamespaceReplicaVersionsOperation() {
    }

    public CheckNamespaceReplicaVersionsOperation(ServiceNamespace namespace, int maxReplicaIndex) {
        checkTrue(maxReplicaIndex > 0 && maxReplicaIndex < MAX_BACKUP_COUNT, "max replica index: "
                + maxReplicaIndex + "  should be a valid backup replica index");
        this.maxReplicaIndex = maxReplicaIndex;
        this.namespace = namespace;
    }

    @Override
    public void run() throws Exception {
        int partitionId = getPartitionId();
        InternalPartitionServiceImpl partitionService = getService();
        versions = partitionService.getPartitionReplicaVersionManager().getPartitionReplicaVersions(partitionId, namespace);
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public int getSyncBackupCount() {
        return maxReplicaIndex;
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new CheckNamespaceReplicaVersionsBackupOperation(namespace, versions);
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.CHECK_NAMESPACE_REPLICA_VERSIONS;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeObject(namespace);
        out.writeInt(maxReplicaIndex);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        namespace = in.readObject();
        maxReplicaIndex = in.readInt();
    }

}
