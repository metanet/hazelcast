package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionReplicaManager;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ServiceNamespace;

import java.io.IOException;

public class CheckNamespaceReplicaVersionsBackupOperation extends Operation implements BackupOperation,
                                                                                       PartitionAwareOperation,
                                                                                       IdentifiedDataSerializable {


    private ServiceNamespace namespace;

    private long[] versions;

    public CheckNamespaceReplicaVersionsBackupOperation() {
    }

    public CheckNamespaceReplicaVersionsBackupOperation(ServiceNamespace namespace, long[] versions) {
        this.namespace = namespace;
        this.versions = versions;
    }

    @Override
    public void run() throws Exception {
        InternalPartitionServiceImpl partitionService = getService();
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();
        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();

        long[] currentVersions = replicaManager.getPartitionReplicaVersions(partitionId, namespace);
        long currentVersion = currentVersions[replicaIndex - 1];
        long primaryVersion = versions[replicaIndex - 1];

        if (replicaManager.isPartitionReplicaVersionDirty(partitionId, namespace) || currentVersion != primaryVersion) {
            String msg = "Namespace: " + namespace + " has replica version mismatch for partitionId=" + partitionId
                    + " replicaIndex=" + replicaIndex + " primaryVersion=" + primaryVersion + " replicaVersion=" + currentVersion;
            throw new IllegalStateException(msg);
        }
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.CHECK_NAMESPACE_REPLICA_VERSIONS_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(namespace);
        out.writeLongArray(versions);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        namespace = in.readObject();
        versions = in.readLongArray();
    }

}
