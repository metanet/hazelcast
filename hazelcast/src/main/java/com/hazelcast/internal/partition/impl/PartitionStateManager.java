/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.partition.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.PartitionListener;
import com.hazelcast.internal.partition.PartitionStateGenerator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.membergroup.MemberGroup;
import com.hazelcast.partition.membergroup.MemberGroupFactory;
import com.hazelcast.partition.membergroup.MemberGroupFactoryFactory;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;

/**
 * TODO: Javadoc Pending...
 *
 */
public class PartitionStateManager {

    private final Node node;
    private final InternalPartitionServiceImpl partitionService;
    private final ILogger logger;

    private final int partitionCount;
    private final InternalPartitionImpl[] partitions;

    @Probe
    private final AtomicInteger stateVersion = new AtomicInteger();

    private final PartitionStateGenerator partitionStateGenerator;
    private final MemberGroupFactory memberGroupFactory;

    // updates will be done under lock, but reads will be multithreaded.
    // set to true when the partitions are assigned for the first time. remains true until partition service has been reset.
    private volatile boolean initialized;

    @Probe
    // can be read and written concurrently...
    private volatile int memberGroupsSize;

    public PartitionStateManager(Node node, InternalPartitionServiceImpl service, PartitionListener listener) {
        this.node = node;
        this.partitionService = service;
        this.logger = node.getLogger(getClass());

        partitionCount = partitionService.getPartitionCount();
        this.partitions = new InternalPartitionImpl[partitionCount];

        Address thisAddress = node.getThisAddress();
        for (int i = 0; i < partitionCount; i++) {
            this.partitions[i] = new InternalPartitionImpl(i, listener, thisAddress);
        }

        memberGroupFactory = MemberGroupFactoryFactory.newMemberGroupFactory(node.getConfig().getPartitionGroupConfig());
        partitionStateGenerator = new PartitionStateGeneratorImpl();
    }

    @Probe
    private int localPartitionCount() {
        int count = 0;
        for (InternalPartition partition : partitions) {
            if (partition.isLocal()) {
                count++;
            }
        }
        return count;
    }

    private Collection<MemberGroup> createMemberGroups() {
        final Collection<Member> members = node.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        return memberGroupFactory.createMemberGroups(members);
    }

    boolean initializePartitionAssignments() {
        ClusterState clusterState = node.getClusterService().getClusterState();
        if (clusterState != ClusterState.ACTIVE) {
            logger.warning("Partitions can't be assigned since cluster-state= " + clusterState);
            return false;
        }

        Collection<MemberGroup> memberGroups = createMemberGroups();
        if (memberGroups.isEmpty()) {
            logger.warning("No member group is available to assign partition ownership...");
            return false;
        }

        logger.info("Initializing cluster partition table arrangement...");
        Address[][] newState = partitionStateGenerator.initialize(memberGroups, partitionCount);
        if (newState.length != partitionCount) {
            throw new HazelcastException("Invalid partition count! "
                    + "Expected: " + partitionCount + ", Actual: " + newState.length);
        }

        // increment state version to make fail cluster state transaction
        // if it's started and not locked the state yet.
        stateVersion.incrementAndGet();
        clusterState = node.getClusterService().getClusterState();
        if (clusterState != ClusterState.ACTIVE) {
            // cluster state is either changed or locked, decrement version back and fail.
            stateVersion.decrementAndGet();
            logger.warning("Partitions can't be assigned since cluster-state= " + clusterState);
            return false;
        }

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            InternalPartitionImpl partition = partitions[partitionId];
            Address[] replicas = newState[partitionId];
            partition.setReplicaAddresses(replicas);
        }
        initialized = true;
        return true;
    }

    void setInitialState(Address[][] newState, int partitionStateVersion) {
        if (initialized) {
            throw new IllegalStateException("Partition table is already initialized!");
        }
        logger.info("Setting cluster partition table ...");
        boolean foundReplica = false;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            InternalPartitionImpl partition = partitions[partitionId];
            Address[] replicas = newState[partitionId];
            if (!foundReplica && replicas != null) {
                for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                    foundReplica |= replicas[i] != null;
                }
            }
            partition.setInitialReplicaAddresses(replicas);
        }
        stateVersion.set(partitionStateVersion);
        initialized = foundReplica;
    }

    void updateMemberGroupsSize() {
        final Collection<MemberGroup> groups = createMemberGroups();
        int size = 0;
        for (MemberGroup group : groups) {
            if (group.size() > 0) {
                size++;
            }
        }
        memberGroupsSize = size;
    }

    public int getMemberGroupsSize() {
        int size = memberGroupsSize;
        if (size > 0) {
            return size;
        }

        // size = 0 means service is not initialized yet.
        // return 1 if current node is a data member since there should be at least one member group
        return node.isLiteMember() ? 0 : 1;
    }

    void removeDeadAddress(Collection<MigrationInfo> migrations, Address address) {
        for (InternalPartitionImpl partition : partitions) {
            int index = partition.removeAddress(address);

            // address is not replica of this partition
            if (index == -1) {
                continue;
            }


            // if owner is null, promote 1st non-null backup to owner
            // don't touch to the other backups
            if (index == 0) {
                Address destination = null;
                for (int i = index + 1; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                    destination = partition.getReplicaAddress(i);
                    if (destination != null) {
                        partition.swapAddresses(0, i);
                        index = i;
                        break;
                    }
                }

                if (destination != null) {
                    // TODO: how do we inform the services about promotion?
                    // run promotion when you get this completed migration?
                    MigrationInfo migration = new MigrationInfo(partition.getPartitionId(), null, destination, -1, -1, 1, 0);
                    migration.setMaster(node.getThisAddress());
                    migration.setStatus(MigrationInfo.MigrationStatus.SUCCESS);
                    partitionService.getMigrationManager().addCompletedMigration(migration);
                }
            }

            logger.info("Partition: " + partition.getPartitionId() + ", Replica: " + index + " is removed: " + partition);

            if (partition.getOwnerOrNull() == null) {
                // we lost the partition!
                continue;
            }

            // search for a destination to assign empty index
            Address destination;
            for (int i = InternalPartition.MAX_REPLICA_COUNT - 1; i > index; i--) {
                destination = partition.getReplicaAddress(i);
                if (destination != null) {
                    MigrationInfo migration = new MigrationInfo(partition.getPartitionId(), null, destination,
                            -1, -1, i, index);
                    migrations.add(migration);
                    break;
                }
            }
        }
    }

    InternalPartition[] getPartitions() {
        return partitions;
    }

    InternalPartition[] getPartitionsCopy() {
        NopPartitionListener listener = new NopPartitionListener();
        InternalPartition[] result = new InternalPartition[partitions.length];
        for (int i = 0; i < partitionCount; i++) {
            result[i] = partitions[i].copy(listener);
        }
        return result;
    }

    public InternalPartitionImpl getPartitionImpl(int partitionId) {
        return partitions[partitionId];
    }

    Address[][] repartition() {
        if (!initialized) {
            return null;
        }
        Collection<MemberGroup> memberGroups = createMemberGroups();
        Address[][] newState = partitionStateGenerator.reArrange(memberGroups, partitions);

        if (newState == null) {
            if (logger.isFinestEnabled()) {
                logger.finest("Partition rearrangement failed. Number of member groups: " + memberGroups.size());
            }
        }

        return newState;
    }

    void setMigrating(int partitionId, boolean migrating) {
        partitions[partitionId].setMigrating(migrating);
    }

    void updateReplicaAddresses(int partitionId, Address[] replicaAddresses) {
        InternalPartitionImpl partition = partitions[partitionId];
        partition.setReplicaAddresses(replicaAddresses);
    }

    // called under partition service lock
    boolean setVersion(int version) {
        if (version < stateVersion.get()) {
            logger.warning("Master version should be greater than ours! Current: " + stateVersion.get()
                    + ", Master: " + version);
            return false;
        } else if (version == stateVersion.get()) {
            if (logger.isFineEnabled()) {
                logger.fine("Master version should be greater than ours! Current: " + stateVersion.get()
                        + ", Master: " + version);
            }

            return false;
        }

        stateVersion.set(version);
        return true;
    }

    public int getVersion() {
        return stateVersion.get();
    }

    void incrementVersion(int delta) {
        if (delta >= 0) {
            stateVersion.addAndGet(delta);
        } else {
            logger.warning("partition table version not incremented by " + delta);
        }
    }

    void incrementVersion() {
        stateVersion.incrementAndGet();
    }

    void setInitialized() {
        initialized = true;
    }

    public boolean isInitialized() {
        return initialized;
    }

    void reset() {
        initialized = false;
        stateVersion.set(0);
        for (InternalPartitionImpl partition : partitions) {
            partition.reset();
        }
    }
}
