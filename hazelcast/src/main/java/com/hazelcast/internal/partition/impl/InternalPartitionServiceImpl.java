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
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationEvent.MigrationStatus;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.PartitionInfo;
import com.hazelcast.internal.partition.PartitionListener;
import com.hazelcast.internal.partition.PartitionReplicaChangeReason;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.PartitionServiceProxy;
import com.hazelcast.internal.partition.impl.InternalMigrationListener.MigrationParticipant;
import com.hazelcast.internal.partition.operation.ClearReplicaOperation;
import com.hazelcast.internal.partition.operation.FetchPartitionStateOperation;
import com.hazelcast.internal.partition.operation.FinalizeMigrationOperation;
import com.hazelcast.internal.partition.operation.HasOngoingMigration;
import com.hazelcast.internal.partition.operation.IsReplicaVersionSync;
import com.hazelcast.internal.partition.operation.MigrationCommitOperation;
import com.hazelcast.internal.partition.operation.MigrationRequestOperation;
import com.hazelcast.internal.partition.operation.PartitionStateOperation;
import com.hazelcast.internal.partition.operation.PromoteFromBackupOperation;
import com.hazelcast.internal.partition.operation.ResetReplicaVersionOperation;
import com.hazelcast.internal.partition.operation.SyncReplicaVersion;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.IPartitionLostEvent;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.PartitionEvent;
import com.hazelcast.partition.PartitionEventListener;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareService;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;
import com.hazelcast.util.FutureUtil.ExceptionHandler;
import com.hazelcast.util.HashUtil;
import com.hazelcast.util.Preconditions;
import com.hazelcast.util.scheduler.CoalescingDelayedTrigger;
import com.hazelcast.util.scheduler.ScheduledEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createErrorLoggingResponseHandler;
import static com.hazelcast.util.FutureUtil.logAllExceptions;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * The {@link InternalPartitionService} implementation.
 */
public class InternalPartitionServiceImpl implements InternalPartitionService, ManagedService,
        EventPublishingService<PartitionEvent, PartitionEventListener<PartitionEvent>>, PartitionAwareService {

    private static final String EXCEPTION_MSG_PARTITION_STATE_SYNC_TIMEOUT = "Partition state sync invocation timed out";

    private static final int DEFAULT_PAUSE_MILLIS = 1000;
    private static final int PARTITION_OWNERSHIP_WAIT_MILLIS = 10;
    private static final int REPLICA_SYNC_CHECK_TIMEOUT_SECONDS = 10;

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    private final int partitionCount;

    private final MigrationThread migrationThread;

    private final long partitionMigrationInterval;
    private final long partitionMigrationTimeout;
    private final long backupSyncCheckInterval;

    private final PartitionServiceProxy proxy;
    private final Lock lock = new ReentrantLock();
    private final InternalPartitionListener partitionListener;

    private final PartitionStateManager partitionStateManager = null;
    private final MigrationManager migrationManager = null;
    private final PartitionReplicaManager replicaManager;


    private final MigrationQueue migrationQueue = new MigrationQueue();
    private final AtomicBoolean migrationAllowed = new AtomicBoolean(true);
    @Probe(name = "lastRepartitionTime")
    private final AtomicLong lastRepartitionTime = new AtomicLong();
    private final CoalescingDelayedTrigger delayedResumeMigrationTrigger;

    private final ExceptionHandler partitionStateSyncTimeoutHandler;

    // updates will be done under lock, but reads will be multithreaded.
    private volatile MigrationInfo activeMigrationInfo;

    // both reads and updates will be done under lock!
    private final LinkedList<MigrationInfo> completedMigrations = new LinkedList<MigrationInfo>();

    @Probe
    private final AtomicLong completedMigrationCounter = new AtomicLong();

    private volatile InternalMigrationListener internalMigrationListener
            = new InternalMigrationListener.NopInternalMigrationListener();

    private volatile Address lastMaster;

    public InternalPartitionServiceImpl(Node node) {
        this.partitionCount = node.groupProperties.getInteger(GroupProperty.PARTITION_COUNT);
        this.node = node;
        this.nodeEngine = node.nodeEngine;
        this.logger = node.getLogger(InternalPartitionService.class);

        replicaManager = new PartitionReplicaManager(node, this);

        partitionStateSyncTimeoutHandler =
                logAllExceptions(logger, EXCEPTION_MSG_PARTITION_STATE_SYNC_TIMEOUT, Level.FINEST);

        partitionListener = new InternalPartitionListener(this, node.getThisAddress());

        long intervalMillis = node.groupProperties.getMillis(GroupProperty.PARTITION_MIGRATION_INTERVAL);
        partitionMigrationInterval = (intervalMillis > 0 ? intervalMillis : 0);

        partitionMigrationTimeout = node.groupProperties.getMillis(GroupProperty.PARTITION_MIGRATION_TIMEOUT);

        migrationThread = new MigrationThread(node);
        proxy = new PartitionServiceProxy(this);

        ExecutionService executionService = nodeEngine.getExecutionService();

        long maxMigrationDelayMs = calculateMaxMigrationDelayOnMemberRemoved();
        long minMigrationDelayMs = calculateMigrationDelayOnMemberRemoved(maxMigrationDelayMs);
        this.delayedResumeMigrationTrigger = new CoalescingDelayedTrigger(
                executionService, minMigrationDelayMs, maxMigrationDelayMs, new Runnable() {
            @Override
            public void run() {
                resumeMigration();
            }
        });

        long definedBackupSyncCheckInterval = node.groupProperties.getSeconds(GroupProperty.PARTITION_BACKUP_SYNC_INTERVAL);
        backupSyncCheckInterval = definedBackupSyncCheckInterval > 0 ? definedBackupSyncCheckInterval : 1;
        nodeEngine.getMetricsRegistry().scanAndRegister(this, "partitions");
    }

    private long calculateMaxMigrationDelayOnMemberRemoved() {
        // hard limit for migration pause is half of the call timeout. otherwise we might experience timeouts
        return node.groupProperties.getMillis(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS) / 2;
    }

    private long calculateMigrationDelayOnMemberRemoved(long maxDelayMs) {
        long migrationDelayMs = node.groupProperties.getMillis(GroupProperty.MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS);

        long connectionErrorDetectionIntervalMs = node.groupProperties.getMillis(GroupProperty.CONNECTION_MONITOR_INTERVAL)
                * node.groupProperties.getInteger(GroupProperty.CONNECTION_MONITOR_MAX_FAULTS) * 5;
        migrationDelayMs = Math.max(migrationDelayMs, connectionErrorDetectionIntervalMs);

        long heartbeatIntervalMs = node.groupProperties.getMillis(GroupProperty.HEARTBEAT_INTERVAL_SECONDS);
        migrationDelayMs = Math.max(migrationDelayMs, heartbeatIntervalMs * 3);

        migrationDelayMs = min(migrationDelayMs, maxDelayMs);
        return migrationDelayMs;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        migrationThread.start();

        int partitionTableSendInterval = node.groupProperties.getSeconds(GroupProperty.PARTITION_TABLE_SEND_INTERVAL);
        if (partitionTableSendInterval <= 0) {
            partitionTableSendInterval = 1;
        }
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleAtFixedRate(new SendPartitionRuntimeStateTask(),
                partitionTableSendInterval, partitionTableSendInterval, TimeUnit.SECONDS);

        executionService.scheduleWithFixedDelay(new SyncReplicaVersionTask(),
                backupSyncCheckInterval, backupSyncCheckInterval, TimeUnit.SECONDS);
    }

    @Override
    public Address getPartitionOwner(int partitionId) {
       return partitionStateManager.getPartitionOwner(partitionId);
    }

    @Override
    public Address getPartitionOwnerOrWait(int partitionId) {
        return partitionStateManager.getPartitionOwnerOrWait(partitionId);
    }

    boolean isClusterFormedByOnlyLiteMembers() {
        final ClusterServiceImpl clusterService = node.getClusterService();
        return clusterService.getMembers(DATA_MEMBER_SELECTOR).isEmpty();
    }

    @Override
    public void firstArrangement() {
        partitionStateManager.firstArrangement();
    }

    @Override
    public int getMemberGroupsSize() {
        return partitionStateManager.getMemberGroupsSize();
    }

    @Probe(name = "maxBackupCount")
    @Override
    public int getMaxAllowedBackupCount() {
        return max(min(getMemberGroupsSize() - 1, InternalPartition.MAX_BACKUP_COUNT), 0);
    }

    @Override
    public void memberAdded(MemberImpl member) {
        if (!member.localMember()) {
            partitionStateManager.updateMemberGroupsSize();
        }
        lastMaster = node.getMasterAddress();

        if (node.isMaster()) {
            lock.lock();
            try {
                migrationQueue.clear();
                if (partitionStateManager.isInitialized()) {
                    final ClusterState clusterState = nodeEngine.getClusterService().getClusterState();
                    if (clusterState == ClusterState.ACTIVE) {
                        partitionStateManager.incrementVersion();
                        migrationQueue.add(new RepartitioningTask());
                    }

                    // send initial partition table to newly joined node.
                    PartitionStateOperation op = new PartitionStateOperation(createPartitionState());
                    nodeEngine.getOperationService().send(op, member.getAddress());
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void memberRemoved(final MemberImpl member) {
        logger.info("Removing " + member);
        partitionStateManager.updateMemberGroupsSize();

        final Address deadAddress = member.getAddress();
        final Address thisAddress = node.getThisAddress();

        lock.lock();
        try {
            // TODO: why not increment only on master and publish it?
            if (partitionStateManager.isInitialized() && node.getClusterService().getClusterState() == ClusterState.ACTIVE) {
                partitionStateManager.incrementVersion();
            }

            // TODO: should not clear all! but only migration tasks? only to/from dead member?
            migrationQueue.clear();

            if (node.isMaster() && !thisAddress.equals(lastMaster)) {
                Runnable runnable = new FixPartitionTableTask();
                migrationQueue.add(runnable);
            }

            lastMaster = node.getMasterAddress();

//            // TODO: why not just rollback them? unless it's committed by target
//            invalidateActiveMigrationsBelongingTo(deadAddress);

            // TODO: still need to pause?
            // Pause migration and let all other members notice the dead member
            // and fix their own partitions.
            // Otherwise new master may take action fast and send new partition state
            // before other members realize the dead one.
//            pauseMigration();

            // TODO: this looks fine, a local optimization
            replicaManager.cancelReplicaSyncRequestsTo(deadAddress);

            if (node.isMaster()) {
                migrationQueue.add(new Runnable() {
                    @Override
                    public void run() {
                        if (!partitionStateManager.isInitialized()) {
                            return;
                        }
                        // invalidate, migration will be eventually rolled back
                        invalidateActiveMigrationsBelongingTo(deadAddress);

                        partitionStateManager.removeDeadAddress(deadAddress);

                        syncPartitionRuntimeState();
                    }
                });

                migrationQueue.add(new RepartitioningTask());
            }

//            resumeMigrationEventually();
        } finally {
            lock.unlock();
        }
    }

    private void invalidateActiveMigrationsBelongingTo(Address deadAddress) {
        if (activeMigrationInfo != null) {
            if (deadAddress.equals(activeMigrationInfo.getSource())
                    || deadAddress.equals(activeMigrationInfo.getDestination())) {
                activeMigrationInfo.invalidate();
            }
        }
    }

    public void cancelReplicaSyncRequestsTo(Address deadAddress) {
        lock.lock();
        try {
            replicaManager.cancelReplicaSyncRequestsTo(deadAddress);
        } finally {
            lock.unlock();
        }
    }

    private void resumeMigrationEventually() {
        delayedResumeMigrationTrigger.executeWithDelay();
    }

    public PartitionRuntimeState createPartitionState() {
        return createPartitionState(getCurrentMembersAndMembersRemovedWhileNotClusterNotActive());
    }

    private PartitionRuntimeState createPartitionState(Collection<MemberImpl> members) {
        if (!partitionStateManager.isInitialized()) {
            return null;
        }

        lock.lock();
        try {
            List<MemberInfo> memberInfos = new ArrayList<MemberInfo>(members.size());
            for (MemberImpl member : members) {
                MemberInfo memberInfo = new MemberInfo(member.getAddress(), member.getUuid(), member.getAttributes());
                memberInfos.add(memberInfo);
            }
            ArrayList<MigrationInfo> migrationInfos = new ArrayList<MigrationInfo>(completedMigrations);
            ILogger logger = node.getLogger(PartitionRuntimeState.class);

            InternalPartition[] partitions = partitionStateManager.getPartitions();

            PartitionRuntimeState state =
                    new PartitionRuntimeState(logger, memberInfos, partitions, migrationInfos, getPartitionStateVersion());
            state.setActiveMigration(activeMigrationInfo);
            return state;
        } finally {
            lock.unlock();
        }
    }

    PartitionRuntimeState createMigrationCommitPartitionState(MigrationInfo migrationInfo, Address[] newAddresses) {
        if (!partitionStateManager.isInitialized()) {
            return null;
        }
        Collection<MemberImpl> members = node.clusterService.getMemberImpls();
        lock.lock();
        try {
            List<MemberInfo> memberInfos = new ArrayList<MemberInfo>(members.size());
            for (MemberImpl member : members) {
                MemberInfo memberInfo = new MemberInfo(member.getAddress(), member.getUuid(), member.getAttributes());
                memberInfos.add(memberInfo);
            }
            ArrayList<MigrationInfo> completedMigrations = new ArrayList<MigrationInfo>(this.completedMigrations);
            ILogger logger = node.getLogger(PartitionRuntimeState.class);

            InternalPartition[] partitions = partitionStateManager.getPartitionsCopy();

            int partitionId = migrationInfo.getPartitionId();
            partitionStateManager.setReplicaAddresses(partitions[partitionId], newAddresses);

            return new PartitionRuntimeState(logger, memberInfos, partitions, completedMigrations, getPartitionStateVersion() + 1);
        } finally {
            lock.unlock();
        }
    }

    void publishPartitionRuntimeState() {
        if (!partitionStateManager.isInitialized()) {
            // do not send partition state until initialized!
            return;
        }

        if (!node.isMaster()) {
            return;
        }

        if (!isReplicaSyncAllowed()) {
            // migration is disabled because of a member leave, wait till enabled!
            return;
        }

        lock.lock();
        try {
            Collection<MemberImpl> members = getCurrentMembersAndMembersRemovedWhileNotClusterNotActive();
            PartitionRuntimeState partitionState = createPartitionState(members);
            PartitionStateOperation op = new PartitionStateOperation(partitionState);

            OperationService operationService = nodeEngine.getOperationService();
            final ClusterServiceImpl clusterService = node.clusterService;
            for (MemberImpl member : members) {
                if (!(member.localMember() || clusterService.isMemberRemovedWhileClusterIsNotActive(member.getAddress()))) {
                    try {
                        operationService.send(op, member.getAddress());
                    } catch (Exception e) {
                        logger.finest(e);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    void syncPartitionRuntimeState() {
        syncPartitionRuntimeState(node.clusterService.getMemberImpls());
    }

    private boolean commitMigrationToDestination(MigrationInfo migrationInfo, Address[] newAddresses) {
        if (!node.isMaster()) {
            return false;
        }

        if (node.getThisAddress().equals(migrationInfo.getDestination())) {
            return true;
        }

        try {
            PartitionRuntimeState partitionState = createMigrationCommitPartitionState(migrationInfo, newAddresses);
            MigrationCommitOperation operation = new MigrationCommitOperation(partitionState);
            Future<Boolean> future = nodeEngine.getOperationService()
                                                             .createInvocationBuilder(SERVICE_NAME, operation,
                                                                     migrationInfo.getDestination())
                                                             .setTryCount(Integer.MAX_VALUE)
                                                             .setCallTimeout(Long.MAX_VALUE).invoke();
            future.get();
            return true;
        } catch (Throwable t) {
            if (t instanceof MemberLeftException || t instanceof TargetNotMemberException) {
                logger.warning("Migration commit failed for " + migrationInfo + " since destination left the cluster");
            } else {
                logger.severe("Migration commit failed for " + migrationInfo + " with " + t);
            }

            return false;
        }
    }

    private void syncPartitionRuntimeState(Collection<MemberImpl> members) {
        if (!partitionStateManager.isInitialized()) {
            // do not send partition state until initialized!
            return;
        }

        if (!node.isMaster()) {
            return;
        }

        lock.lock();
        try {
            PartitionRuntimeState partitionState = createPartitionState(members);
            OperationService operationService = nodeEngine.getOperationService();

            List<Future> calls = firePartitionStateOperation(members, partitionState, operationService);
            waitWithDeadline(calls, 3, TimeUnit.SECONDS, partitionStateSyncTimeoutHandler);
        } finally {
            lock.unlock();
        }
    }

    private List<Future> firePartitionStateOperation(Collection<MemberImpl> members,
                                                     PartitionRuntimeState partitionState,
                                                     OperationService operationService) {
        final ClusterServiceImpl clusterService = node.clusterService;
        List<Future> calls = new ArrayList<Future>(members.size());
        for (MemberImpl member : members) {
            if (!(member.localMember() || clusterService.isMemberRemovedWhileClusterIsNotActive(member.getAddress()))) {
                try {
                    Address address = member.getAddress();
                    PartitionStateOperation operation = new PartitionStateOperation(partitionState, true);
                    Future<Object> f = operationService.invokeOnTarget(SERVICE_NAME, operation, address);
                    calls.add(f);
                } catch (Exception e) {
                    logger.finest(e);
                }
            }
        }
        return calls;
    }

    public void processPartitionRuntimeState(PartitionRuntimeState partitionState) {
        lock.lock();
        try {
            final Address sender = partitionState.getEndpoint();
            if (!node.getNodeExtension().isStartCompleted()) {
                logger.warning("Ignoring received partition table, startup is not completed yet. Sender: " + sender);
                return;
            }

            final Address master = node.getMasterAddress();
            if (node.isMaster()) {
                logger.warning("This is the master node and received a PartitionRuntimeState from "
                        + sender + ". Ignoring incoming state! ");
                return;
            } else {
                if (sender == null || !sender.equals(master)) {
                    if (node.clusterService.getMember(sender) == null) {
                        logger.severe("Received a ClusterRuntimeState from an unknown member!"
                                + " => Sender: " + sender + ", Master: " + master + "! ");
                        return;
                    } else {
                        logger.warning("Received a ClusterRuntimeState, but its sender doesn't seem to be master!"
                                + " => Sender: " + sender + ", Master: " + master + "! "
                                + "(Ignore if master node has changed recently.)");
                        return;
                    }
                }
            }

            applyNewState(partitionState, sender);
        } finally {
            lock.unlock();
        }
    }

    private void applyNewState(PartitionRuntimeState partitionState, Address sender) {
        partitionStateManager.setVersion(partitionState.getVersion());
        partitionStateManager.setInitialized();

        PartitionInfo[] state = partitionState.getPartitions();
        filterAndLogUnknownAddressesInPartitionTable(sender, state);
        finalizeOrRollbackMigration(partitionState, state);
    }

    private void finalizeOrRollbackMigration(PartitionRuntimeState partitionState, PartitionInfo[] state) {
        Collection<MigrationInfo> completedMigrations = partitionState.getCompletedMigrations();
        for (MigrationInfo completedMigration : completedMigrations) {
            addCompletedMigration(completedMigration);
            int partitionId = completedMigration.getPartitionId();
            PartitionInfo partitionInfo = state[partitionId];
            // mdogan:
            // Each partition should be updated right after migration is finalized
            // at the moment, it doesn't cause any harm to existing services,
            // because we have a `migrating` flag in partition which is cleared during migration finalization.
            // But from API point of view, we should provide explicit guarantees.
            // For the time being, leaving this stuff as is to not to change behaviour.

            // TODO BASRI IS THIS STILL NECESSARY? CAN WE MOVE UPDATEALLPARTITIONS(state) BEFORE FOR LOOP AND REMOVE NEXT LINE?
            partitionStateManager.updateReplicaAddresses(partitionInfo.getPartitionId(), partitionInfo.getReplicaAddresses());
            scheduleActiveMigrationFinalization(completedMigration);
        }

        updateAllPartitions(state);
    }

    private void updateAllPartitions(PartitionInfo[] state) {
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            final PartitionInfo partitionInfo = state[partitionId];
            partitionStateManager.updateReplicaAddresses(partitionInfo.getPartitionId(), partitionInfo.getReplicaAddresses());
        }
    }

    private void filterAndLogUnknownAddressesInPartitionTable(Address sender, PartitionInfo[] state) {
        final Set<Address> unknownAddresses = new HashSet<Address>();
        for (int partitionId = 0; partitionId < state.length; partitionId++) {
            PartitionInfo partitionInfo = state[partitionId];
            searchUnknownAddressesInPartitionTable(sender, unknownAddresses, partitionId, partitionInfo);
        }
        logUnknownAddressesInPartitionTable(sender, unknownAddresses);
    }

    private void logUnknownAddressesInPartitionTable(Address sender, Set<Address> unknownAddresses) {
        if (!unknownAddresses.isEmpty() && logger.isLoggable(Level.WARNING)) {
            StringBuilder s = new StringBuilder("Following unknown addresses are found in partition table")
                    .append(" sent from master[").append(sender).append("].")
                    .append(" (Probably they have recently joined or left the cluster.)")
                    .append(" {");
            for (Address address : unknownAddresses) {
                s.append("\n\t").append(address);
            }
            s.append("\n}");
            logger.warning(s.toString());
        }
    }

    private void searchUnknownAddressesInPartitionTable(Address sender, Set<Address> unknownAddresses, int partitionId,
                                                        PartitionInfo partitionInfo) {
        final ClusterServiceImpl clusterService = node.clusterService;
        final ClusterState clusterState = clusterService.getClusterState();
        for (int index = 0; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
            Address address = partitionInfo.getReplicaAddress(index);
            if (address != null && getMember(address) == null) {
                if (clusterState == ClusterState.ACTIVE || !clusterService.isMemberRemovedWhileClusterIsNotActive(address)) {
                    if (logger.isFinestEnabled()) {
                        logger.finest(
                                "Unknown " + address + " found in partition table sent from master "
                                        + sender + ". It has probably already left the cluster. partitionId="
                                        + partitionId);
                    }
                    unknownAddresses.add(address);
                }
            }
        }
    }

    private void scheduleActiveMigrationFinalization(final MigrationInfo migrationInfo) {
        lock.lock();
        try {
            if (migrationInfo.equals(activeMigrationInfo)) {
                if (activeMigrationInfo.startProcessing()) {
                    finalizeMigrationInfo(activeMigrationInfo);
                } else {
                    logger.info("Scheduling finalization of " + migrationInfo
                            + ", because migration process is currently running.");
                    nodeEngine.getExecutionService().schedule(new Runnable() {
                        @Override
                        public void run() {
                            scheduleActiveMigrationFinalization(migrationInfo);
                        }
                    }, 3, TimeUnit.SECONDS);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void finalizeMigrationInfo(MigrationInfo migrationInfo) {
        try {
            Address thisAddress = node.getThisAddress();
            boolean source = thisAddress.equals(migrationInfo.getSource());
            boolean destination = thisAddress.equals(migrationInfo.getDestination());
            if (source || destination) {
                int partitionId = migrationInfo.getPartitionId();
                InternalPartitionImpl migratingPartition = partitionStateManager.getPartitionImpl(partitionId);
                Address ownerAddress = migratingPartition.getOwnerOrNull();
                boolean success = migrationInfo.getDestination().equals(ownerAddress);
                MigrationParticipant participant = source ? MigrationParticipant.SOURCE : MigrationParticipant.DESTINATION;
                if (success) {
                    internalMigrationListener.onMigrationCommit(participant, migrationInfo);
                } else {
                    internalMigrationListener.onMigrationRollback(participant, migrationInfo);
                }

                MigrationEndpoint endpoint = source ? MigrationEndpoint.SOURCE : MigrationEndpoint.DESTINATION;
                FinalizeMigrationOperation op = new FinalizeMigrationOperation(endpoint, success);
                op.setPartitionId(partitionId)
                        .setNodeEngine(nodeEngine)
                        .setValidateTarget(false)
                        .setService(this);
                nodeEngine.getOperationService().executeOperation(op);
            } else {
                logger.severe("Failed to finalize migration because this member " + thisAddress
                        + " is not a participant of the migration: " + migrationInfo);
            }
        } catch (Exception e) {
            logger.warning(e);
        } finally {
            migrationInfo.doneProcessing();
        }
    }

    public boolean addActiveMigration(MigrationInfo migrationInfo) {
        lock.lock();
        try {
            if (activeMigrationInfo == null) {
                partitionStateManager.setMigrating(migrationInfo.getPartitionId(), true);
                activeMigrationInfo = migrationInfo;
                return true;
            }

            logger.warning(migrationInfo + " not added! Already existing active migration: " + activeMigrationInfo);
            return false;
        } finally {
            lock.unlock();
        }
    }

    MemberImpl getMasterMember() {
        return node.clusterService.getMember(node.getMasterAddress());
    }

    public MigrationInfo getActiveMigration(int partitionId) {
        MigrationInfo activeMigrationInfo = this.activeMigrationInfo;
        if (activeMigrationInfo != null && activeMigrationInfo.getPartitionId() == partitionId) {
            return activeMigrationInfo;
        }

        return null;
    }

    public boolean removeActiveMigration(int partitionId) {
        boolean success = false;
        lock.lock();
        try {
            if (activeMigrationInfo != null) {
                if (activeMigrationInfo.getPartitionId() == partitionId) {
                    partitionStateManager.setMigrating(partitionId, false);
                    this.activeMigrationInfo = null;
                    success = true;
                } else if (logger.isFinestEnabled()) {
                    logger.finest("active migration not remove because it has different partitionId! partitionId=" + partitionId
                    + " active migration=" + activeMigrationInfo);
                }
            }
        } finally {
            lock.unlock();
        }

        return success;
    }

    private void addCompletedMigration(MigrationInfo migrationInfo) {
        completedMigrationCounter.incrementAndGet();

        lock.lock();
        try {
            if (completedMigrations.size() > 25) {
                completedMigrations.removeFirst();
            }
            completedMigrations.add(migrationInfo);
        } finally {
            lock.unlock();
        }
    }

    private void evictCompletedMigrations() {
        lock.lock();
        try {
            if (!completedMigrations.isEmpty()) {
                completedMigrations.removeFirst();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public InternalPartition[] getPartitions() {
        //a defensive copy is made to prevent breaking with the old approach, but imho not needed
        InternalPartition[] result = new InternalPartition[partitionCount];
        System.arraycopy(partitionStateManager.getPartitions(), 0, result, 0, partitionCount);
        return result;
    }

    public MemberImpl getMember(Address address) {
        return node.clusterService.getMember(address);
    }

    @Override
    public InternalPartition getPartition(int partitionId) {
        return getPartition(partitionId, true);
    }

    @Override
    public InternalPartition getPartition(int partitionId, boolean triggerOwnerAssignment) {
        return partitionStateManager.getPartition(partitionId, triggerOwnerAssignment);
    }

    @Override
    public boolean prepareToSafeShutdown(long timeout, TimeUnit unit) {
        long timeoutInMillis = unit.toMillis(timeout);
        long sleep = DEFAULT_PAUSE_MILLIS;
        while (timeoutInMillis > 0) {
            while (timeoutInMillis > 0 && shouldWaitMigrationOrBackups(Level.INFO)) {
                timeoutInMillis = sleepWithBusyWait(timeoutInMillis, sleep);
            }
            if (timeoutInMillis <= 0) {
                break;
            }

            if (node.isMaster()) {
                final List<MemberImpl> members = getCurrentMembersAndMembersRemovedWhileNotClusterNotActive();
                syncPartitionRuntimeState(members);
            } else {
                timeoutInMillis = waitForOngoingMigrations(timeoutInMillis, sleep);
                if (timeoutInMillis <= 0) {
                    break;
                }
            }

            long start = Clock.currentTimeMillis();
            boolean ok = checkReplicaSyncState();
            timeoutInMillis -= (Clock.currentTimeMillis() - start);
            if (ok) {
                logger.finest("Replica sync state before shutdown is OK");
                return true;
            } else {
                if (timeoutInMillis <= 0) {
                    break;
                }
                logger.info("Some backup replicas are inconsistent with primary, waiting for synchronization. Timeout: "
                        + timeoutInMillis + "ms");
                timeoutInMillis = sleepWithBusyWait(timeoutInMillis, sleep);
            }
        }
        return false;
    }

    private List<MemberImpl> getCurrentMembersAndMembersRemovedWhileNotClusterNotActive() {
        final List<MemberImpl> members = new ArrayList<MemberImpl>();
        members.addAll(node.clusterService.getMemberImpls());
        members.addAll(node.clusterService.getMembersRemovedWhileClusterIsNotActive());
        return members;
    }

    private long waitForOngoingMigrations(long timeoutInMillis, long sleep) {
        long timeout = timeoutInMillis;
        while (timeout > 0 && hasOnGoingMigrationMaster(Level.WARNING)) {
            // ignore elapsed time during master inv.
            logger.info("Waiting for the master node to complete remaining migrations!");
            timeout = sleepWithBusyWait(timeout, sleep);
        }
        return timeout;
    }

    private long sleepWithBusyWait(long timeoutInMillis, long sleep) {
        try {
            //noinspection BusyWait
            Thread.sleep(sleep);
        } catch (InterruptedException ie) {
            logger.finest("Busy wait interrupted", ie);
        }
        return timeoutInMillis - sleep;
    }

    @Override
    public boolean isMemberStateSafe() {
        return getMemberState() == InternalPartitionServiceState.SAFE;
    }

    public InternalPartitionServiceState getMemberState() {
        if (hasOnGoingMigrationLocal()) {
            return InternalPartitionServiceState.MIGRATION_LOCAL;
        }

        if (!node.isMaster()) {
            if (hasOnGoingMigrationMaster(Level.OFF)) {
                return InternalPartitionServiceState.MIGRATION_ON_MASTER;
            }
        }

        return isReplicaInSyncState() ? InternalPartitionServiceState.SAFE : InternalPartitionServiceState.REPLICA_NOT_SYNC;
    }

    @Override
    public boolean hasOnGoingMigration() {
        return hasOnGoingMigrationLocal() || (!node.isMaster() && hasOnGoingMigrationMaster(Level.FINEST));
    }

    private boolean hasOnGoingMigrationMaster(Level level) {
        Address masterAddress = node.getMasterAddress();
        if (masterAddress == null) {
            return node.joined();
        }
        Operation operation = new HasOngoingMigration();
        OperationService operationService = nodeEngine.getOperationService();
        InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation,
                masterAddress);
        Future future = invocationBuilder.setTryCount(100).setTryPauseMillis(100).invoke();
        try {
            return (Boolean) future.get(1, TimeUnit.MINUTES);
        } catch (InterruptedException ie) {
            Logger.getLogger(InternalPartitionServiceImpl.class).finest("Future wait interrupted", ie);
        } catch (Exception e) {
            logger.log(level, "Could not get a response from master about migrations! -> " + e.toString());
        }
        return false;
    }

    @Override
    public boolean hasOnGoingMigrationLocal() {
        return activeMigrationInfo != null || migrationQueue.isNonEmpty()
                || migrationQueue.hasMigrationTasks();
    }

    private boolean isReplicaInSyncState() {
        if (!partitionStateManager.isInitialized() || !hasMultipleMemberGroups()) {
            return true;
        }
        final int replicaIndex = 1;
        final List<Future> futures = new ArrayList<Future>();
        final Address thisAddress = node.getThisAddress();
        for (InternalPartition partition : partitionStateManager.getPartitions()) {
            final Address owner = partition.getOwnerOrNull();
            if (thisAddress.equals(owner)) {
                if (partition.getReplicaAddress(replicaIndex) != null) {
                    final int partitionId = partition.getPartitionId();
                    final long replicaVersion = getCurrentReplicaVersion(replicaIndex, partitionId);
                    final Operation operation = createReplicaSyncStateOperation(replicaVersion, partitionId);
                    final Future future = invoke(operation, replicaIndex, partitionId);
                    futures.add(future);
                }
            }
        }
        if (futures.isEmpty()) {
            return true;
        }
        for (Future future : futures) {
            boolean isSync = getFutureResult(future, REPLICA_SYNC_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!isSync) {
                return false;
            }
        }
        return true;
    }

    private long getCurrentReplicaVersion(int replicaIndex, int partitionId) {
        final long[] versions = getPartitionReplicaVersions(partitionId);
        return versions[replicaIndex - 1];
    }

    private boolean getFutureResult(Future future, long seconds, TimeUnit unit) {
        boolean sync;
        try {
            sync = (Boolean) future.get(seconds, unit);
        } catch (Throwable t) {
            sync = false;
            logger.warning("Exception while getting future", t);
        }
        return sync;
    }

    private Future invoke(Operation operation, int replicaIndex, int partitionId) {
        final OperationService operationService = nodeEngine.getOperationService();
        return operationService.createInvocationBuilder(InternalPartitionService.SERVICE_NAME, operation, partitionId)
                .setTryCount(3)
                .setTryPauseMillis(250)
                .setReplicaIndex(replicaIndex)
                .invoke();
    }

    private Operation createReplicaSyncStateOperation(long replicaVersion, int partitionId) {
        final Operation op = new IsReplicaVersionSync(replicaVersion);
        op.setService(this);
        op.setNodeEngine(nodeEngine);
        op.setOperationResponseHandler(createErrorLoggingResponseHandler(node.getLogger(IsReplicaVersionSync.class)));
        op.setPartitionId(partitionId);

        return op;
    }

    private boolean checkReplicaSyncState() {
        if (!partitionStateManager.isInitialized()) {
            return true;
        }

        if (!hasMultipleMemberGroups()) {
            return true;
        }

        final Address thisAddress = node.getThisAddress();
        final Semaphore s = new Semaphore(0);
        final AtomicBoolean ok = new AtomicBoolean(true);
        final ExecutionCallback<Object> callback = new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                if (Boolean.FALSE.equals(response)) {
                    ok.compareAndSet(true, false);
                }
                s.release();
            }

            @Override
            public void onFailure(Throwable t) {
                ok.compareAndSet(true, false);
            }
        };
        int ownedCount = submitSyncReplicaOperations(thisAddress, s, ok, callback);
        try {
            if (ok.get()) {
                int permits = ownedCount * getMaxAllowedBackupCount();
                return s.tryAcquire(permits, REPLICA_SYNC_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS) && ok.get();
            } else {
                return false;
            }
        } catch (InterruptedException ignored) {
            return false;
        }
    }

    private int submitSyncReplicaOperations(Address thisAddress, Semaphore s, AtomicBoolean ok,
                                            ExecutionCallback callback) {

        int ownedCount = 0;
        ILogger responseLogger = node.getLogger(SyncReplicaVersion.class);
        OperationResponseHandler responseHandler =
                createErrorLoggingResponseHandler(responseLogger);

        int maxBackupCount = getMaxAllowedBackupCount();

        for (InternalPartition partition : partitionStateManager.getPartitions()) {
            Address owner = partition.getOwnerOrNull();
            if (thisAddress.equals(owner)) {
                for (int i = 1; i <= maxBackupCount; i++) {
                    final Address replicaAddress = partition.getReplicaAddress(i);
                    if (replicaAddress != null) {
                        if (checkClusterStateForReplicaSync(replicaAddress)) {
                            SyncReplicaVersion op = new SyncReplicaVersion(i, callback);
                            op.setService(this);
                            op.setNodeEngine(nodeEngine);
                            op.setOperationResponseHandler(responseHandler);
                            op.setPartitionId(partition.getPartitionId());
                            nodeEngine.getOperationService().executeOperation(op);
                        } else {
                            s.release();
                        }
                    } else {
                        ok.set(false);
                        s.release();
                    }
                }
                ownedCount++;
            } else if (owner == null) {
                ok.set(false);
            }
        }
        return ownedCount;
    }

    private boolean checkClusterStateForReplicaSync(final Address address) {
        final ClusterServiceImpl clusterService = node.clusterService;
        final ClusterState clusterState = clusterService.getClusterState();

        if (clusterState == ClusterState.ACTIVE || clusterState == ClusterState.IN_TRANSITION) {
            return true;
        }

        return !clusterService.isMemberRemovedWhileClusterIsNotActive(address);
    }

    private boolean shouldWaitMigrationOrBackups(Level level) {
        if (!preCheckShouldWaitMigrationOrBackups()) {
            return false;
        }

        if (checkForActiveMigrations(level)) {
            return true;
        }

        for (InternalPartition partition : partitionStateManager.getPartitions()) {
            if (partition.getReplicaAddress(1) == null) {
                final boolean canTakeBackup = !isClusterFormedByOnlyLiteMembers();

                if (canTakeBackup && logger.isLoggable(level)) {
                    logger.log(level, "Should take backup of partitionId=" + partition.getPartitionId());
                }

                return canTakeBackup;
            }
        }
        int replicaSyncProcesses = replicaManager.onGoingReplicationProcesses();
        if (replicaSyncProcesses > 0) {
            if (logger.isLoggable(level)) {
                logger.log(level, "Processing replica sync requests: " + replicaSyncProcesses);
            }
            return true;
        }
        return false;
    }

    private boolean preCheckShouldWaitMigrationOrBackups() {
        if (!partitionStateManager.isInitialized()) {
            return false;
        }

        return hasMultipleMemberGroups();
    }

    private boolean hasMultipleMemberGroups() {
        return getMemberGroupsSize() >= 2;
    }

    private boolean checkForActiveMigrations(Level level) {
        final MigrationInfo activeMigrationInfo = this.activeMigrationInfo;
        if (activeMigrationInfo != null) {
            if (logger.isLoggable(level)) {
                logger.log(level, "Waiting for active migration: " + activeMigrationInfo);
            }
            return true;
        }

        int queueSize = migrationQueue.size();
        if (queueSize != 0) {
            if (logger.isLoggable(level)) {
                logger.log(level, "Waiting for cluster migration tasks: " + queueSize);
            }
            return true;
        }
        return false;
    }

    @Override
    public final int getPartitionId(Data key) {
        return HashUtil.hashToIndex(key.getPartitionHash(), partitionCount);
    }

    @Override
    public final int getPartitionId(Object key) {
        return getPartitionId(nodeEngine.toData(key));
    }

    @Override
    public final int getPartitionCount() {
        return partitionCount;
    }

    public long getPartitionMigrationTimeout() {
        return partitionMigrationTimeout;
    }

    // called in operation threads
    // Caution: Returning version array without copying for performance reasons. Callers must not modify this array!
    @Override
    public long[] incrementPartitionReplicaVersions(int partitionId, int backupCount) {
        return replicaManager.incrementPartitionReplicaVersions(partitionId, backupCount);
    }

    // called in operation threads
    @Override
    public void updatePartitionReplicaVersions(int partitionId, long[] versions, int replicaIndex) {
        replicaManager.updatePartitionReplicaVersions(partitionId, versions, replicaIndex);
    }

    // called in operation threads
    // Caution: Returning version array without copying for performance reasons. Callers must not modify this array!
    @Override
    public long[] getPartitionReplicaVersions(int partitionId) {
        return replicaManager.getPartitionReplicaVersions(partitionId);
    }

    // called in operation threads
    @Override
    public void setPartitionReplicaVersions(int partitionId, long[] versions, int replicaOffset) {
        replicaManager.setPartitionReplicaVersions(partitionId, versions, replicaOffset);
    }

    @Override
    public void clearPartitionReplicaVersions(int partitionId) {
        replicaManager.clearPartitionReplicaVersions(partitionId);
    }

    public PartitionReplicaManager getReplicaManager() {
        return replicaManager;
    }

    @Override
    public Map<Address, List<Integer>> getMemberPartitionsMap() {
        final Collection<Member> dataMembers = node.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        final int dataMembersSize = dataMembers.size();
        Map<Address, List<Integer>> memberPartitions = new HashMap<Address, List<Integer>>(dataMembersSize);
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            final Address owner = getPartitionOwnerOrWait(partitionId);

            List<Integer> ownedPartitions = memberPartitions.get(owner);
            if (ownedPartitions == null) {
                ownedPartitions = new ArrayList<Integer>();
                memberPartitions.put(owner, ownedPartitions);
            }
            ownedPartitions.add(partitionId);
        }
        return memberPartitions;
    }

    @Override
    public List<Integer> getMemberPartitions(Address target) {
        List<Integer> ownedPartitions = new LinkedList<Integer>();
        for (int i = 0; i < partitionCount; i++) {
            final Address owner = getPartitionOwner(i);
            if (target.equals(owner)) {
                ownedPartitions.add(i);
            }
        }
        return ownedPartitions;
    }

    @Override
    public void reset() {
        migrationQueue.clear();
        replicaManager.reset();

        lock.lock();
        try {

            partitionStateManager.reset();

            // TODO BASRI IS THIS SAFE?
            activeMigrationInfo = null;
            completedMigrations.clear();

        } finally {
            lock.unlock();
        }
    }

    @Override
    public void pauseMigration() {
        migrationAllowed.set(false);
    }

    @Override
    public void resumeMigration() {
        migrationAllowed.set(true);
    }

    public boolean isReplicaSyncAllowed() {
        return migrationAllowed.get();
    }

    public boolean isMigrationAllowed() {
        if (migrationAllowed.get()) {
            ClusterState clusterState = node.getClusterService().getClusterState();
            return clusterState == ClusterState.ACTIVE;
        }

        return false;
    }

    @Override
    public void shutdown(boolean terminate) {
        logger.finest("Shutting down the partition service");
        migrationThread.stopNow();
        reset();
    }

    @Override
    @Probe(name = "migrationQueueSize")
    public long getMigrationQueueSize() {
        return migrationQueue.size();
    }

    public PartitionServiceProxy getPartitionServiceProxy() {
        return proxy;
    }

    void sendMigrationEvent(final MigrationInfo migrationInfo, final MigrationStatus status) {
        MemberImpl current = getMember(migrationInfo.getSource());
        MemberImpl newOwner = getMember(migrationInfo.getDestination());
        MigrationEvent event = new MigrationEvent(migrationInfo.getPartitionId(), current, newOwner, status);
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, MIGRATION_EVENT_TOPIC);
        eventService.publishEvent(SERVICE_NAME, registrations, event, event.getPartitionId());
    }

    @Override
    public String addMigrationListener(MigrationListener listener) {
        if (listener == null) {
            throw new NullPointerException("listener can't be null");
        }

        final MigrationListenerAdapter adapter = new MigrationListenerAdapter(listener);

        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = eventService.registerListener(SERVICE_NAME, MIGRATION_EVENT_TOPIC, adapter);
        return registration.getId();
    }

    @Override
    public boolean removeMigrationListener(String registrationId) {
        if (registrationId == null) {
            throw new NullPointerException("registrationId can't be null");
        }

        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(SERVICE_NAME, MIGRATION_EVENT_TOPIC, registrationId);
    }

    @Override
    public String addPartitionLostListener(PartitionLostListener listener) {
        if (listener == null) {
            throw new NullPointerException("listener can't be null");
        }

        final PartitionLostListenerAdapter adapter = new PartitionLostListenerAdapter(listener);

        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = eventService.registerListener(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, adapter);
        return registration.getId();
    }

    @Override
    public String addLocalPartitionLostListener(PartitionLostListener listener) {
        if (listener == null) {
            throw new NullPointerException("listener can't be null");
        }

        final PartitionLostListenerAdapter adapter = new PartitionLostListenerAdapter(listener);

        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration =
                eventService.registerLocalListener(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, adapter);
        return registration.getId();
    }

    @Override
    public boolean removePartitionLostListener(String registrationId) {
        if (registrationId == null) {
            throw new NullPointerException("registrationId can't be null");
        }

        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, registrationId);
    }

    @Override
    public void dispatchEvent(PartitionEvent partitionEvent, PartitionEventListener partitionEventListener) {
        partitionEventListener.onEvent(partitionEvent);
    }

    public void addPartitionListener(PartitionListener listener) {
        lock.lock();
        try {
            PartitionListenerNode head = partitionListener.listenerHead;
            partitionListener.listenerHead = new PartitionListenerNode(listener, head);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return "PartitionManager[" + getPartitionStateVersion() + "] {\n\n" + "migrationQ: " + migrationQueue.size() + "\n}";
    }

    public Node getNode() {
        return node;
    }

    @Override
    public boolean isPartitionOwner(int partitionId) {
        InternalPartition partition = partitionStateManager.getPartitionImpl(partitionId);
        return partition.isLocal();
    }

    @Override
    public int getPartitionStateVersion() {
        return partitionStateManager.getVersion();
    }

    @Override
    public void onPartitionLost(IPartitionLostEvent event) {
        final PartitionLostEvent partitionLostEvent = new PartitionLostEvent(event.getPartitionId(), event.getLostReplicaIndex(),
                event.getEventSource());
        final EventService eventService = nodeEngine.getEventService();
        final Collection<EventRegistration> registrations = eventService
                .getRegistrations(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC);
        eventService.publishEvent(SERVICE_NAME, registrations, partitionLostEvent, event.getPartitionId());
    }

    public void setInternalMigrationListener(InternalMigrationListener listener) {
        Preconditions.checkNotNull(listener);
        internalMigrationListener = listener;
    }

    public void resetInternalMigrationListener() {
        internalMigrationListener = new InternalMigrationListener.NopInternalMigrationListener();
    }

    public InternalMigrationListener getInternalMigrationListener() {
        return internalMigrationListener;
    }

    /**
     * @return copy of ongoing replica-sync operations
     */
    public List<ReplicaSyncInfo> getOngoingReplicaSyncRequests() {
        return replicaManager.getOngoingReplicaSyncRequests();
    }

    /**
     * @return copy of scheduled replica-sync requests
     */
    public List<ScheduledEntry<Integer, ReplicaSyncInfo>> getScheduledReplicaSyncRequests() {
        return replicaManager.getScheduledReplicaSyncRequests();
    }

    private class SendPartitionRuntimeStateTask
            implements Runnable {
        @Override
        public void run() {
            if (node.isMaster() && node.getState() == NodeState.ACTIVE) {
                if (migrationQueue.isNonEmpty() && isMigrationAllowed()) {
                    logger.info("Remaining migration tasks in queue => " + migrationQueue.size());
                }
                publishPartitionRuntimeState();
            }
        }
    }

    private class SyncReplicaVersionTask implements Runnable {
        @Override
        public void run() {
            if (node.nodeEngine.isRunning() && isReplicaSyncAllowed()) {
                for (InternalPartition partition : partitionStateManager.getPartitions()) {
                    if (partition.isLocal()) {
                        for (int index = 1; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
                            if (partition.getReplicaAddress(index) != null) {
                                SyncReplicaVersion op = new SyncReplicaVersion(index, null);
                                op.setService(InternalPartitionServiceImpl.this);
                                op.setNodeEngine(nodeEngine);
                                op.setOperationResponseHandler(
                                        createErrorLoggingResponseHandler(node.getLogger(SyncReplicaVersion.class)));
                                op.setPartitionId(partition.getPartitionId());
                                nodeEngine.getOperationService().executeOperation(op);
                            }
                        }
                    }
                }
            }
        }
    }

    private class RepartitioningTask implements Runnable {
        @Override
        public void run() {
            if (!node.isMaster()) {
                return;
            }

            lock.lock();
            try {
                if (!partitionStateManager.isInitialized()) {
                    return;
                }
                if (!isMigrationAllowed()) {
                    return;
                }

                migrationQueue.clear();
//                Collection<MemberGroup> memberGroups = createMemberGroups();
//                Address[][] newState = psg.reArrange(memberGroups, partitions);

                Address[][] newState = null;
                if (newState == null) {
                    return;
                }

                if (!isMigrationAllowed()) {
                    return;
                }

                processNewPartitionState(newState);
                syncPartitionRuntimeState();
            } finally {
                lock.unlock();
            }
        }

        private void processNewPartitionState(Address[][] newState) {
            int migrationCount = 0;
            int lostCount = 0;
            lastRepartitionTime.set(Clock.currentTimeMillis());
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                Address[] replicas = newState[partitionId];
                InternalPartitionImpl currentPartition = partitionStateManager.getPartitionImpl(partitionId);
                Address currentOwner = currentPartition.getOwnerOrNull();
                Address newOwner = replicas[0];

                if (currentOwner == null) {
                    // assign new owner for lost partition
                    lostCount++;
                    assignNewPartitionOwner(partitionId, replicas, currentPartition, newOwner);
                } else if (newOwner != null && !currentOwner.equals(newOwner)) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("PartitionToMigrate partitionId=" + partitionId
                                + " replicas=" + Arrays.toString(replicas) + " currentOwner="
                                + currentOwner + " newOwner=" + newOwner);
                    }

                    migrationCount++;
                    migratePartitionToNewOwner(partitionId, replicas, currentOwner, newOwner);
                } else {
                    currentPartition.setReplicaAddresses(replicas);
                }
            }
            logMigrationStatistics(migrationCount, lostCount);
        }

        private void logMigrationStatistics(int migrationCount, int lostCount) {
            if (lostCount > 0) {
                logger.warning("Assigning new owners for " + lostCount + " LOST partitions!");
            }

            if (migrationCount > 0) {
                logger.info("Re-partitioning cluster data... Migration queue size: " + migrationCount);
            } else {
                logger.info("Partition balance is ok, no need to re-partition cluster data... ");
            }
        }

        private void migratePartitionToNewOwner(int partitionId, Address[] replicas, Address currentOwner, Address newOwner) {
            MigrationInfo info = new MigrationInfo(partitionId, currentOwner, newOwner);
            MigrateTask migrateTask = new MigrateTask(info, replicas);
            migrationQueue.add(migrateTask);
        }

        private void assignNewPartitionOwner(int partitionId, Address[] replicas, InternalPartitionImpl currentPartition,
                                             Address newOwner) {
            currentPartition.setReplicaAddresses(replicas);
            MigrationInfo migrationInfo = new MigrationInfo(partitionId, null, newOwner);
            sendMigrationEvent(migrationInfo, MigrationStatus.STARTED);
            sendMigrationEvent(migrationInfo, MigrationStatus.COMPLETED);
        }

        private boolean isMigrationAllowed() {
            if (InternalPartitionServiceImpl.this.isMigrationAllowed()) {
                return true;
            }
            migrationQueue.add(this);
            return false;
        }
    }

    class MigrateTask implements Runnable {
        final MigrationInfo migrationInfo;
        final Address[] addresses;

        public MigrateTask(MigrationInfo migrationInfo, Address[] addresses) {
            this.migrationInfo = migrationInfo;
            this.addresses = addresses;
            final MemberImpl masterMember = getMasterMember();
            if (masterMember != null) {
                migrationInfo.setMasterUuid(masterMember.getUuid());
                migrationInfo.setMaster(masterMember.getAddress());
            }
        }

        @Override
        public void run() {
            if (!node.isMaster()) {
                return;
            }
            final MigrationRequestOperation migrationRequestOp = new MigrationRequestOperation(migrationInfo);
            try {
                MigrationInfo info = migrationInfo;
                InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(info.getPartitionId());
                Address owner = partition.getOwnerOrNull();
                if (owner == null) {
                    logger.severe("ERROR: partition owner is not set! -> partitionId=" + info.getPartitionId()
                            + " , " + partition + " -VS- " + info);
                    return;
                }
                if (!owner.equals(info.getSource())) {
                    logger.severe("ERROR: partition owner is not the source of migration! -> partitionId="
                            + info.getPartitionId() + " , " + partition + " -VS- " + info + " found owner=" + owner);
                    return;
                }
                internalMigrationListener.onMigrationStart(MigrationParticipant.MASTER, migrationInfo);
                sendMigrationEvent(migrationInfo, MigrationStatus.STARTED);

                Boolean result;
                MemberImpl fromMember = getMember(migrationInfo.getSource());
                if (logger.isFinestEnabled()) {
                    logger.finest("Starting Migration: " + migrationInfo);
                }
                if (fromMember == null) {
                    // Partition is lost! Assign new owner and exit.
                    logger.warning("Partition is lost! Assign new owner and exit... partitionId=" + info.getPartitionId());
                    result = Boolean.TRUE;
                    // TODO BASRI UNDERSTAND HOW THIS PART WILL WORK. THIS CASE SHOULD CAN BE TESTED WITH OUR MIGRATION LISTENER
                } else {
                    result = executeMigrateOperation(migrationRequestOp, fromMember);
                }
                processMigrationResult(result);
            } catch (Throwable t) {
                final Level level = migrationEndpointsActive() ? Level.WARNING : Level.FINEST;
                logger.log(level, "Error [" + t.getClass() + ": " + t.getMessage() + "] while executing " + migrationRequestOp);
                logger.finest(t);
                migrationOperationFailed();
            }
        }

        private void processMigrationResult(Boolean result) {
            if (Boolean.TRUE.equals(result)) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Finished Migration: " + migrationInfo);
                }
                migrationOperationSucceeded();
            } else {
                final Level level = migrationEndpointsActive() ? Level.WARNING : Level.FINEST;
                logger.log(level, "Migration failed: " + migrationInfo);
                migrationOperationFailed();
            }
        }

        private Boolean executeMigrateOperation(MigrationRequestOperation migrationRequestOp, MemberImpl fromMember) {
            Future future = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, migrationRequestOp,
                    migrationInfo.getSource())
                    .setCallTimeout(partitionMigrationTimeout)
                    .setTryPauseMillis(DEFAULT_PAUSE_MILLIS).invoke();

            try {
                Object response = future.get();
                return (Boolean) nodeEngine.toObject(response);
            } catch (Throwable e) {
                final Level level = nodeEngine.isRunning() && migrationEndpointsActive() ? Level.WARNING : Level.FINEST;
                logger.log(level, "Failed migration from " + fromMember + " for " + migrationRequestOp.getMigrationInfo(), e);
            }
            return Boolean.FALSE;
        }

        private void migrationOperationFailed() {
            internalMigrationListener.onMigrationComplete(MigrationParticipant.MASTER, migrationInfo, false);
            lock.lock();
            try {
                addCompletedMigration(migrationInfo);
                internalMigrationListener.onMigrationRollback(MigrationParticipant.MASTER, migrationInfo);
                scheduleActiveMigrationFinalization(migrationInfo);
                syncPartitionRuntimeState();
            } finally {
                lock.unlock();
            }
            sendMigrationEvent(migrationInfo, MigrationStatus.FAILED);

            // Migration failed.
            // Pause migration process for a small amount of time, if a migration attempt is failed.
            // Otherwise, migration failures can do a busy spin until migration problem is resolved.
            // Migration can fail either a node's just joined and not completed start yet or it's just left the cluster.
            pauseMigration();
            // Re-execute RepartitioningTask when all other migration tasks are done,
            // an imbalance may occur because of this failure.
            migrationQueue.add(new RepartitioningTask());
            resumeMigrationEventually();
        }

        private void migrationOperationSucceeded() {
            internalMigrationListener.onMigrationComplete(MigrationParticipant.MASTER, migrationInfo, true);
            addCompletedMigration(migrationInfo);

            boolean commitSuccessful = commitMigrationToDestination(migrationInfo, addresses);

            lock.lock();
            try {
                if (commitSuccessful) {
                    partitionStateManager.updateReplicaAddresses(migrationInfo.getPartitionId(), addresses);
                    internalMigrationListener.onMigrationCommit(MigrationParticipant.MASTER, migrationInfo);
                } else {
                    internalMigrationListener.onMigrationRollback(MigrationParticipant.MASTER, migrationInfo);
                }

                scheduleActiveMigrationFinalization(migrationInfo);
                syncPartitionRuntimeState();
            } finally {
                lock.unlock();
            }
            sendMigrationEvent(migrationInfo, MigrationStatus.COMPLETED);
        }

        private boolean migrationEndpointsActive() {
            return getMember(migrationInfo.getSource()) != null
                    && getMember(migrationInfo.getDestination()) != null;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" + "migrationInfo=" + migrationInfo + '}';
        }
    }

    private class MigrationThread extends Thread implements Runnable {
        private final long sleepTime = max(250L, partitionMigrationInterval);

        MigrationThread(Node node) {
            super(node.getHazelcastThreadGroup().getInternalThreadGroup(),
                    node.getHazelcastThreadGroup().getThreadNamePrefix("migration"));
        }

        @Override
        public void run() {
            try {
                while (!isInterrupted()) {
                    doRun();
                }
            } catch (InterruptedException e) {
                if (logger.isFinestEnabled()) {
                    logger.finest("MigrationThread is interrupted: " + e.getMessage());
                }
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
            } finally {
                migrationQueue.clear();
            }
        }

        private void doRun() throws InterruptedException {
            boolean migrating = false;
            for (; ;) {
                if (!isMigrationAllowed()) {
                    break;
                }
                Runnable r = migrationQueue.poll(1, TimeUnit.SECONDS);
                if (r == null) {
                    break;
                }

                migrating |= r instanceof MigrateTask;
                processTask(r);
                if (partitionMigrationInterval > 0) {
                    Thread.sleep(partitionMigrationInterval);
                }
            }
            boolean hasNoTasks = !migrationQueue.hasMigrationTasks();
            if (hasNoTasks) {
                if (migrating) {
                    logger.info("All migration tasks have been completed, queues are empty.");
                }
                evictCompletedMigrations();
                Thread.sleep(sleepTime);
            } else if (!isMigrationAllowed()) {
                Thread.sleep(sleepTime);
            }
        }

        boolean processTask(Runnable r) {
            try {
                if (r == null || isInterrupted()) {
                    return false;
                }

                r.run();
            } catch (Throwable t) {
                logger.warning(t);
            } finally {
                migrationQueue.afterTaskCompletion(r);
            }

            return true;
        }

        void stopNow() {
            migrationQueue.clear();
            interrupt();
        }

    }

    private static final class InternalPartitionListener implements PartitionListener {
        final Address thisAddress;
        final InternalPartitionServiceImpl partitionService;
        volatile PartitionListenerNode listenerHead;

        private InternalPartitionListener(InternalPartitionServiceImpl partitionService, Address thisAddress) {
            this.thisAddress = thisAddress;
            this.partitionService = partitionService;
        }

        @Override
        public void replicaChanged(PartitionReplicaChangeEvent event) {
            final int partitionId = event.getPartitionId();
            final int replicaIndex = event.getReplicaIndex();
            final Address newAddress = event.getNewAddress();
            final Address oldAddress = event.getOldAddress();
            final PartitionReplicaChangeReason reason = event.getReason();

            final boolean initialAssignment = event.getOldAddress() == null;

            if (replicaIndex > 0) {
                // backup replica owner changed!
                if (thisAddress.equals(oldAddress)) {
                    clearPartition(partitionId, replicaIndex);
                } else if (thisAddress.equals(newAddress)) {
                    synchronizePartition(partitionId, replicaIndex, reason, initialAssignment);
                }
            } else {
                if (!initialAssignment && thisAddress.equals(newAddress)) {
                    // it is possible that I might become owner while waiting for sync request from the previous owner.
                    // I should check whether if have failed to get backups from the owner and lost the partition for
                    // some backups.
                    promoteFromBackups(partitionId, reason, oldAddress);
                }
                partitionService.replicaManager.cancelReplicaSync(partitionId);
            }

            Node node = partitionService.node;
            if (replicaIndex == 0 && newAddress == null && node.isRunning() && node.joined()) {
                logOwnerOfPartitionIsRemoved(event);
            }
            if (node.isMaster()) {
                partitionService.partitionStateManager.incrementVersion();
            }

            callListeners(event);
        }

        private void callListeners(PartitionReplicaChangeEvent event) {
            PartitionListenerNode listenerNode = listenerHead;
            while (listenerNode != null) {
                try {
                    listenerNode.listener.replicaChanged(event);
                } catch (Throwable e) {
                    partitionService.logger.warning("While calling PartitionListener: " + listenerNode.listener, e);
                }
                listenerNode = listenerNode.next;
            }
        }

        private void clearPartition(final int partitionId, final int oldReplicaIndex) {
            NodeEngine nodeEngine = partitionService.nodeEngine;
            ClearReplicaOperation op = new ClearReplicaOperation(oldReplicaIndex);
            op.setPartitionId(partitionId).setNodeEngine(nodeEngine).setService(partitionService);
            nodeEngine.getOperationService().executeOperation(op);
        }

        private void synchronizePartition(int partitionId, int replicaIndex,
                                          PartitionReplicaChangeReason reason, boolean initialAssignment) {
            // if not initialized yet, no need to sync, since this is the initial partition assignment
            if (partitionService.partitionStateManager.isInitialized()) {
                long delayMillis = 0L;
                if (replicaIndex > 1) {
                    // immediately trigger replica synchronization for the first backups
                    // postpone replica synchronization for greater backups to a later time
                    // high priority is 1st backups
                    delayMillis = (long) (REPLICA_SYNC_RETRY_DELAY + (Math.random() * DEFAULT_REPLICA_SYNC_DELAY));
                }

                resetReplicaVersion(partitionId, replicaIndex, reason, initialAssignment);
                partitionService.replicaManager.triggerPartitionReplicaSync(partitionId, replicaIndex, delayMillis);
            }
        }

        private void resetReplicaVersion(int partitionId, int replicaIndex,
                                         PartitionReplicaChangeReason reason, boolean initialAssignment) {
            NodeEngine nodeEngine = partitionService.nodeEngine;
            ResetReplicaVersionOperation op = new ResetReplicaVersionOperation(reason, initialAssignment);
            op.setPartitionId(partitionId).setReplicaIndex(replicaIndex)
                    .setNodeEngine(nodeEngine).setService(partitionService);
            nodeEngine.getOperationService().executeOperation(op);
        }

        private void promoteFromBackups(int partitionId, PartitionReplicaChangeReason reason, Address oldAddress) {
            NodeEngine nodeEngine = partitionService.nodeEngine;
            PromoteFromBackupOperation op = new PromoteFromBackupOperation(reason, oldAddress);
            op.setPartitionId(partitionId).setNodeEngine(nodeEngine).setService(partitionService);
            nodeEngine.getOperationService().executeOperation(op);
        }

        private void logOwnerOfPartitionIsRemoved(PartitionReplicaChangeEvent event) {
            String warning = "Owner of partition is being removed! "
                    + "Possible data loss for partitionId=" + event.getPartitionId() + " , " + event;
            partitionService.logger.warning(warning);
        }
    }

    private static final class PartitionListenerNode {
        final PartitionListener listener;
        final PartitionListenerNode next;

        PartitionListenerNode(PartitionListener listener, PartitionListenerNode next) {
            this.listener = listener;
            this.next = next;
        }
    }

    // TODO: need a better task name!
    private class FixPartitionTableTask implements Runnable {
        private final Address thisAddress = node.getThisAddress();

        public void run() {
            Collection<MemberImpl> members = node.clusterService.getMemberImpls();
            Collection<Future<PartitionRuntimeState>> futures = new ArrayList<Future<PartitionRuntimeState>>(
                    members.size());

            for (MemberImpl m : members) {
                if (m.localMember()) {
                    continue;
                }
                Future<PartitionRuntimeState> future = nodeEngine.getOperationService()
                        .createInvocationBuilder(SERVICE_NAME, new FetchPartitionStateOperation(),
                                m.getAddress()).setTryCount(Integer.MAX_VALUE)
                        .setCallTimeout(Long.MAX_VALUE).invoke();
                futures.add(future);
            }

            int version = getPartitionStateVersion();
            PartitionRuntimeState newState = null;

            Collection<MigrationInfo> activeMigrations = new ArrayList<MigrationInfo>();
            Collection<MigrationInfo> allCompletedMigrations = new HashSet<MigrationInfo>();

            for (Future<PartitionRuntimeState> future : futures) {
                try {
                    PartitionRuntimeState state = future.get();
                    if (version < state.getVersion()) {
                        newState = state;
                        version = state.getVersion();
                    }
                    allCompletedMigrations.addAll(state.getCompletedMigrations());

                    if (state.getActiveMigration() != null) {
                        activeMigrations.add(state.getActiveMigration());
                    }
                } catch (MemberLeftException e) {
                    // ignore
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }

            if (newState != null) {
                applyNewState(newState, thisAddress);
            }

            lock.lock();
            try {
                // TODO: merge completed migrations
                allCompletedMigrations.addAll(completedMigrations);
                completedMigrations.clear();
                completedMigrations.addAll(allCompletedMigrations);

                // TODO: or should we ask all members about status of an ongoing migration?
                // TODO: handle & rollback active migrations started but not completed yet
//                        rollbackActiveMigrationsFromPreviousMaster(node.getLocalMember().getUuid());

            } finally {
                lock.unlock();
            }
        }
    }
}
