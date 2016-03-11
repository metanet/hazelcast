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

package com.hazelcast.internal.partition;

import com.hazelcast.core.Member;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.core.Partition;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.internal.partition.operation.SafeStateCheckOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class PartitionServiceProxy implements com.hazelcast.core.PartitionService {

    private final NodeEngine nodeEngine;
    private final InternalPartitionService partitionService;
    private final ConcurrentMap<Integer, PartitionProxy> mapPartitions
            = new ConcurrentHashMap<Integer, PartitionProxy>();
    private final Set<Partition> partitions = new TreeSet<Partition>();
    private final Random random = new Random();
    private final ILogger logger;

    public PartitionServiceProxy(NodeEngine nodeEngine, InternalPartitionService partitionService) {
        this.nodeEngine = nodeEngine;
        this.partitionService = partitionService;
        for (int i = 0; i < partitionService.getPartitionCount(); i++) {
            PartitionProxy partitionProxy = new PartitionProxy(i);
            partitions.add(partitionProxy);
            mapPartitions.put(i, partitionProxy);
        }
        logger = nodeEngine.getLogger(PartitionServiceProxy.class);
    }

    @Override
    public String randomPartitionKey() {
        return Integer.toString(random.nextInt(partitionService.getPartitionCount()));
    }

    @Override
    public Set<Partition> getPartitions() {
        return partitions;
    }

    @Override
    public PartitionProxy getPartition(Object key) {
        int partitionId = partitionService.getPartitionId(key);
        return getPartition(partitionId);
    }

    @Override
    public String addMigrationListener(final MigrationListener migrationListener) {
        return partitionService.addMigrationListener(migrationListener);
    }

    @Override
    public boolean removeMigrationListener(final String registrationId) {
        return partitionService.removeMigrationListener(registrationId);
    }

    @Override
    public String addPartitionLostListener(PartitionLostListener partitionLostListener) {
        return partitionService.addPartitionLostListener(partitionLostListener);
    }

    @Override
    public boolean removePartitionLostListener(String registrationId) {
        return partitionService.removePartitionLostListener(registrationId);
    }

    @Override
    public boolean isClusterSafe() {
        final Collection<Member> memberList = nodeEngine.getClusterService().getMembers();
        if (memberList == null || memberList.isEmpty() || memberList.size() < 2) {
            return true;
        }
        final Collection<Future> futures = new ArrayList<Future>(memberList.size());
        for (Member member : memberList) {
            final Address target = member.getAddress();
            final Operation operation = new SafeStateCheckOperation();
            final InternalCompletableFuture future = nodeEngine.getOperationService()
                    .invokeOnTarget(InternalPartitionService.SERVICE_NAME, operation, target);
            futures.add(future);
        }
        // todo this max wait is appropriate?
        final int maxWaitTime = getMaxWaitTime();
        for (Future future : futures) {
            try {
                final Object result = future.get(maxWaitTime, TimeUnit.SECONDS);
                final boolean safe = (Boolean) result;
                if (!safe) {
                    return false;
                }
            } catch (Exception e) {
                logger.warning("Error while querying cluster's safe state", e);
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isMemberSafe(Member member) {
        if (member == null) {
            throw new NullPointerException("Parameter member should not be null");
        }
        final Member localMember = nodeEngine.getLocalMember();
        if (localMember.equals(member)) {
            return isLocalMemberSafe();
        }
        final Address target = member.getAddress();
        final Operation operation = new SafeStateCheckOperation();
        final InternalCompletableFuture future = nodeEngine.getOperationService()
                .invokeOnTarget(InternalPartitionService.SERVICE_NAME, operation, target);
        boolean safe;
        try {
            final Object result = future.get(10, TimeUnit.SECONDS);
            safe = (Boolean) result;
        } catch (Throwable t) {
            safe = false;
            logger.warning("Error while querying member's safe state [" + member + "]", t);
        }
        return safe;
    }

    @Override
    public boolean isLocalMemberSafe() {
        if (!nodeActive()) {
            return true;
        }
        return partitionService.isMemberStateSafe();
    }

    @Override
    public boolean forceLocalMemberToBeSafe(long timeout, TimeUnit unit) {
        if (unit == null) {
            throw new NullPointerException();
        }

        if (timeout < 1L) {
            throw new IllegalArgumentException();
        }

        if (!nodeActive()) {
            return true;
        }
        return partitionService.prepareToSafeShutdown(timeout, unit);
    }

    private boolean nodeActive() {
        return nodeEngine.isRunning();
    }

    private int getMaxWaitTime() {
        return nodeEngine.getGroupProperties().getSeconds(GroupProperty.GRACEFUL_SHUTDOWN_MAX_WAIT);
    }

    public PartitionProxy getPartition(int partitionId) {
        return mapPartitions.get(partitionId);
    }

    public class PartitionProxy implements Partition, Comparable {

        final int partitionId;

        PartitionProxy(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public Member getOwner() {
            // triggers initial partition assignment
            final Address address = partitionService.getPartitionOwner(partitionId);
            if (address == null) {
                return null;
            }

            return nodeEngine.getClusterService().getMember(address);
        }

        @Override
        public int compareTo(Object o) {
            PartitionProxy partition = (PartitionProxy) o;
            Integer id = partitionId;
            return (id.compareTo(partition.getPartitionId()));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartitionProxy partition = (PartitionProxy) o;
            return partitionId == partition.partitionId;
        }

        @Override
        public int hashCode() {
            return partitionId;
        }

        @Override
        public String toString() {
            return "Partition [" + +partitionId + "], owner=" + getOwner();
        }
    }
}
