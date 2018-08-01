/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.lock;

import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.config.raft.RaftLockConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.ILock;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.blocking.AbstractBlockingService;
import com.hazelcast.raft.service.lock.proxy.RaftLockProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * TODO: Javadoc Pending...
 */
public class RaftLockService extends AbstractBlockingService<LockInvocationKey, RaftLock, LockRegistry> {

    public static final long INVALID_FENCE = 0L;
    public static final String SERVICE_NAME = "hz:raft:lockService";

    public RaftLockService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    public ILock createRaftObjectProxy(String name) {
        try {
            RaftGroupId groupId = createRaftGroup(name).get();
            SessionManagerService sessionManager = nodeEngine.getService(SessionManagerService.SERVICE_NAME);
            return new RaftLockProxy(raftService.getInvocationManager(), sessionManager, groupId, name);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public ICompletableFuture<RaftGroupId> createRaftGroup(String name) {
        String raftGroupRef = getRaftGroupRef(name);

        RaftInvocationManager invocationManager = raftService.getInvocationManager();
        return invocationManager.createRaftGroup(raftGroupRef);
    }

    private String getRaftGroupRef(String name) {
        RaftLockConfig config = getConfig(name);
        return config != null ? config.getRaftGroupRef() : RaftGroupConfig.DEFAULT_GROUP;
    }

    private RaftLockConfig getConfig(String name) {
        return nodeEngine.getConfig().findRaftLockConfig(name);
    }

    public long acquire(RaftGroupId groupId, String name, LockEndpoint endpoint, long commitIndex, UUID invocationUid) {
        heartbeatSession(groupId, endpoint.sessionId());
        long fence = getOrInitRegistry(groupId).acquire(name, endpoint, commitIndex, invocationUid);
        if (logger.isFineEnabled()) {
            logger.fine("Lock[" + name + "] in " + groupId + " acquired: " + (fence != INVALID_FENCE) + " by <" + endpoint + ", "
                    + invocationUid + ">");
        }
        return fence;
    }

    public long tryAcquire(RaftGroupId groupId, String name, LockEndpoint endpoint, long commitIndex, UUID invocationUid,
                          long timeoutMs) {
        heartbeatSession(groupId, endpoint.sessionId());
        long fence = getOrInitRegistry(groupId).tryAcquire(name, endpoint, commitIndex, invocationUid, timeoutMs);
        if (logger.isFineEnabled()) {
            logger.fine("Lock[" + name + "] in " + groupId + " acquired: " + (fence != INVALID_FENCE) + " by <" + endpoint + ", "
                    + invocationUid + ">");
        }

        if (fence == INVALID_FENCE) {
            scheduleTimeout(groupId, new LockInvocationKey(name, endpoint, commitIndex, invocationUid), timeoutMs);
        }

        return fence;
    }

    public void release(RaftGroupId groupId, String name, LockEndpoint endpoint, UUID invocationUid) {
        heartbeatSession(groupId, endpoint.sessionId());
        LockRegistry registry = getLockRegistryOrFail(groupId, name);
        Collection<LockInvocationKey> waitKeys = registry.release(name, endpoint, invocationUid);

        if (logger.isFineEnabled()) {
            logger.fine("Lock[" + name + "] in " + groupId + " is released by <" + endpoint + ", " + invocationUid + ">");
        }

        notifyLockAcquiredWaitKeys(groupId, name, waitKeys);
    }

    public void forceRelease(RaftGroupId groupId, String name, long expectedFence, UUID invocationUid) {
        LockRegistry registry = getLockRegistryOrFail(groupId, name);
        Collection<LockInvocationKey> waitKeys = registry.forceRelease(name, expectedFence, invocationUid);

        if (logger.isFineEnabled()) {
            logger.fine("Lock[" + name + "] in " + groupId + " is force-released by " + invocationUid + " for fence: "
                    + expectedFence);
        }

        notifyLockAcquiredWaitKeys(groupId, name, waitKeys);
    }

    private void notifyLockAcquiredWaitKeys(RaftGroupId groupId, String name, Collection<LockInvocationKey> waitEntries) {
        if (waitEntries.isEmpty()) {
            return;
        }

        LockInvocationKey waitKey = waitEntries.iterator().next();
        if (logger.isFineEnabled()) {
            logger.fine("Lock[" + name + "] in " + groupId + " is acquired by <" + waitKey.endpoint() + ", "
                    + waitKey.invocationUid() + ">");
        }

        notifyWaitKeys(groupId, waitEntries, waitKey.commitIndex());
    }

    public int getLockCount(RaftGroupId groupId, String name, LockEndpoint endpoint) {
        checkNotNull(groupId);
        checkNotNull(name);

        LockRegistry registry = getRegistryOrNull(groupId);
        return registry != null ? registry.getLockCount(name, endpoint) : 0;
    }

    public long getLockFence(RaftGroupId groupId, String name, LockEndpoint endpoint) {
        checkNotNull(groupId);
        checkNotNull(name);

        return getLockRegistryOrFail(groupId, name).getLockFence(name, endpoint);
    }

    private LockRegistry getLockRegistryOrFail(RaftGroupId groupId, String name) {
        checkNotNull(groupId);
        LockRegistry registry = getRegistryOrNull(groupId);
        if (registry == null) {
            throw new IllegalMonitorStateException("Lock registry of " + groupId + " not found for Lock[" + name + "]");
        }

        return registry;
    }

    @Override
    protected LockRegistry createNewRegistry(RaftGroupId groupId) {
        return new LockRegistry(groupId);
    }

    @Override
    protected Object invalidatedWaitKeyResponse() {
        return INVALID_FENCE;
    }

    @Override
    protected String serviceName() {
        return SERVICE_NAME;
    }
}
