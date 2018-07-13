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

package com.hazelcast.raft.service.semaphore;

import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.config.raft.RaftSemaphoreConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.blocking.AbstractBlockingService;
import com.hazelcast.raft.service.semaphore.proxy.RaftSessionAwareSemaphoreProxy;
import com.hazelcast.raft.service.semaphore.proxy.RaftSessionlessSemaphoreProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
public class RaftSemaphoreService extends AbstractBlockingService<SemaphoreInvocationKey, RaftSemaphore, SemaphoreRegistry> {

    public static final String SERVICE_NAME = "hz:raft:semaphoreService";

    public RaftSemaphoreService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    public ISemaphore createRaftObjectProxy(String name) {
        try {
            RaftGroupId groupId = createRaftGroup(name).get();
            RaftSemaphoreConfig config = getConfig(name);
            SessionManagerService sessionManager = nodeEngine.getService(SessionManagerService.SERVICE_NAME);
            RaftInvocationManager invocationManager = raftService.getInvocationManager();
            return config != null && config.isStrictModeEnabled()
                    ? new RaftSessionAwareSemaphoreProxy(invocationManager, sessionManager, groupId, name)
                    : new RaftSessionlessSemaphoreProxy(invocationManager, groupId, name);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public ICompletableFuture<RaftGroupId> createRaftGroup(String name) {
        String groupRef = getRaftGroupRef(name);
        return raftService.getInvocationManager().createRaftGroup(groupRef);
    }

    private String getRaftGroupRef(String name) {
        RaftSemaphoreConfig config = getConfig(name);
        return config != null ? config.getRaftGroupRef() : RaftGroupConfig.DEFAULT_GROUP;
    }

    private RaftSemaphoreConfig getConfig(String name) {
        return nodeEngine.getConfig().findRaftSemaphoreConfig(name);
    }

    public boolean initSemaphore(RaftGroupId groupId, String name, int permits) {
        return getOrInitRegistry(groupId).init(name, permits);
    }

    public int availablePermits(RaftGroupId groupId, String name) {
        SemaphoreRegistry registry = getRegistryOrNull(groupId);
        return registry != null ? registry.availablePermits(name) : 0;
    }

    public boolean acquirePermits(RaftGroupId groupId, long commitIndex, String name, long sessionId, UUID invocationUid,
                                  int permits, long timeoutMs) {
        heartbeatSession(groupId, sessionId);
        SemaphoreInvocationKey key = new SemaphoreInvocationKey(name, commitIndex, sessionId, invocationUid, permits);
        boolean success = getOrInitRegistry(groupId).acquire(name, key, permits, timeoutMs);
        if (!success) {
            scheduleTimeout(groupId, key, timeoutMs);
        }

        return success;
    }

    public void releasePermits(RaftGroupId groupId, String name, long sessionId, UUID invocationUid, int permits) {
        heartbeatSession(groupId, sessionId);
        Collection<SemaphoreInvocationKey> keys = getOrInitRegistry(groupId).release(name, sessionId, invocationUid, permits);
        notifyWaitKeys(groupId, keys, true);
    }

    public int drainPermits(RaftGroupId groupId, String name, long sessionId, UUID invocationUid) {
        SemaphoreRegistry registry = getRegistryOrNull(groupId);
        return registry != null ? registry.drainPermits(name, sessionId, invocationUid) : 0;
    }

    public boolean changePermits(RaftGroupId groupId, String name, int permits) {
        Tuple2<Boolean, Collection<SemaphoreInvocationKey>> t = getOrInitRegistry(groupId).changePermits(name, permits);
        notifyWaitKeys(groupId, t.element2, true);

        return t.element1;
    }

    @Override
    protected SemaphoreRegistry createNewRegistry(RaftGroupId groupId) {
        return new SemaphoreRegistry(groupId);
    }

    @Override
    protected Object invalidatedWaitKeyResponse() {
        return false;
    }

    @Override
    protected String serviceName() {
        return SERVICE_NAME;
    }

}
