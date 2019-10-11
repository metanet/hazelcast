/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.semaphore.proxy.SessionAwareSemaphoreProxy;
import com.hazelcast.cp.internal.datastructures.semaphore.proxy.SessionlessSemaphoreProxy;
import com.hazelcast.cp.internal.datastructures.spi.blocking.AbstractBlockingService;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.Collection;

import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;
import static com.hazelcast.cp.internal.RaftService.withoutDefaultGroupName;
import static com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult.AcquireStatus.FAILED;
import static com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult.AcquireStatus.SUCCESSFUL;
import static com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult.AcquireStatus.WAIT_KEY_ADDED;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Contains Raft-based semaphore instances
 */
public class SemaphoreService extends AbstractBlockingService<SemaphoreInvocationKey, Semaphore, SemaphoreRegistry> {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:semaphoreService";

    public SemaphoreService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    private SemaphoreConfig getConfig(String name) {
        return nodeEngine.getConfig().getCPSubsystemConfig().findSemaphoreConfig(name);
    }

    public boolean initSemaphore(CPGroupId groupId, String name, int permits) {
        try {
            Collection<SemaphoreInvocationKey> acquired = getOrInitRegistry(groupId).init(name, permits);
            notifyWaitKeys(groupId, name, acquired, true);

            return true;
        } catch (IllegalStateException ignored) {
            return false;
        }
    }

    public int availablePermits(CPGroupId groupId, String name) {
        SemaphoreRegistry registry = getRegistryOrNull(groupId);
        return registry != null ? registry.availablePermits(name) : 0;
    }

    public AcquireResult acquirePermits(CPGroupId groupId, String name, SemaphoreInvocationKey key, long timeoutMs) {
        heartbeatSession(groupId, key.sessionId());
        AcquireResult result = getOrInitRegistry(groupId).acquire(name, key, timeoutMs);

        if (logger.isFineEnabled()) {
            if (result.status() == SUCCESSFUL) {
                logger.fine("Semaphore[" + name + "] in " + groupId + " acquired permits: " + key.permits() + " by <"
                        + key.endpoint() + ", " + key.invocationUid() + "> at commit index: " + key.commitIndex());
            } else if (result.status() == WAIT_KEY_ADDED) {
                logger.fine("Semaphore[" + name + "] in " + groupId + " wait key added for permits: " + key.permits() + " by <"
                        + key.endpoint() + ", " + key.invocationUid() + "> at commit index: " + key.commitIndex());
            } else if (result.status() == FAILED) {
                logger.fine("Semaphore[" + name + "] in " + groupId + " not acquired permits: " + key.permits() + " by <"
                        + key.endpoint() + ", " + key.invocationUid() + "> at commit index: " + key.commitIndex());
            }
        }

        notifyCancelledWaitKey(groupId, name, result.cancelledWaitKey());

        if (result.status() == WAIT_KEY_ADDED) {
            scheduleTimeout(groupId, name, key.invocationUid(), timeoutMs);
        }

        return result;
    }

    public void releasePermits(CPGroupId groupId, String name, SemaphoreInvocationKey key) {
        heartbeatSession(groupId, key.endpoint().sessionId());
        ReleaseResult result = getOrInitRegistry(groupId).release(name, key);
        notifyCancelledWaitKey(groupId, name, result.cancelledWaitKey());
        notifyWaitKeys(groupId, name, result.acquiredWaitKeys(), true);

        if (logger.isFineEnabled()) {
            if (result.success()) {
                logger.fine("Semaphore[" + name + "] in " + groupId + " released permits: " + key.permits() + " by <"
                        + key.endpoint() + ", " + key.invocationUid() + "> at commit index: " + key.commitIndex()
                        + " new acquires: " + result.acquiredWaitKeys());
            } else {
                logger.fine("Semaphore[" + name + "] in " + groupId + " not-released permits: " + key.permits() + " by <" +
                        key.endpoint() + ", " + key.invocationUid() + "> at commit index: " + key.commitIndex());
            }
        }

        if (!result.success()) {
            throw new IllegalArgumentException();
        }
    }

    public int drainPermits(CPGroupId groupId, String name, SemaphoreInvocationKey key) {
        heartbeatSession(groupId, key.endpoint().sessionId());
        AcquireResult result = getOrInitRegistry(groupId).drainPermits(name, key);
        notifyCancelledWaitKey(groupId, name, result.cancelledWaitKey());

        if (logger.isFineEnabled()) {
            logger.fine("Semaphore[" + name + "] in " + groupId + " drained permits: " + result.permits()
                    + " by <" + key.endpoint() + ", " + key.invocationUid() + "> at commit index: " + key.commitIndex());
        }

        return result.permits();
    }

    public boolean changePermits(CPGroupId groupId, String name, SemaphoreInvocationKey key) {
        heartbeatSession(groupId, key.endpoint().sessionId());
        ReleaseResult result = getOrInitRegistry(groupId).changePermits(name, key);
        notifyCancelledWaitKey(groupId, name, result.cancelledWaitKey());
        notifyWaitKeys(groupId, name, result.acquiredWaitKeys(), true);

        if (logger.isFineEnabled()) {
            logger.fine("Semaphore[" + name + "] in " + groupId + " changed permits: " + key.permits() + " by <" + key.endpoint()
                    + ", " + key.invocationUid() + "> at commit index: " + key.commitIndex() + ". new acquires: "
                    + result.acquiredWaitKeys());
        }

        return result.success();
    }

    @Override
    protected SemaphoreRegistry createNewRegistry(CPGroupId groupId) {
        return new SemaphoreRegistry(groupId);
    }

    @Override
    protected Object expiredWaitKeyResponse() {
        return false;
    }

    @Override
    protected String serviceName() {
        return SERVICE_NAME;
    }

    @Override
    public ISemaphore createProxy(String proxyName) {
        try {
            proxyName = withoutDefaultGroupName(proxyName);
            RaftGroupId groupId = raftService.createRaftGroupForProxy(proxyName);
            String objectName = getObjectNameForProxy(proxyName);
            SemaphoreConfig config = getConfig(proxyName);
            return config != null && config.isJDKCompatible()
                    ? new SessionlessSemaphoreProxy(nodeEngine, groupId, proxyName, objectName)
                    : new SessionAwareSemaphoreProxy(nodeEngine, groupId, proxyName, objectName);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }
}
