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

package com.hazelcast.raft.service.blocking;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftGroupLifecycleAwareService;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.session.SessionAccessor;
import com.hazelcast.raft.impl.session.SessionAwareService;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.blocking.operation.InvalidateWaitKeysOp;
import com.hazelcast.raft.service.session.AbstractSessionManager;
import com.hazelcast.raft.service.spi.RaftRemoteService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * TODO: Javadoc Pending...
 */
public abstract class AbstractBlockingService<W extends WaitKey, R extends BlockingResource<W>, RR extends ResourceRegistry<W, R>>
        implements ManagedService, RaftGroupLifecycleAwareService, RaftRemoteService, SessionAwareService,
                   SnapshotAwareService<RR> {

    public static final long WAIT_TIMEOUT_TASK_UPPER_BOUND_MILLIS = 1500;
    private static final long WAIT_TIMEOUT_TASK_PERIOD_MILLIS = 500;

    private final ConcurrentMap<RaftGroupId, RR> registries = new ConcurrentHashMap<RaftGroupId, RR>();
    protected final NodeEngine nodeEngine;
    protected final ILogger logger;
    protected volatile RaftService raftService;
    private volatile SessionAccessor sessionAccessor;

    protected AbstractBlockingService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public final void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(new InvalidateExpiredWaitKeysPeriodicTask(),
                WAIT_TIMEOUT_TASK_PERIOD_MILLIS, WAIT_TIMEOUT_TASK_PERIOD_MILLIS, MILLISECONDS);

        initImpl();
    }

    protected void initImpl() {
    }

    @Override
    public void reset() {
    }

    @Override
    public final void shutdown(boolean terminate) {
        registries.clear();
        shutdownImpl(terminate);
    }

    protected void shutdownImpl(boolean terminate) {
    }

    @Override
    public boolean destroyRaftObject(RaftGroupId groupId, String name) {
        Collection<Long> indices = getOrInitRegistry(groupId).destroyResource(name);
        if (indices == null) {
            return false;
        }

        completeFutures(groupId, indices, new DistributedObjectDestroyedException(name + " is destroyed"));
        return true;
    }

    @Override
    public final RR takeSnapshot(RaftGroupId groupId, long commitIndex) {
        return getRegistryOrNull(groupId);
    }

    @Override
    public final void restoreSnapshot(RaftGroupId groupId, long commitIndex, RR registry) {
        RR prev = registries.put(registry.getGroupId(), registry);
        // do not shift the already existing wait timeouts...
        Map<W, Tuple2<Long, Long>> existingWaitTimeouts =
                prev != null ? prev.getWaitTimeouts() : Collections.<W, Tuple2<Long,Long>>emptyMap();
        Map<W, Long> newWaitKeys = registry.overwriteWaitTimeouts(existingWaitTimeouts);
        for (Entry<W, Long> e : newWaitKeys.entrySet()) {
            scheduleTimeout(groupId, e.getKey(), e.getValue());
        }
    }

    @Override
    public void setSessionAccessor(SessionAccessor accessor) {
        this.sessionAccessor = accessor;
    }

    @Override
    public final void onSessionInvalidated(RaftGroupId groupId, long sessionId) {
        ResourceRegistry<W, R> registry = registries.get(groupId);
        if (registry == null) {
            logger.warning("Resource registry of " + groupId + " not found to handle invalidated Session[" + sessionId + "]");
            return;
        }

        Map<Long, Object> result = registry.invalidateSession(sessionId);
        RaftNodeImpl raftNode = (RaftNodeImpl) raftService.getRaftNode(groupId);
        for (Entry<Long, Object> entry : result.entrySet()) {
            raftNode.completeFuture(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public final Collection<Long> getActiveSessions(RaftGroupId groupId) {
        RR registry = getRegistryOrNull(groupId);
        return registry != null ? registry.getActiveSessions() : Collections.<Long>emptyList();
    }

    @Override
    public final void onGroupDestroy(final RaftGroupId groupId) {
        ResourceRegistry<W, R> registry = registries.get(groupId);
        if (registry != null) {
            Collection<Long> indices = registry.destroy();
            completeFutures(groupId, indices, new DistributedObjectDestroyedException(groupId + " is destroyed"));
        }
    }

    public final void invalidateWaitKeys(RaftGroupId groupId, Collection<W> keys) {
        // no need to validate the session. if the session is invalid, the corresponding wait key is gone already
        ResourceRegistry<W, R> registry = registries.get(groupId);
        if (registry == null) {
            logger.severe("Lock registry of " + groupId + " not found to invalidate wait keys: " + keys);
            return;
        }

        List<Long> invalidated = new ArrayList<Long>();
        for (W key : keys) {
            if (registry.invalidateWaitKey(key)) {
                invalidated.add(key.commitIndex());
                if (logger.isFineEnabled()) {
                    logger.fine("Wait key of " + key + " is invalidated.");
                }
            }
        }

        completeFutures(groupId, invalidated, invalidatedWaitKeyResponse());
    }

    public final RR getRegistryOrNull(RaftGroupId groupId) {
        return registries.get(groupId);
    }

    protected final RR getOrInitRegistry(RaftGroupId groupId) {
        checkNotNull(groupId);
        RR registry = registries.get(groupId);
        if (registry == null) {
            registry = createNewRegistry(groupId);
            registries.put(groupId, registry);
        }
        return registry;
    }

    protected final void scheduleTimeout(RaftGroupId groupId, W waitKey, long timeoutMs) {
        if (timeoutMs > 0 && timeoutMs <= WAIT_TIMEOUT_TASK_UPPER_BOUND_MILLIS) {
            Runnable task = new InvalidateExpiredWaitKeysTask(groupId, waitKey);
            ExecutionService executionService = nodeEngine.getExecutionService();
            executionService.schedule(task, timeoutMs, MILLISECONDS);
        }
    }

    protected final void heartbeatSession(RaftGroupId groupId, long sessionId) {
        if (sessionId == AbstractSessionManager.NO_SESSION_ID) {
            return;
        }

        if (sessionAccessor.isValid(groupId, sessionId)) {
            sessionAccessor.heartbeat(groupId, sessionId);
            return;
        }

        throw new SessionExpiredException();
    }

    protected final void notifyWaitKeys(RaftGroupId groupId, Collection<W> keys, Object result) {
        if (keys.isEmpty()) {
            return;
        }

        List<Long> indices = new ArrayList<Long>(keys.size());
        for (W key : keys) {
            indices.add(key.commitIndex());
        }

        completeFutures(groupId, indices, result);
    }

    private void completeFutures(RaftGroupId groupId, Collection<Long> indices, Object result) {
        if (!indices.isEmpty()) {
            RaftNodeImpl raftNode = (RaftNodeImpl) raftService.getRaftNode(groupId);
            for (Long index : indices) {
                raftNode.completeFuture(index, result);
            }
        }
    }

    protected abstract RR createNewRegistry(RaftGroupId groupId);

    protected abstract Object invalidatedWaitKeyResponse();

    protected abstract String serviceName();

    private void invokeInvalidateWaitKeysOp(RaftGroupId groupId, Collection<W> keys) {
        try {
            RaftNode raftNode = raftService.getRaftNode(groupId);
            if (raftNode != null) {
                Future f = raftNode.replicate(new InvalidateWaitKeysOp<W>(serviceName(), keys));
                f.get();
            }
        } catch (Exception e) {
            if (logger.isFineEnabled()) {
                logger.fine("Could not invalidate wait entries: " + keys + " in " + groupId, e);
            }
        }
    }

    private class InvalidateExpiredWaitKeysTask implements Runnable {
        final RaftGroupId groupId;
        final Collection<W> keys;

        InvalidateExpiredWaitKeysTask(RaftGroupId groupId, W key) {
            this.groupId = groupId;
            this.keys = Collections.singleton(key);
        }

        @Override
        public void run() {
            invokeInvalidateWaitKeysOp(groupId, keys);
        }
    }

    private class InvalidateExpiredWaitKeysPeriodicTask implements Runnable {
        @Override
        public void run() {
            for (Entry<RaftGroupId, Collection<W>> e : getExpiredWaitKeys().entrySet()) {
                invokeInvalidateWaitKeysOp(e.getKey(), e.getValue());
            }
        }

        // queried locally
        private Map<RaftGroupId, Collection<W>> getExpiredWaitKeys() {
            Map<RaftGroupId, Collection<W>> timeouts = new HashMap<RaftGroupId, Collection<W>>();
            long now = Clock.currentTimeMillis();
            for (ResourceRegistry<W, R> registry : registries.values()) {
                Collection<W> t = registry.getExpiredWaitKeys(now);
                if (t.size() > 0) {
                    timeouts.put(registry.getGroupId(), t);
                }
            }

            return timeouts;
        }

    }
}
