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

package com.hazelcast.raft.impl.session;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.RaftSessionManagementService;
import com.hazelcast.raft.SessionInfo;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftGroupLifecycleAwareService;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.TermChangeAwareService;
import com.hazelcast.raft.impl.session.operation.CloseInactiveSessionsOp;
import com.hazelcast.raft.impl.session.operation.CloseSessionOp;
import com.hazelcast.raft.impl.session.operation.InvalidateSessionsOp;
import com.hazelcast.raft.impl.util.PartitionSpecificRunnableAdaptor;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableCollection;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * TODO: Javadoc Pending...
 */
public class RaftSessionService
        implements ManagedService, SnapshotAwareService<SessionRegistrySnapshot>, SessionAccessor,
        TermChangeAwareService, RaftGroupLifecycleAwareService, RaftSessionManagementService {

    public static String SERVICE_NAME = "hz:core:raftSession";
    private static final long CHECK_EXPIRED_SESSIONS_TASK_PERIOD_IN_MILLIS = SECONDS.toMillis(1);

    private static final long CHECK_INACTIVE_SESSIONS_TASK_PERIOD_IN_MILLIS = SECONDS.toMillis(30);

    private final NodeEngine nodeEngine;
    private final ILogger logger;
    private volatile RaftService raftService;

    private final Map<RaftGroupId, SessionRegistry> registries = new ConcurrentHashMap<RaftGroupId, SessionRegistry>();

    public RaftSessionService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        Collection<SessionAwareService> services = nodeEngine.getServices(SessionAwareService.class);
        for (SessionAwareService service : services) {
            service.setSessionAccessor(this);
        }
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(new CheckExpiredSessions(), CHECK_EXPIRED_SESSIONS_TASK_PERIOD_IN_MILLIS,
                CHECK_EXPIRED_SESSIONS_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
        executionService.scheduleWithRepetition(new CheckInactiveSessions(), CHECK_INACTIVE_SESSIONS_TASK_PERIOD_IN_MILLIS,
                CHECK_INACTIVE_SESSIONS_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        registries.clear();
    }

    @Override
    public SessionRegistrySnapshot takeSnapshot(RaftGroupId groupId, long commitIndex) {
        SessionRegistry registry = registries.get(groupId);
        return registry != null ? registry.toSnapshot() : null;
    }

    @Override
    public void restoreSnapshot(RaftGroupId groupId, long commitIndex, SessionRegistrySnapshot snapshot) {
        if (snapshot != null) {
            SessionRegistry registry = new SessionRegistry(groupId, snapshot);
            registries.put(groupId, registry);
        }
    }

    @Override
    public void onNewTermCommit(RaftGroupId groupId) {
        SessionRegistry registry = registries.get(groupId);
        if (registry != null) {
            registry.shiftExpirationTimes(getHeartbeatIntervalMillis());
            logger.info("Session expiration times are shifted in " + groupId);
        }
    }

    @Override
    public void onGroupDestroy(RaftGroupId groupId) {
        SessionRegistry registry = registries.remove(groupId);
        // TODO: anything todo more?
    }

    @Override
    public Collection<SessionInfo> getSessions(RaftGroupId groupId) {
        SessionRegistry registry = registries.get(groupId);
        if (registry == null) {
            return emptySet();
        }
        return unmodifiableCollection(registry.getSessions());
    }

    @Override
    public ICompletableFuture<Boolean> forceCloseSession(RaftGroupId groupId, long sessionId) {
        return raftService.getInvocationManager().invoke(groupId, new CloseSessionOp(sessionId));
    }

    public SessionResponse createNewSession(RaftGroupId groupId, Address endpoint) {
        SessionRegistry registry = registries.get(groupId);
        if (registry == null) {
            registry = new SessionRegistry(groupId);
            registries.put(groupId, registry);
            logger.info("Created new session registry for " + groupId);
        }

        long sessionTTLMillis = getSessionTTLMillis();
        long sessionId = registry.createNewSession(sessionTTLMillis, endpoint);
        logger.info("Created new session: " + sessionId + " in " + groupId);
        return new SessionResponse(sessionId, sessionTTLMillis, getHeartbeatIntervalMillis());
    }

    public void heartbeat(RaftGroupId groupId, long sessionId) {
        SessionRegistry registry = registries.get(groupId);
        if (registry == null) {
            throw new IllegalStateException("No session: " + sessionId + " for raft group: " + groupId);
        }

        registry.heartbeat(sessionId, getSessionTTLMillis());
        logger.info("Session: " + sessionId + " heartbeat in " + groupId);
    }

    public boolean closeSession(RaftGroupId groupId, long sessionId) {
        SessionRegistry registry = registries.get(groupId);
        if (registry == null) {
            return false;
        }

        if (registry.closeSession(sessionId)) {
            notifyServices(groupId, Collections.singleton(sessionId));
            return true;
        }

        return false;
    }

    public void invalidateSessions(RaftGroupId groupId, Collection<Tuple2<Long, Long>> sessionsToInvalidate) {
        SessionRegistry registry = registries.get(groupId);
        if (registry == null) {
            return;
        }

        List<Long> invalidated = new ArrayList<Long>();
        for (Tuple2<Long, Long> s : sessionsToInvalidate) {
            long sessionId = s.element1;
            long version = s.element2;
            if (registry.invalidateSession(sessionId, version)) {
                invalidated.add(sessionId);
            }
        }

        if (invalidated.size() > 0) {
            logger.info("Sessions: " + invalidated + " are invalidated in " + groupId);
            notifyServices(groupId, invalidated);
        }
    }

    public void closeInactiveSessions(RaftGroupId groupId, Collection<Long> inactiveSessions) {
        SessionRegistry registry = registries.get(groupId);
        if (registry == null) {
            return;
        }

        Collection<Long> closed = new HashSet<Long>(inactiveSessions);
        for (SessionAwareService service : nodeEngine.getServices(SessionAwareService.class)) {
            closed.removeAll(service.getActiveSessions(groupId));
        }

        for (long sessionId : closed) {
            registry.closeSession(sessionId);
        }

        if (closed.size() > 0) {
            logger.info("Inactive sessions: " + closed + " are closed in " + groupId);
            notifyServices(groupId, closed);
        }
    }

    // queried locally in tests
    SessionRegistry getSessionRegistryOrNull(RaftGroupId groupId) {
        return registries.get(groupId);
    }

    private long getHeartbeatIntervalMillis() {
        return raftService.getConfig().getSessionHeartbeatIntervalMillis();
    }

    private long getSessionTTLMillis() {
        return SECONDS.toMillis(raftService.getConfig().getSessionTimeToLiveSeconds());
    }

    private void notifyServices(RaftGroupId groupId, Collection<Long> sessionIds) {
        Collection<SessionAwareService> services = nodeEngine.getServices(SessionAwareService.class);
        for (SessionAwareService sessionAwareService : services) {
            for (long sessionId : sessionIds) {
                sessionAwareService.onSessionInvalidated(groupId, sessionId);
            }
        }
    }

    @Override
    public boolean isValid(RaftGroupId groupId, long sessionId) {
        SessionRegistry sessionRegistry = registries.get(groupId);
        if (sessionRegistry == null) {
            return false;
        }
        Session session = sessionRegistry.getSession(sessionId);
        return session != null;
    }

    // queried locally
    private Map<RaftGroupId, Collection<Tuple2<Long, Long>>> getExpiredSessions() {
        Map<RaftGroupId, Collection<Tuple2<Long, Long>>> expired = new HashMap<RaftGroupId, Collection<Tuple2<Long, Long>>>();
        for (SessionRegistry registry : registries.values()) {
            Collection<Tuple2<Long, Long>> e = registry.getExpiredSessions();
            if (!e.isEmpty()) {
                expired.put(registry.groupId(), e);
            }
        }

        return expired;
    }

    private Map<RaftGroupId, Collection<Long>> getInactiveSessions() {
        final Map<RaftGroupId, Collection<Long>> response = new ConcurrentHashMap<RaftGroupId, Collection<Long>>();
        final Semaphore semaphore = new Semaphore(0);

        OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();
        Collection<SessionRegistry> registries = new ArrayList<SessionRegistry>(this.registries.values());

        for (final SessionRegistry registry : registries) {
            final RaftGroupId groupId = registry.groupId();
            operationService.execute(new PartitionSpecificRunnableAdaptor(new Runnable() {
                @Override
                public void run() {
                    Set<Long> activeSessionIds = new HashSet<Long>();
                    for (SessionAwareService service : nodeEngine.getServices(SessionAwareService.class)) {
                        activeSessionIds.addAll(service.getActiveSessions(groupId));
                    }

                    Set<Long> inactiveSessionIds = new HashSet<Long>();
                    for (SessionInfo session : registry.getSessions()) {
                        if (!activeSessionIds.contains(session.id())
                                && session.creationTime() + getSessionTTLMillis() < Clock.currentTimeMillis()) {
                            inactiveSessionIds.add(session.id());
                        }
                    }

                    if (inactiveSessionIds.size() > 0) {
                        response.put(groupId, inactiveSessionIds);
                    }

                    semaphore.release();
                }
            }, nodeEngine.getPartitionService().getPartitionId(groupId)));
        }

        try {
            semaphore.tryAcquire(registries.size(), 5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return response;
    }

    private class CheckExpiredSessions implements Runnable {
        @Override
        public void run() {
            Map<RaftGroupId, Collection<Tuple2<Long, Long>>> expiredSessions = getExpiredSessions();
            for (Entry<RaftGroupId, Collection<Tuple2<Long, Long>>> entry : expiredSessions.entrySet()) {
                RaftGroupId groupId = entry.getKey();
                RaftNode raftNode = raftService.getRaftNode(groupId);
                if (raftNode != null) {
                    Collection<Tuple2<Long, Long>> sessions = entry.getValue();
                    try {
                        ICompletableFuture f = raftNode.replicate(new InvalidateSessionsOp(sessions));
                        f.get();
                    } catch (Exception e) {
                        if (logger.isFineEnabled()) {
                            logger.fine("Could not invalidate sessions: " + sessions + " of " + groupId, e);
                        }
                    }
                }
            }
        }
    }

    private class CheckInactiveSessions implements Runnable {
        @Override
        public void run() {
            Map<RaftGroupId, Collection<Long>> inactiveSessions = getInactiveSessions();
            for (Entry<RaftGroupId, Collection<Long>> entry : inactiveSessions.entrySet()) {
                RaftGroupId groupId = entry.getKey();
                RaftNode raftNode = raftService.getRaftNode(groupId);
                if (raftNode != null) {
                    Collection<Long> sessions = entry.getValue();
                    try {
                        ICompletableFuture f = raftNode.replicate(new CloseInactiveSessionsOp(sessions));
                        f.get();
                    } catch (Exception e) {
                        if (logger.isFineEnabled()) {
                            logger.fine("Could not close inactive sessions: " + sessions + " of " + groupId, e);
                        }
                    }
                }
            }

        }
    }
}
