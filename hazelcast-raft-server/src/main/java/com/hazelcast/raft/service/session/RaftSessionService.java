package com.hazelcast.raft.service.session;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class RaftSessionService implements ManagedService, SnapshotAwareService<RaftSessionService.RaftGroupSessionsSnapshot> {

    public static final String SERVICE_NAME = "hz:raft:sessionService";


    private ILogger logger;

    private long keepAliveMillis = TimeUnit.SECONDS.toMillis(10); // TODO [basri] init this

    private ConcurrentMap<RaftGroupId, RaftGroupSessionsContainer> containers = new ConcurrentHashMap<RaftGroupId, RaftGroupSessionsContainer>();

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        logger = nodeEngine.getLogger(RaftSessionService.class);
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {

    }

    @Override
    public RaftGroupSessionsSnapshot takeSnapshot(RaftGroupId groupId, long commitIndex) {
        return getRaftGroupSessionsContainer(groupId).toSnapshot();
    }

    @Override
    public void restoreSnapshot(RaftGroupId groupId, long commitIndex, RaftGroupSessionsSnapshot snapshot) {
        containers.put(groupId, new RaftGroupSessionsContainer(snapshot));
    }

    public long createNewSession(RaftGroupId groupId) {
        return getRaftGroupSessionsContainer(groupId).createNewSession();
    }

    public long keepAlive(RaftGroupId groupId, long sessionId) {
        return getRaftGroupSessionsContainer(groupId).keepAlive(sessionId);
    }

    public void closeSession(RaftGroupId groupId, long sessionId) {
        getRaftGroupSessionsContainer(groupId).closeSession(sessionId);
    }

    public void keepAliveAll(RaftGroupId groupId) {
        getRaftGroupSessionsContainer(groupId).keepAliveAll();
    }

    public Collection<Long> expireSessions(RaftGroupId groupId, Map<Long, Long> sessionExpirationTimes) {
        return getRaftGroupSessionsContainer(groupId).expireSessions(sessionExpirationTimes);
    }

    RaftGroupSessionsContainer getRaftGroupSessionsContainerOrNull(RaftGroupId groupId) {
        return containers.get(groupId);
    }

    private RaftGroupSessionsContainer getRaftGroupSessionsContainer(RaftGroupId groupId) {
        RaftGroupSessionsContainer container = containers.get(groupId);
        if (container == null) {
            container = new RaftGroupSessionsContainer();
            containers.put(groupId, container);
        }

        return container;
    }

    class RaftGroupSessionsContainer {

        private long nextSessionId;

        private Map<Long, Long> sessionExpirationTimes = new ConcurrentHashMap<Long, Long>();

        RaftGroupSessionsContainer() {
        }

        RaftGroupSessionsContainer(RaftGroupSessionsSnapshot snapshot) {
            this.nextSessionId = snapshot.nextSessionId;
            this.sessionExpirationTimes.putAll(snapshot.sessionExpirationTimes);
        }

        long createNewSession() {
            long newSessionId = nextSessionId++;
            long expirationTime = System.currentTimeMillis() + keepAliveMillis;
            sessionExpirationTimes.put(newSessionId, expirationTime);

            logger.info("Session: " + newSessionId + " is created");

            return newSessionId;
        }

        long keepAlive(long sessionId) {
            Long expirationTime = sessionExpirationTimes.get(sessionId);
            if (expirationTime == null) {
                throw new IllegalStateException("Session: " + sessionId + " is expired!");
            }

            long newExpirationTime = System.currentTimeMillis() + keepAliveMillis;
            sessionExpirationTimes.put(sessionId, newExpirationTime);

            logger.info("Session: " + sessionId + " is kept alive until " + newExpirationTime);

            return newExpirationTime;
        }

        void keepAliveAll() {
            long newExpirationTime = System.currentTimeMillis() + keepAliveMillis;
            for (long sessionId : new ArrayList<Long>(sessionExpirationTimes.keySet())) {
                sessionExpirationTimes.put(sessionId, newExpirationTime);
            }

            logger.info("All sessions are kept alive until " + newExpirationTime);
        }

        void closeSession(long sessionId) {
            Long expirationTime = sessionExpirationTimes.remove(sessionId);
            if (expirationTime == null) {
                throw new IllegalStateException("Session: " + sessionId + " is expired!");
            }

            logger.info("Session: " + sessionId + " is closed");
        }

        Collection<Long> expireSessions(Map<Long, Long> sessionExpirationTimes) {
            Collection<Long> expiredSessionIds = new ArrayList<Long>();
            for (Entry<Long, Long> e : sessionExpirationTimes.entrySet()) {
                final long sessionId = e.getKey();
                final Long expectedExpiry = e.getValue();
                if (expectedExpiry.equals(this.sessionExpirationTimes.get(sessionId))) {
                    this.sessionExpirationTimes.remove(sessionId);
                    expiredSessionIds.add(sessionId);
                }
            }

            if (expiredSessionIds.size() > 0) {
                logger.info("Expired sessions: " + expiredSessionIds);
            } else {
                logger.warning("No expiration for expected session expiration times: " + sessionExpirationTimes);
            }

            return expiredSessionIds;
        }

        Long getSessionExpirationTime(long sessionId) {
            return sessionExpirationTimes.get(sessionId);
        }

        RaftGroupSessionsSnapshot toSnapshot() {
            return new RaftGroupSessionsSnapshot(nextSessionId, sessionExpirationTimes);
        }

    }

    public static class RaftGroupSessionsSnapshot implements IdentifiedDataSerializable {

        private long nextSessionId;

        private Map<Long, Long> sessionExpirationTimes;

        public RaftGroupSessionsSnapshot() {
        }

        RaftGroupSessionsSnapshot(long nextSessionId, Map<Long, Long> sessionExpirationTimes) {
            this.nextSessionId = nextSessionId;
            this.sessionExpirationTimes = new HashMap<Long, Long>(sessionExpirationTimes);
        }

        @Override
        public int getFactoryId() {
            return RaftSessionDataSerializerHook.F_ID;
        }

        @Override
        public int getId() {
            return RaftSessionDataSerializerHook.RAFT_GROUP_SESSIONS_SNAPSHOT;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {
            out.writeLong(nextSessionId);
            out.writeInt(sessionExpirationTimes.size());
            for (Entry<Long, Long> e : sessionExpirationTimes.entrySet()) {
                out.writeLong(e.getKey());
                out.writeLong(e.getValue());
            }
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {
            nextSessionId = in.readLong();
            int size = in.readInt();
            sessionExpirationTimes = new HashMap<Long, Long>(size);
            for (int i = 0; i < size; i++) {
                long sessionId = in.readLong();
                long expirationTime = in.readLong();
                sessionExpirationTimes.put(sessionId, expirationTime);
            }
        }
    }
}
