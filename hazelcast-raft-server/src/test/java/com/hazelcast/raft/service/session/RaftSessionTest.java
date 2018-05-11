package com.hazelcast.raft.service.session;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.session.RaftSessionService.RaftGroupSessionsContainer;
import com.hazelcast.raft.service.session.operation.CloseSessionOp;
import com.hazelcast.raft.service.session.operation.CreateSessionOp;
import com.hazelcast.raft.service.session.operation.KeepAliveAllSessionsOp;
import com.hazelcast.raft.service.session.operation.KeepAliveSessionOp;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftSessionTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private RaftInvocationManager invocationManager;
    private RaftGroupId groupId;

    @Before
    public void setup() throws ExecutionException, InterruptedException {
        int raftGroupSize = 3;
        Address[] raftAddresses = createAddresses(raftGroupSize);
        instances = newInstances(raftAddresses, raftGroupSize, 0);
        invocationManager = getRaftInvocationService(instances[0]);
        groupId = invocationManager.createRaftGroup("sessions", raftGroupSize).get();
    }

    @Test
    public void testSessionStart() throws ExecutionException, InterruptedException {
        final long sessionId = invocationManager.<Long>invoke(groupId, new CreateSessionOp(groupId)).get();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    RaftGroupSessionsContainer container = service.getRaftGroupSessionsContainerOrNull(groupId);
                    assertNotNull(container);
                    assertNotNull(container.getSessionExpirationTime(sessionId));
                }
            }
        });
    }

    @Test
    public void testKeepAlive()
            throws ExecutionException, InterruptedException {
        final long sessionId = invocationManager.<Long>invoke(groupId, new CreateSessionOp(groupId)).get();

        final long[] sessionExpirationTimes = new long[instances.length];
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < instances.length; i++) {
                    RaftSessionService service = getNodeEngineImpl(instances[i]).getService(RaftSessionService.SERVICE_NAME);
                    RaftGroupSessionsContainer container = service.getRaftGroupSessionsContainerOrNull(groupId);
                    assertNotNull(container);
                    Long l = container.getSessionExpirationTime(sessionId);
                    assertNotNull(l);
                    sessionExpirationTimes[i] = l;
                }
            }
        });

        invocationManager.invoke(groupId, new KeepAliveSessionOp(groupId, sessionId)).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < instances.length; i++) {
                    RaftSessionService service = getNodeEngineImpl(instances[i]).getService(RaftSessionService.SERVICE_NAME);
                    RaftGroupSessionsContainer container = service.getRaftGroupSessionsContainerOrNull(groupId);
                    Long newSessionExpirationTime = container.getSessionExpirationTime(sessionId);
                    assertNotNull(newSessionExpirationTime);
                    assertTrue(newSessionExpirationTime > sessionExpirationTimes[i]);
                }
            }
        });
    }

    @Test
    public void testKeepAliveAll() throws ExecutionException, InterruptedException {
        final long sessionId = invocationManager.<Long>invoke(groupId, new CreateSessionOp(groupId)).get();

        final long[] sessionExpirationTimes = new long[instances.length];
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < instances.length; i++) {
                    RaftSessionService service = getNodeEngineImpl(instances[i]).getService(RaftSessionService.SERVICE_NAME);
                    RaftGroupSessionsContainer container = service.getRaftGroupSessionsContainerOrNull(groupId);
                    assertNotNull(container);
                    Long l = container.getSessionExpirationTime(sessionId);
                    assertNotNull(l);
                    sessionExpirationTimes[i] = l;
                }
            }
        });

        invocationManager.invoke(groupId, new KeepAliveAllSessionsOp(groupId)).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < instances.length; i++) {
                    RaftSessionService service = getNodeEngineImpl(instances[i]).getService(RaftSessionService.SERVICE_NAME);
                    RaftGroupSessionsContainer container = service.getRaftGroupSessionsContainerOrNull(groupId);
                    Long newSessionExpirationTime = container.getSessionExpirationTime(sessionId);
                    assertNotNull(newSessionExpirationTime);
                    assertTrue(newSessionExpirationTime > sessionExpirationTimes[i]);
                }
            }
        });
    }

    @Test
    public void testKeepAliveFailsAfterSessionClosed() throws ExecutionException, InterruptedException {
        final long sessionId = invocationManager.<Long>invoke(groupId, new CreateSessionOp(groupId)).get();
        invocationManager.invoke(groupId, new CloseSessionOp(groupId, sessionId)).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    RaftGroupSessionsContainer container = service.getRaftGroupSessionsContainerOrNull(groupId);
                    assertNull(container.getSessionExpirationTime(sessionId));
                }
            }
        });

        try {
            invocationManager.invoke(groupId, new KeepAliveSessionOp(groupId, sessionId)).get();
            fail();
        } catch (IllegalStateException ignored) {
        }
    }

    @Override
    protected Config createConfig(Address[] raftAddresses, int metadataGroupSize) {
        ServiceConfig atomicLongServiceConfig = new ServiceConfig().setEnabled(true)
                                                                   .setName(RaftSessionService.SERVICE_NAME)
                                                                   .setClassName(RaftSessionService.class.getName());

        Config config = super.createConfig(raftAddresses, metadataGroupSize);
        config.getServicesConfig().addServiceConfig(atomicLongServiceConfig);


        return config;
    }

}
