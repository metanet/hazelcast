package com.hazelcast.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.a.A_PartitionLostListenerStressTest.EventCollectingPartitionLostListener;
import com.hazelcast.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PartitionLostListenerTest
        extends AbstractPartitionLostListenerTest {

    @Override
    public int getNodeCount() {
        return 2;
    }

    @Test
    public void test_partitionLostListenerInvoked(){
        final List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp(1);
        final HazelcastInstance instance = instances.get(0);
        final EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();
        instance.getPartitionService().addPartitionLostListener(listener);

        final InternalPartitionLostEvent internalEvent = new InternalPartitionLostEvent(1, 0, null);

        final InternalPartitionServiceImpl partitionService =
                (InternalPartitionServiceImpl) getNode(instance).getNodeEngine().getPartitionService();
        partitionService.onPartitionLost(internalEvent);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final List<PartitionLostEvent> events = listener.getEvents();
                assertEquals(1, events.size());
                final PartitionLostEvent event = events.get(0);
                assertEquals(internalEvent.getPartitionId(), event.getPartitionId());
                assertEquals(internalEvent.getLostReplicaIndex(), event.getLostBackupCount());
            }
        });
    }

    @Test
    public void test_partitionLostListenerInvoked_whenNodeCrashed() {
        final List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp();

        final HazelcastInstance survivingInstance = instances.get(0);
        final HazelcastInstance terminatingInstance = instances.get(1);

        final EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();
        survivingInstance.getPartitionService().addPartitionLostListener(listener);

        final Set<Integer> survivingPartitionIds = new HashSet<Integer>();
        final Node survivingNode = getNode(survivingInstance);
        final Address survivingAddress = survivingNode.getThisAddress();

        for (InternalPartition partition : survivingNode.getPartitionService().getPartitions()) {
            if (survivingAddress.equals(partition.getReplicaAddress(0))) {
                survivingPartitionIds.add(partition.getPartitionId());
            }
        }

        terminatingInstance.getLifecycleService().terminate();
        waitAllForSafeState(survivingInstance);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final List<PartitionLostEvent> events = listener.getEvents();
                assertFalse(events.isEmpty());

                for (PartitionLostEvent event : events) {
                    assertEquals(survivingAddress, event.getEventSource());
                    assertFalse(survivingPartitionIds.contains(event.getPartitionId()));
                    assertEquals(0, event.getLostBackupCount());
                }
            }
        });
    }

}
