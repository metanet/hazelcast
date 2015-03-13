package com.hazelcast.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.PartitionLostEvent;
import com.hazelcast.core.PartitionLostListener;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class PartitionLostListenerInvocationTest
        extends AbstractPartitionLostListenerTest {

    static class PartitionIdCollectingPartitionLostListener
            implements PartitionLostListener {

        private List<PartitionLostEvent> lostPartitions = new ArrayList<PartitionLostEvent>();

        @Override
        public synchronized void onEvent(PartitionLostEvent event) {
            lostPartitions.add(event);
        }

        public synchronized List<PartitionLostEvent> getLostPartitions() {
            return lostPartitions;
        }
    }

    private static final int NODE_COUNT = 5;

    private static final int ITEM_COUNT_PER_MAP = 1000;

    @Test
    public void test_partitionLostListenerInvoked_when1NodeCrashed_withoutData()
            throws InterruptedException {
        testPartitionLostListener(NODE_COUNT, 1, false);
    }

    @Test
    public void test_partitionLostListenerInvoked_when1NodeCrashed_withData()
            throws InterruptedException {
        testPartitionLostListener(NODE_COUNT, 1, true);
    }

    @Test
    public void test_partitionLostListenerInvoked_when2NodesCrashed_withoutData()
            throws InterruptedException {
        testPartitionLostListener(NODE_COUNT, 2, false);
    }

    @Test
    public void test_partitionLostListenerInvoked_when2NodesCrashed_withData()
            throws InterruptedException {
        testPartitionLostListener(NODE_COUNT, 2, true);
    }

    @Test
    public void test_partitionLostListenerInvoked_when3NodesCrashed_withoutData()
            throws InterruptedException {
        testPartitionLostListener(NODE_COUNT, 3, false);
    }

    @Test
    public void test_partitionLostListenerInvoked_when3NodesCrashed_withData()
            throws InterruptedException {
        testPartitionLostListener(NODE_COUNT, 3, true);
    }

    @Test
    public void test_partitionLostListenerInvoked_when4NodesCrashed_withoutData()
            throws InterruptedException {
        testPartitionLostListener(NODE_COUNT, 4, false);
    }

    @Test
    public void test_partitionLostListenerInvoked_when4NodesCrashed_withData()
            throws InterruptedException {
        testPartitionLostListener(NODE_COUNT, 4, true);
    }

    @Test
    public void test_partitionLostListenerNotInvoked_whenNewNodesJoin() {
        final HazelcastInstance master = createInstances(1).get(0);
        final PartitionIdCollectingPartitionLostListener listener = registerPartitionLostListener(master);
        final List<HazelcastInstance> others = createInstances(NODE_COUNT - 1);

        waitAllForSafeState(master);
        waitAllForSafeState(others);

        assertTrue("No invocation to PartitionLostListener when new nodes join to cluster",
                listener.getLostPartitions().isEmpty());
    }

    public int getNodeCount(){
        return NODE_COUNT;
    }

    private void testPartitionLostListener(final int nodeCount, final int numberOfNodesToCrash, final boolean withData) {
        final List<HazelcastInstance> instances = getCreatedInstancesShuffledAndWarmedUp(nodeCount);

        List<HazelcastInstance> survivingInstances = new ArrayList<HazelcastInstance>(instances);
        final List<HazelcastInstance> terminatingInstances = survivingInstances.subList(0, numberOfNodesToCrash);
        survivingInstances = survivingInstances.subList(numberOfNodesToCrash, instances.size());

        if (withData) {
            populateMaps(survivingInstances.get(0), nodeCount);
        }

        final StringBuilder logBuilder = new StringBuilder();
        logBuilder.append("Surviving: ").append(survivingInstances).append(" Terminating: ").append(terminatingInstances);
        final PartitionIdCollectingPartitionLostListener listener = registerPartitionLostListener(survivingInstances.get(0));
        final Map<Integer, Integer> survivingPartitions = getOwnedOrReplicaPartitions(survivingInstances, nodeCount);

        terminateInstances(terminatingInstances);
        waitAllForSafeState(survivingInstances);

        assertLostPartitions(logBuilder, listener, survivingPartitions);
    }

    private void assertLostPartitions(final StringBuilder logBuilder, final PartitionIdCollectingPartitionLostListener listener,
                                      final Map<Integer, Integer> survivingPartitions) {
        final List<PartitionLostEvent> failedPartitions = listener.getLostPartitions();

        assertFalse(survivingPartitions.isEmpty());

        Map<Integer, Integer> memorizedPartitionFailures = new HashMap<Integer, Integer>();

        for (PartitionLostEvent event : failedPartitions) {
            final int failedPartitionId = event.getPartitionId();
            final int lostReplicaIndex = event.getLostReplicaIndex();
            final int survivingReplicaIndex = survivingPartitions.get(failedPartitionId);

            final String logMsg = logBuilder.toString() +
                    ", PartitionId: " + failedPartitionId + " LostReplicaIndex: " + lostReplicaIndex + " SurvivingReplicaIndex: "
                    + survivingReplicaIndex + " Event Source: " + event.getEventSource();

            assertTrue(logMsg, survivingReplicaIndex > 0);
            assertTrue(logMsg, lostReplicaIndex >= 0);
            assertTrue(logMsg, lostReplicaIndex <= survivingReplicaIndex - 1);

            final Integer previouslyLostReplicaIndex = memorizedPartitionFailures.get(failedPartitionId);
            if (previouslyLostReplicaIndex != null) {
                assertTrue(logMsg + " PreviouslyLostReplicaIndex: " + previouslyLostReplicaIndex,
                        previouslyLostReplicaIndex < lostReplicaIndex);
            }
            memorizedPartitionFailures.put(failedPartitionId, lostReplicaIndex);
        }
    }

    private void populateMaps(final HazelcastInstance instance, final int nodeCount) {
        for (int i = 0; i < nodeCount; i++) {
            final Map<Integer, Integer> map = instance.getMap("map" + i);
            for (int j = 0; j < ITEM_COUNT_PER_MAP; j++) {
                map.put(j, j);
            }
        }
    }

    private PartitionIdCollectingPartitionLostListener registerPartitionLostListener(final HazelcastInstance instance) {
        final PartitionIdCollectingPartitionLostListener listener = new PartitionIdCollectingPartitionLostListener();
        instance.getPartitionService().addPartitionLostListener(listener);
        return listener;
    }


}
