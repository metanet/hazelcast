package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapPartitionLostEvent;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.partition.AbstractPartitionLostListenerTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapPartitionLostListenerTest
        extends AbstractPartitionLostListenerTest {

    static class EventCollectingMapPartitionLostListener
            implements MapPartitionLostListener {

        private final List<MapPartitionLostEvent> events = Collections.synchronizedList(new LinkedList<MapPartitionLostEvent>());

        private final int backupCount;

        public EventCollectingMapPartitionLostListener(int backupCount) {
            this.backupCount = backupCount;
        }

        @Override
        public void partitionLost(MapPartitionLostEvent event) {
            this.events.add(event);
        }

        public List<MapPartitionLostEvent> getEvents() {
            synchronized (events) {
                return new ArrayList<MapPartitionLostEvent>(events);
            }
        }

        public int getBackupCount() {
            return backupCount;
        }
    }

    private static final int NODE_COUNT = 5;

    private static final int ITEM_COUNT_PER_MAP = 1000;

    public int getNodeCount() {
        return NODE_COUNT;
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when1NodeCrashed_withoutData()
            throws InterruptedException {
        testMapPartitionLostListener(NODE_COUNT, 1, false);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when1NodeCrashed_withData()
            throws InterruptedException {
        testMapPartitionLostListener(NODE_COUNT, 1, true);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when2NodesCrashed_withoutData()
            throws InterruptedException {
        testMapPartitionLostListener(NODE_COUNT, 2, false);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when2NodesCrashed_withData()
            throws InterruptedException {
        testMapPartitionLostListener(NODE_COUNT, 2, true);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when3NodesCrashed_withoutData()
            throws InterruptedException {
        testMapPartitionLostListener(NODE_COUNT, 3, false);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when3NodesCrashed_withData()
            throws InterruptedException {
        testMapPartitionLostListener(NODE_COUNT, 3, true);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when4NodesCrashed_withoutData()
            throws InterruptedException {
        testMapPartitionLostListener(NODE_COUNT, 4, false);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when4NodesCrashed_withData()
            throws InterruptedException {
        testMapPartitionLostListener(NODE_COUNT, 4, true);
    }

    private void testMapPartitionLostListener(final int nodeCount, final int numberOfNodesToCrash, final boolean withData) {
        final List<HazelcastInstance> instances = getCreatedInstancesShuffledAndWarmedUp(nodeCount);

        List<HazelcastInstance> survivingInstances = new ArrayList<HazelcastInstance>(instances);
        final List<HazelcastInstance> terminatingInstances = survivingInstances.subList(0, numberOfNodesToCrash);
        survivingInstances = survivingInstances.subList(numberOfNodesToCrash, instances.size());

        final List<EventCollectingMapPartitionLostListener> listeners = createMaps(nodeCount, survivingInstances.get(0),
                withData);

        final StringBuilder logBuilder = new StringBuilder();
        logBuilder.append("Surviving: ").append(survivingInstances).append(" Terminating: ").append(terminatingInstances);
        final Map<Integer, Integer> survivingPartitions = getOwnedOrReplicaPartitions(survivingInstances, nodeCount);

        terminateInstances(terminatingInstances);
        waitAllForSafeState(survivingInstances);

        for (int i = 0; i < nodeCount; i++) {
            final EventCollectingMapPartitionLostListener listener = listeners.get(i);

            if (i < numberOfNodesToCrash) {
                assertLostPartitions(logBuilder, listener, survivingPartitions);
            } else {
                final String logMsg = logBuilder.toString() + " listener-" + i + " should not be invoked!";
                assertTrue(logMsg, listener.getEvents().isEmpty());
            }
        }
    }

    private void assertLostPartitions(final StringBuilder logBuilder,
                                      final EventCollectingMapPartitionLostListener listener,
                                      final Map<Integer, Integer> survivingPartitions) {
        final List<MapPartitionLostEvent> events = listener.getEvents();

        assertFalse(survivingPartitions.isEmpty());

        for (MapPartitionLostEvent event : events) {
            final int failedPartitionId = event.getLostPartitionId();
            final Integer survivingReplicaIndex = survivingPartitions.get(failedPartitionId);
            final String logMsg = logBuilder.toString() +
                    ", PartitionId: " + failedPartitionId
                    + " SurvivingReplicaIndex: " + survivingReplicaIndex +" Map Name: " + event.getName();
            assertTrue(logMsg, (survivingReplicaIndex == null || survivingReplicaIndex > listener.getBackupCount()));
        }
    }

    private List<EventCollectingMapPartitionLostListener> createMaps(final int nodeCount,
                                                                     final HazelcastInstance instance,
                                                                     final boolean withData) {
        final List<EventCollectingMapPartitionLostListener> listeners = new ArrayList<EventCollectingMapPartitionLostListener>();
        for (int i = 0; i < nodeCount; i++) {
            final IMap<Integer, Integer> map = instance.getMap("map" + i);
            final EventCollectingMapPartitionLostListener listener = new EventCollectingMapPartitionLostListener(i);
            map.addPartitionLostListener(listener);
            listeners.add(listener);
            if (withData) {
                for (int j = 0; j < ITEM_COUNT_PER_MAP; j++) {
                    map.put(j, j);
                }
            }
        }

        return listeners;
    }

}
