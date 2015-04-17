package com.hazelcast.partition;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.partition.impl.ReplicaSyncInfo;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.util.scheduler.ScheduledEntry;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.partition.PartitionReplicaVersionsCorrectnessStressTest.getReplicaVersions;

public abstract class AbstractPartitionLostListenerTest
        extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory hazelcastInstanceFactory;

    protected abstract int getNodeCount();

    protected int getMapEntryCount() {
        return 0;
    }

    @Before
    public void createHazelcastInstanceFactory()
            throws IOException {
        hazelcastInstanceFactory = createHazelcastInstanceFactory(getNodeCount());
    }

    @After
    public void terminateAllInstances() {
        hazelcastInstanceFactory.terminateAll();
    }

    final protected void terminateInstances(final List<HazelcastInstance> terminatingInstances) {
        for (HazelcastInstance instance : terminatingInstances) {
            instance.getLifecycleService().terminate();
        }
    }

    final protected List<HazelcastInstance> getCreatedInstancesShuffledAfterWarmedUp() {
        return getCreatedInstancesShuffledAfterWarmedUp(getNodeCount());
    }

    final protected List<HazelcastInstance> getCreatedInstancesShuffledAfterWarmedUp(final int nodeCount) {
        final List<HazelcastInstance> instances = createInstances(nodeCount);
        warmUpPartitions(instances);
        Collections.shuffle(instances);
        return instances;
    }

    final protected List<HazelcastInstance> createInstances(final int nodeCount) {
        final List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>();
        final Config config = createConfig(nodeCount);

        for (int i = 0; i < nodeCount; i++) {
            instances.add(hazelcastInstanceFactory.newHazelcastInstance(config));
        }
        return instances;
    }

    private Config createConfig(final int nodeCount) {
        final Config config = new Config();
//        config.setProperty( "hazelcast.logging.type", "jdk" );
        for (int i = 0; i < nodeCount; i++) {
            config.getMapConfig(getIthMapName(i)).setBackupCount(i);
        }

        return config;
    }

    final protected void populateMaps(final HazelcastInstance instance) {
        for (int i = 0; i < getNodeCount(); i++) {
            final Map<Integer, Integer> map = instance.getMap(getIthMapName(i));
            for (int j = 0; j < getMapEntryCount(); j++) {
                map.put(j, j);
            }
        }
    }

    final protected String getIthMapName(final int i) {
        return "map-" + i;
    }

    final protected void collectMinReplicaIndicesAndPartitionTablesByPartitionId(final List<HazelcastInstance> instances,
                                                                                 final Map<Integer, Integer> survivingPartitions) {
        for (HazelcastInstance instance : instances) {
            final Node survivingNode = getNode(instance);
            final Address survivingNodeAddress = survivingNode.getThisAddress();

            for (InternalPartition partition : survivingNode.getPartitionService().getPartitions()) {
                if (partition.isOwnerOrBackup(survivingNodeAddress)) {
                    for (int replicaIndex = 0; replicaIndex < getNodeCount(); replicaIndex++) {
                        if (survivingNodeAddress.equals(partition.getReplicaAddress(replicaIndex))) {
                            final Integer replicaIndexOfOtherInstance = survivingPartitions.get(partition.getPartitionId());
                            if (replicaIndexOfOtherInstance != null) {
                                survivingPartitions
                                        .put(partition.getPartitionId(), Math.min(replicaIndex, replicaIndexOfOtherInstance));
                            } else {
                                survivingPartitions.put(partition.getPartitionId(), replicaIndex);
                            }

                            break;
                        }
                    }
                }
            }
        }
    }

    final protected void waitAllForSafeStateAndLogReplicaVersions(final List<HazelcastInstance> instances)
            throws InterruptedException {
        try {
            waitAllForSafeState(instances, 300);
        } catch (AssertionError e) {
            final Map<Node, List<Entry<Integer, long[]>>> replicaVersionsByNodes = new HashMap<Node, List<Entry<Integer, long[]>>>();
            final Map<Integer, List<Address>> partitionTables = new HashMap<Integer, List<Address>>();
            collectPartitionReplicaVersions(instances, replicaVersionsByNodes, partitionTables);
            logReplicaVersions(replicaVersionsByNodes);
            logPartitionTables(instances, partitionTables);
            logReplicaSyncInfos(instances);
            throw e;
        }
    }

    final protected void logReplicaVersions(Map<Node, List<Entry<Integer, long[]>>> replicaVersionsByNodes) {
        for (Entry<Node, List<Entry<Integer, long[]>>> nodeEntry : replicaVersionsByNodes.entrySet()) {
            final Node node = nodeEntry.getKey();
            final ILogger logger = node.getLogger(this.getClass());
            for (Entry<Integer, long[]> versionsEntry : nodeEntry.getValue()) {
                logger.info("PartitionReplicaVersions >> " + node.getThisAddress() + " partitionId=" + versionsEntry.getKey()
                        + " replicaVersions=" + Arrays.toString(versionsEntry.getValue()));
            }
        }
    }

    final protected void collectPartitionReplicaVersions(final List<HazelcastInstance> instances,
                                                         final Map<Node, List<Entry<Integer, long[]>>> replicaVersionsByAddress,
                                                         final Map<Integer, List<Address>> partitionTables)
            throws InterruptedException {
        for (HazelcastInstance instance : instances) {
            final Node node = getNode(instance);
            final Address nodeAddress = node.getThisAddress();

            for (InternalPartition partition : node.getPartitionService().getPartitions()) {

                if (nodeAddress.equals(partition.getReplicaAddress(0))) {
                    final List<Address> replicas = new ArrayList<Address>();
                    for (int replicaIndex = 0; replicaIndex < instances.size(); replicaIndex++) {
                        replicas.add(partition.getReplicaAddress(replicaIndex));
                    }
                    partitionTables.put(partition.getPartitionId(), replicas);
                }

                final int partitionId = partition.getPartitionId();
                if (partition.isOwnerOrBackup(nodeAddress)) {
                    final long[] replicaVersions = getReplicaVersions(node, partitionId);

                    List<Entry<Integer, long[]>> nodeReplicaVersions = replicaVersionsByAddress.get(node);
                    if (nodeReplicaVersions == null) {
                        replicaVersionsByAddress.put(node, (nodeReplicaVersions = new ArrayList<Entry<Integer, long[]>>()));
                    }

                    nodeReplicaVersions.add(new SimpleImmutableEntry<Integer, long[]>(partitionId, replicaVersions));
                }
            }
        }

        for (List<Entry<Integer, long[]>> nodeReplicaVersions : replicaVersionsByAddress.values()) {
            Collections.sort(nodeReplicaVersions, new Comparator<Entry<Integer, long[]>>() {
                @Override
                public int compare(Entry<Integer, long[]> e1, Entry<Integer, long[]> e2) {
                    return e1.getKey().compareTo(e2.getKey());
                }
            });
        }
    }

    final protected void logPartitionTables(final List<HazelcastInstance> instances,
                                            final Map<Integer, List<Address>> partitionTables) {
        final Node node = getNode(instances.get(0));
        final ILogger logger = node.getLogger(this.getClass());
        for (Map.Entry<Integer, List<Address>> entry : partitionTables.entrySet()) {
            logger.info("PartitionTable >> partitionId=" + entry.getKey() + " replicas=" + entry.getValue());
        }
    }

    final protected void logReplicaSyncInfos(final List<HazelcastInstance> instances) {
        for (HazelcastInstance instance : instances) {
            final Node node = getNode(instance);
            final ILogger logger = node.getLogger(this.getClass());

            final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) node.getPartitionService();
            final AtomicReferenceArray<ReplicaSyncInfo> replicaSyncRequests = partitionService.getReplicaSyncRequests();
            for (int i=0; i < replicaSyncRequests.length(); i++) {
                final ReplicaSyncInfo replicaSyncInfo = replicaSyncRequests.get(i);
                if (replicaSyncInfo != null) {
                    logger.info("ReplicaSyncInfo >>> " + node.getThisAddress() + " partitionId=" + i + " replicaSyncInfo=" + replicaSyncInfo);
                }
            }

            for(Map.Entry<Integer, ConcurrentMap<Object, ScheduledEntry<Integer, ReplicaSyncInfo>>> e1 : partitionService.getScheduledReplicaSyncRequests().entrySet()){
                for (Map.Entry<Object, ScheduledEntry<Integer, ReplicaSyncInfo>> e2 : e1.getValue().entrySet()){
                    logger.info("Scheduled >>> " + node.getThisAddress() + " timeKey=" + e1.getKey() + " " + " partitionId=" + e2.getKey() + " replicaSyncInfo=" + e2.getValue());
                }
            }
        }
    }

}
