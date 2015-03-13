package com.hazelcast.partition;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractPartitionLostListenerTest extends HazelcastTestSupport {

    protected TestHazelcastInstanceFactory hazelcastInstanceFactory;

    private void initLogger()
            throws IOException {
        Layout layout = new PatternLayout();
        FileAppender appender = new FileAppender(layout, "target/tests/" + System.currentTimeMillis() + ".log");
        Logger root = Logger.getRootLogger();
        root.setLevel(Level.DEBUG);
        root.removeAllAppenders();
        root.addAppender(appender);
    }

    public abstract int getNodeCount();

    @Before
    public void before()
            throws IOException {
        initLogger();
        hazelcastInstanceFactory = createHazelcastInstanceFactory(getNodeCount());
    }

    @After
    public void after() {
        hazelcastInstanceFactory.terminateAll();
    }

    protected void terminateInstances(final List<HazelcastInstance> terminatingInstances) {
        for (HazelcastInstance instance : terminatingInstances) {
            instance.getLifecycleService().terminate();
        }
    }

    protected List<HazelcastInstance> getCreatedInstancesShuffledAndWarmedUp(final int nodeCount) {
        final List<HazelcastInstance> instances = createInstances(nodeCount);
        Collections.shuffle(instances);
        warmUpPartitions(instances);
        return instances;
    }

    protected List<HazelcastInstance> createInstances(final int nodeCount) {
        final List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>();
        final Config config = createConfig(nodeCount);

        for (int i = 0; i < nodeCount; i++) {
            instances.add(hazelcastInstanceFactory.newHazelcastInstance(config));
        }
        return instances;
    }

    protected Config createConfig(final int nodeCount) {
        final Config config = new Config();
        config.setProperty("hazelcast.logging.type", "log4j");
        for (int i = 0; i < nodeCount; i++) {
            MapConfig mapConfig = config.getMapConfig("map" + i);
            mapConfig.setBackupCount(i);
        }
        return config;
    }

    protected Map<Integer, Integer> getOwnedOrReplicaPartitions(final List<HazelcastInstance> instances, final int nodeCount) {
        final Map<Integer, Integer> survivingPartitions = new HashMap<Integer, Integer>();

        for(HazelcastInstance instance : instances) {
            final Node survivingNode = getNode(instance);
            final Address survivingNodeAddress = survivingNode.getThisAddress();

            for (InternalPartition partition : survivingNode.getPartitionService().getPartitions()) {
                if (partition.isOwnerOrBackup(survivingNodeAddress)) {
                    for (int replicaIndex = 0; replicaIndex < nodeCount; replicaIndex++) {
                        if (survivingNodeAddress.equals(partition.getReplicaAddress(replicaIndex))) {
                            final Integer replicaIndexOfOtherInstance = survivingPartitions.get(partition.getPartitionId());
                            if(replicaIndexOfOtherInstance != null) {
                                survivingPartitions.put(partition.getPartitionId(), Math.min(replicaIndex, replicaIndexOfOtherInstance));
                            } else {
                                survivingPartitions.put(partition.getPartitionId(), replicaIndex);
                            }

                            break;
                        }
                    }
                }
            }
        }

        return survivingPartitions;
    }

}
