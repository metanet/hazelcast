package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.operation.PutOperation;
import com.hazelcast.replicatedmap.impl.operation.VersionResponsePair;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.util.Preconditions.isNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(value = {SlowTest.class, ParallelTest.class})
public class ReplicatedMapStressTest
        extends HazelcastTestSupport {

    @Repeat(100)
    @Test
    public void testPutWithTTL_withoutMigration()
            throws Exception {
        int nodeCount = 2;
        final int keyCount = 1000000;
        final int threadCount = 4;
        final int partitionId = 0;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final HazelcastInstance[] instances = factory
                .newInstances(new Config().setProperty("hazelcast.logging.type", "log4j"), nodeCount);

        warmUpPartitions(instances);

        final String mapName = randomMapName();
        final NodeEngineImpl nodeEngine1 = getNodeEngineImpl(instances[0]);
        final NodeEngineImpl nodeEngine2 = getNodeEngineImpl(instances[1]);

        final Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final int startIndex = i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = startIndex; j < keyCount; j += threadCount) {
                        put(nodeEngine1, mapName, partitionId, j, j);
//                        System.out.println("Thread " + startIndex + " is putting " + j);
                    }
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                ReplicatedMapService service1 = nodeEngine1.getService(ReplicatedMapService.SERVICE_NAME);
                ReplicatedMapService service2 = nodeEngine2.getService(ReplicatedMapService.SERVICE_NAME);
                ReplicatedRecordStore store1 = service1.getReplicatedRecordStore(mapName, false, partitionId);
                ReplicatedRecordStore store2 = service2.getReplicatedRecordStore(mapName, false, partitionId);

                for (int i = 0; i < keyCount; i++) {
                    assertTrue(store1.containsKey(i));
                    assertTrue(store2.containsKey(i));
                }
            }
        });
    }

    private <K, V> V put(final NodeEngine nodeEngine, final String mapName, final int partitionId, K key, V value) {
        isNotNull(key, "key must not be null!");
        isNotNull(value, "value must not be null!");
        Data dataKey = nodeEngine.toData(key);
        Data dataValue = nodeEngine.toData(value);

        PutOperation putOperation = new PutOperation(mapName, dataKey, dataValue);
        InternalCompletableFuture<Object> future = nodeEngine.getOperationService()
                                                             .invokeOnPartition(ReplicatedMapService.SERVICE_NAME, putOperation,
                                                                     partitionId);
        VersionResponsePair result = (VersionResponsePair) future.join();
        return nodeEngine.toObject(result.getResponse());
    }

}
