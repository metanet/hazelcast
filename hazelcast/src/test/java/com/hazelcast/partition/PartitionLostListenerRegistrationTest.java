package com.hazelcast.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.PartitionLostListener;
import com.hazelcast.core.PartitionService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class PartitionLostListenerRegistrationTest
        extends HazelcastTestSupport {

    @Test(expected = NullPointerException.class)
    public void testAddPartitionLostListener_whenNullListener() {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionService partitionService = hz.getPartitionService();

        partitionService.addPartitionLostListener(null);
    }

    @Test
    public void testAddPartitionLostListener_whenListenerRegistered() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        PartitionService partitionService = hz1.getPartitionService();

        PartitionLostListener listener = mock(PartitionLostListener.class);

        String id = partitionService.addPartitionLostListener(listener);
        assertNotNull(id);
    }

    @Test
    public void testAddPartitionLostListener_whenListenerRegisteredTwice() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        PartitionService partitionService = hz1.getPartitionService();

        PartitionLostListener listener = mock(PartitionLostListener.class);

        String id1 = partitionService.addPartitionLostListener(listener);
        String id2 = partitionService.addPartitionLostListener(listener);

        // first we check if the registration id's are different
        assertNotEquals(id1, id2);
    }

    @Test
    public void testRemoveMigrationListener_whenRegisteredListenerRemovedSuccessfully() {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionService partitionService = hz.getPartitionService();

        PartitionLostListener listener = mock(PartitionLostListener.class);

        String id1 = partitionService.addPartitionLostListener(listener);
        boolean result = partitionService.removePartitionLostListener(id1);

        assertTrue(result);
    }


    @Test
    public void testRemoveMigrationListener_whenNonExistingRegistrationIdRemoved() {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionService partitionService = hz.getPartitionService();

        boolean result = partitionService.removePartitionLostListener("notexist");

        assertFalse(result);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveMigrationListener_whenNullRegistrationIdRemoved() {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionService partitionService = hz.getPartitionService();

        partitionService.removePartitionLostListener(null);
    }

}
