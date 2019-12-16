/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.atomicref;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicFencedReference;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.atomicref.proxy.AtomicFencedRefProxy;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cp.CPGroup.DEFAULT_GROUP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AtomicFencedRefBasicTest
        extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private IAtomicFencedReference<String> atomicRef;

    @Before
    public void setup() {
        instances = newInstances(3, 3, 0);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instances[0]);
        RaftGroupId groupId = getRaftInvocationManager(instances[0]).createRaftGroup(DEFAULT_GROUP_NAME, 3).joinInternal();
        atomicRef = new AtomicFencedRefProxy<>(nodeEngine, groupId, "ref", "ref");
        assertNotNull(atomicRef);
    }

    @Test
    public void testX() {
        assertNull(atomicRef.get());

        assertTrue(atomicRef.set("val1", 5));

        assertEquals("val1", atomicRef.get());

        assertFalse(atomicRef.set("val2", 3));

        assertTrue(atomicRef.attemptFence(10));

        assertFalse(atomicRef.attemptFence(9));

        assertFalse(atomicRef.set("val3", 5));

        assertTrue(atomicRef.set("val4", 15));

        assertEquals("val4", atomicRef.get());

        assertTrue(atomicRef.compareAndSet("val4", "val5", 15));
        assertFalse(atomicRef.compareAndSet("val4", "val5", 15));
        assertFalse(atomicRef.compareAndSet("val5", "val6", 14));
    }

}
