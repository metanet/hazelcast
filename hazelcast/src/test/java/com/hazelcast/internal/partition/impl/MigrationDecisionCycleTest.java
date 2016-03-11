/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.nio.Address;
import org.junit.Test;

import java.net.UnknownHostException;

import static com.hazelcast.internal.partition.impl.MigrationDecision.fixCycle;
import static com.hazelcast.internal.partition.impl.MigrationDecision.isCyclic;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MigrationDecisionCycleTest {

    @Test
    public void testCycle1()
            throws UnknownHostException {
        final Address[] oldReplicas = new Address[]{new Address("localhost", 5701), new Address("localhost",
                5702), null, null, null, null, null};

        final Address[] newReplicas = new Address[]{new Address("localhost", 5702), new Address("localhost",
                5701), null, null, null, null, null};

        assertTrue(isCyclic(oldReplicas, newReplicas));
    }

    @Test
    public void testCycle1_fixed()
            throws UnknownHostException {
        final Address[] oldReplicas = new Address[]{new Address("localhost", 5701), new Address("localhost",
                5702), null, null, null, null, null};

        final Address[] newReplicas = new Address[]{new Address("localhost", 5702), new Address("localhost",
                5701), null, null, null, null, null};

        assertTrue(fixCycle(oldReplicas, newReplicas));
        assertArrayEquals(oldReplicas, newReplicas);
    }

    @Test
    public void testCycle2()
            throws UnknownHostException {
        final Address[] oldReplicas = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), null, null, null, null};

        final Address[] newReplicas = new Address[]{new Address("localhost", 5703), new Address("localhost", 5701), new Address(
                "localhost", 5702), null, null, null, null};

        assertTrue(isCyclic(oldReplicas, newReplicas));
    }

    @Test
    public void testCycle2_fixed()
            throws UnknownHostException {
        final Address[] oldReplicas = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), null, null, null, null};

        final Address[] newReplicas = new Address[]{new Address("localhost", 5703), new Address("localhost", 5701), new Address(
                "localhost", 5702), null, null, null, null};

        assertTrue(fixCycle(oldReplicas, newReplicas));
        assertArrayEquals(oldReplicas, newReplicas);
    }

    @Test
    public void testCycle3()
            throws UnknownHostException {
        final Address[] oldReplicas = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5704), new Address("localhost", 5705), null, null};

        final Address[] newReplicas = new Address[]{new Address("localhost", 5705), new Address("localhost", 5702), new Address(
                "localhost", 5701), new Address("localhost", 5704), new Address("localhost", 5703), null, null};

        assertTrue(isCyclic(oldReplicas, newReplicas));
    }

    @Test
    public void testCycle3_fixed()
            throws UnknownHostException {
        final Address[] oldReplicas = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5704), new Address("localhost", 5705), null, null};

        final Address[] newReplicas = new Address[]{new Address("localhost", 5705), new Address("localhost", 5702), new Address(
                "localhost", 5701), new Address("localhost", 5704), new Address("localhost", 5703), null, null};

        assertTrue(fixCycle(oldReplicas, newReplicas));
        assertArrayEquals(oldReplicas, newReplicas);
    }

    @Test
    public void testCycle4()
            throws UnknownHostException {
        final Address[] oldReplicas = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5704), new Address("localhost", 5705), new Address("localhost",
                5706), new Address("localhost", 5707)};

        final Address[] newReplicas = new Address[]{new Address("localhost", 5705), new Address("localhost", 5702), new Address(
                "localhost", 5701), new Address("localhost", 5704), new Address("localhost", 5703), new Address("localhost",
                5707), new Address("localhost", 5706)};

        assertTrue(isCyclic(oldReplicas, newReplicas));
    }

    @Test
    public void testCycle4_fixed()
            throws UnknownHostException {
        final Address[] oldReplicas = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5704), new Address("localhost", 5705), new Address("localhost",
                5706), new Address("localhost", 5707)};

        final Address[] newReplicas = new Address[]{new Address("localhost", 5705), new Address("localhost", 5702), new Address(
                "localhost", 5701), new Address("localhost", 5704), new Address("localhost", 5703), new Address("localhost",
                5707), new Address("localhost", 5706)};

        assertTrue(fixCycle(oldReplicas, newReplicas));
        assertArrayEquals(oldReplicas, newReplicas);
    }

    @Test
    public void testNoCycle()
            throws UnknownHostException {
        final Address[] oldReplicas = new Address[]{new Address("localhost", 5701), new Address("localhost",
                5702), null, null, null, null, null};

        final Address[] newReplicas = new Address[]{new Address("localhost", 5702), new Address("localhost",
                5703), null, null, null, null, null};

        assertFalse(isCyclic(oldReplicas, newReplicas));
    }

    @Test
    public void testNoCycle2()
            throws UnknownHostException {
        final Address[] oldReplicas = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5704), new Address("localhost", 5705), null, null};

        final Address[] newReplicas = new Address[]{new Address("localhost", 5706), new Address("localhost", 5702), new Address(
                "localhost", 5701), new Address("localhost", 5704), new Address("localhost", 5703), null, null};

        assertFalse(isCyclic(oldReplicas, newReplicas));
    }

}
