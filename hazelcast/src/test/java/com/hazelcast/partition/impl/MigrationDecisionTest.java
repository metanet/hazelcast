package com.hazelcast.partition.impl;

import com.hazelcast.nio.Address;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MigrationDecisionTest {

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

        assertTrue(isCyclic2(oldReplicas, newReplicas));
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

        assertTrue(isCyclic2(oldReplicas, newReplicas));
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

        assertTrue(isCyclic2(oldReplicas, newReplicas));
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

        assertTrue(isCyclic2(oldReplicas, newReplicas));
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

    private boolean isCyclic2(Address[] oldReplicas, Address[] newReplicas) {
        boolean cyclic = false;
        for (int i = 0; i < oldReplicas.length; i++) {
            final Address oldAddress = oldReplicas[i];
            final Address newAddress = newReplicas[i];

            if (oldAddress == null || newAddress == null || oldAddress.equals(newAddress)) {
                continue;
            }

            if (isCyclic(oldReplicas, newReplicas, i)) {
                fixCycle(oldReplicas, newReplicas, i);
                cyclic = true;
            }
        }

        return cyclic;
    }

    private boolean isCyclic(Address[] oldReplicas, Address[] newReplicas) {
        for (int i = 0; i < oldReplicas.length; i++) {
            final Address oldAddress = oldReplicas[i];
            final Address newAddress = newReplicas[i];

            if (oldAddress == null || newAddress == null || oldAddress.equals(newAddress)) {
                continue;
            }

            if (isCyclic(oldReplicas, newReplicas, i)) {
                return true;
            }
        }

        return false;
    }

    private boolean isCyclic(Address[] oldReplicas, Address[] newReplicas, int index) {
        final Address newOwner = newReplicas[index];
        while (index < newReplicas.length && newReplicas[index] != null) {
            int nextIndex = findIndex(newReplicas, oldReplicas[index]);
            if (nextIndex == -1) {
                return false;
            } else if (newOwner.equals(oldReplicas[nextIndex])) {
                return true;
            } else {
                index = nextIndex;
            }
        }

        return false;
    }

    private void fixCycle(Address[] oldReplicas, Address[] newReplicas, int index) {
        while (index < newReplicas.length && newReplicas[index] != null) {
            int nextIndex = findIndex(newReplicas, oldReplicas[index]);
            newReplicas[index] = oldReplicas[index];
            if (nextIndex == -1) {
                return;
            }
            index = nextIndex;
        }
    }

    private int findIndex(Address[] replicas, Address address) {
        for (int i = 0; i < replicas.length; i++) {
            if (address.equals(replicas[i])) {
                return i;
            }
        }

        return -1;
    }

}
