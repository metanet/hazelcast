package com.hazelcast.partition.impl;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.nio.Address;
import com.sun.istack.internal.NotNull;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MigrationDecisionTest2 {

    @Test
    public void test()
            throws UnknownHostException {

        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5705), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5704), new Address("localhost", 5703), new Address(
                "localhost", 5705), new Address("localhost", 5706), new Address("localhost", 5702), new Address("localhost",
                5701), null};

        migrate(oldAddresses, newAddresses);

    }

    @Test
    public void test2()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5705), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5704), new Address("localhost", 5703), new Address(
                "localhost", 5705), new Address("localhost", 5706), new Address("localhost", 5701), null, null};

        migrate(oldAddresses, newAddresses);
    }

    @Test
    public void testRandom()
            throws UnknownHostException {
        for (int i = 0; i < 100; i++) {
            testRandom(3);
            testRandom(4);
            testRandom(5);
        }
    }

    private static void testRandom(int initialLen) throws java.net.UnknownHostException {
        Address[] oldAddresses = new Address[InternalPartition.MAX_REPLICA_COUNT];
        for (int i = 0; i < initialLen; i++) {
            oldAddresses[i] = newAddress(5000 + i);
        }

        Address[] newAddresses = Arrays.copyOf(oldAddresses, oldAddresses.length);
        int newLen = (int) (Math.random() * (oldAddresses.length - initialLen + 1));
        for (int i = 0; i < newLen; i++) {
            newAddresses[i + initialLen] = newAddress(6000 + i);
        }

        shuffle(newAddresses, initialLen + newLen);

        migrate(oldAddresses, newAddresses);
    }

    private static void shuffle(Address[] array, int len) {
        int index;
        Address temp;
        Random random = new Random();
        for (int i = len - 1; i > 0; i--) {
            index = random.nextInt(i + 1);
            temp = array[index];
            array[index] = array[i];
            array[i] = temp;
        }
    }

    @NotNull
    private static Address newAddress(int port) throws java.net.UnknownHostException {
        return new Address("localhost", port);
    }

    public static void migrate(Address[] oldAddresses, Address[] newAddresses) {

        System.out.println("#############################################");
        Address[] state = new Address[oldAddresses.length];
        System.arraycopy(oldAddresses, 0, state, 0, oldAddresses.length);
        System.out.println("INITIAL STATE: " + Arrays.toString(state));
        System.out.println("FINAL STATE: " + Arrays.toString(newAddresses));
        if (fixCycle(oldAddresses, newAddresses)) {
            System.out.println("cycle fixed");
        }
        System.out.println("EXPECTED STATE: " + Arrays.toString(newAddresses));

        int currentIndex = 0;
        while (currentIndex < oldAddresses.length) {

            if (newAddresses[currentIndex] == null) {
                currentIndex++;
                System.out.println("new address is null at index: " + currentIndex);
            } else if (state[currentIndex] == null) {
                System.out.println("COPY " + newAddresses[currentIndex] + " to index: " + currentIndex);
                state[currentIndex] = newAddresses[currentIndex];
                currentIndex++;
            } else if (newAddresses[currentIndex].equals(state[currentIndex])) {
                System.out.println("Addresses are same on index: " + currentIndex);
                currentIndex++;
            } else if (findIndex(newAddresses, state[currentIndex]) == -1 && findIndex(state, newAddresses[currentIndex]) == -1) {
                System.out.println("MOVE " + newAddresses[currentIndex] + " to index: " + currentIndex);
                state[currentIndex] = newAddresses[currentIndex];
                currentIndex++;
            } else { // IT IS A MOVE COPY BACK
                if (findIndex(state, newAddresses[currentIndex]) == -1) {
                    int keepReplicaIndex = findIndex(newAddresses, state[currentIndex]);
                    System.out.println("MOVE_COPY_BACK " + newAddresses[currentIndex] + " to index: " + currentIndex
                            + " with keepReplicaIndex: " + keepReplicaIndex);

                    state[keepReplicaIndex] = state[currentIndex];
                    state[currentIndex] = newAddresses[currentIndex];
                    currentIndex++;
                } else {
                    Address target = newAddresses[currentIndex];

                    while (true) {
                        int j = findIndex(state, target);

                        if (j == -1) {
                            System.out.println(target + " is not present in " + Arrays.toString(state));
                            fail();
                            break;
                        } else if (newAddresses[j] == null) {
                            System.out.println(
                                    "SHIFT UP " + state[j] + " from old addresses index: " + j + " to index: " + currentIndex);
                            state[currentIndex] = state[j];
                            break;
                        } else if (findIndex(state, newAddresses[j]) == -1) { //
                            System.out.println("MOVE2 " + newAddresses[j] + " to index: " + j);
                            state[j] = newAddresses[j];
                            break;
                        } else { // newAddresses[j] is also present in old partition replicas
                            target = newAddresses[j];
                        }
                    }
                }
            }

            System.out.println("STATE: " + Arrays.toString(state));

            Set<Address> verify = new HashSet<Address>();
            for (Address s : state) {
                if (s != null) {
                    assertTrue(verify.add(s));
                }
            }

        }

        assertArrayEquals(state, newAddresses);
    }

    private static int findIndex(Address[] replicas, Address address) {
        if (address == null) {
            return -1;
        }

        for (int i = 0; i < replicas.length; i++) {
            if (address.equals(replicas[i])) {
                return i;
            }
        }

        return -1;
    }

    static boolean fixCycle(Address[] oldReplicas, Address[] newReplicas) {
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

    private static boolean isCyclic(Address[] oldReplicas, Address[] newReplicas, int index) {
        final Address newOwner = newReplicas[index];
        while (true) {
            int nextIndex = findIndex(newReplicas, oldReplicas[index]);
            if (nextIndex == -1) {
                return false;
            } else if (newOwner.equals(oldReplicas[nextIndex])) {
                return true;
            } else {
                index = nextIndex;
            }
        }
    }

    private static void fixCycle(Address[] oldReplicas, Address[] newReplicas, int index) {
        while (true) {
            int nextIndex = findIndex(newReplicas, oldReplicas[index]);
            newReplicas[index] = oldReplicas[index];
            if (nextIndex == -1) {
                return;
            }
            index = nextIndex;
        }
    }

}
