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

package com.hazelcast.cp;

import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.collection.ISet;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.session.CPSession;
import com.hazelcast.cp.session.CPSessionManagementService;

/**
 * CP Subsystem is a component of Hazelcast that builds a strongly consistent
 * layer for a set of distributed data structures. Its APIs can be used for
 * implementing distributed coordination use cases, such as leader election,
 * distributed locking, synchronization, and metadata management.
 * It is accessed via {@link HazelcastInstance#getCPSubsystem()}. Its data
 * structures are CP with respect to the CAP principle, i.e., they always
 * maintain linearizability and prefer consistency over availability during
 * network partitions. Besides network partitions, CP Subsystem withstands
 * server and client failures.
 * <p>
 * Currently, CP Subsystem contains only the implementations of Hazelcast's
 * concurrency APIs. Since these APIs do not maintain large states, all members
 * of a Hazelcast cluster do not necessarily take part in CP Subsystem.
 * The number of Hazelcast members that takes part in CP Subsystem is specified
 * with {@link CPSubsystemConfig#setCPMemberCount(int)}. Say that it is
 * configured as C. Then, when a Hazelcast cluster starts, the first C members
 * form CP Subsystem. These members are called {@link CPMember}s, and they can
 * also contain data for other regular -AP- Hazelcast data structures, such as
 * {@link IMap}, {@link ISet}, etc.
 * <p>
 * Data structures in CP Subsystem run in {@link CPGroup}s. Each CP group
 * elects its own Raft leader and runs the Raft consensus algorithm
 * independently. CP Subsystem runs 2 CP groups by default. The first one is
 * the METADATA CP group which is an internal CP group responsible for managing
 * CP members and CP groups. It is initialized during cluster startup if CP
 * Subsystem is enabled via {@link CPSubsystemConfig#setCPMemberCount(int)}.
 * The second CP group is the DEFAULT CP group, whose name is given in
 * {@link CPGroup#DEFAULT_GROUP_NAME}. If a group name is not specified while
 * creating a CP data structure proxy, that data structure is mapped to
 * the DEFAULT CP group. For instance, when a CP {@link IAtomicLong} instance
 * is created via {@code .getAtomicLong("myAtomicLong")}, it is initialized on
 * the DEFAULT CP group. Besides these 2 pre-defined CP groups, custom CP
 * groups can be created at run-time while fetching CP data structure proxies.
 * For instance, if a CP {@link IAtomicLong} is created by calling
 * {@code .getAtomicLong("myAtomicLong@myGroup")}, first a new CP group is
 * created with the name "myGroup" and then "myAtomicLong" is initialized on
 * this custom CP group.
 * <p>
 * This design implies that each CP member can participate to more than one CP
 * group. CP Subsystem runs a periodic background task to ensure that each CP
 * member performs the Raft leadership role for roughly equal number of CP
 * groups. For instance, if there are 3 CP members and 3 CP groups, each CP
 * member becomes Raft leader for only 1 CP group. If one more CP group is
 * created, then one of the CP members gets the Raft leader role for 2 CP
 * groups.
 * <p>
 * CP member count of CP groups are specified via
 * {@link CPSubsystemConfig#setGroupSize(int)}. Please note that this
 * configuration does not have to be same with the CP member count. Namely,
 * number of CP members in CP Subsystem can be larger than the configured
 * CP group size. CP groups usually consist of an odd number of CP members
 * between 3 and 7. Operations are committed & executed only after they are
 * successfully replicated to the majority of CP members in a CP group.
 * An odd number of CP members is more advantageous to an even number because
 * of the quorum or majority calculations. For a CP group of N members,
 * majority is calculated as N / 2 + 1. For instance, in a CP group of 5 CP
 * members, operations are committed when they are replicated to at least 3 CP
 * members. This CP group can tolerate failure of 2 CP members and remain
 * available. However, if we run a CP group with 6 CP members, it can still
 * tolerate failure of 2 CP members because majority of 6 is 4. Therefore,
 * it does not improve the degree of fault tolerance compared to 5 CP members.
 * <p>
 * CP Subsystem is horizontally scalable thanks to all of the aforementioned CP
 * group management capabilities. You can scale out the throughput and memory
 * capacity by distributing your CP data structures to multiple CP groups
 * (i.e., manual partitioning/sharding) and distributing those CP groups over
 * CP members (i.e., choosing a CP group size that is smaller than the CP
 * member count configuration). Nevertheless, the current set of CP data
 * structures have quite low memory overheads. Moreover, related to the Raft
 * consensus algorithm, each CP group makes uses of internal heartbeat RPCs
 * to maintain authority of the Raft leader and help lagging CP group members
 * to make progress. Last but not least, the new CP lock and semaphore
 * implementations rely on a brand new session mechanism. In a nutshell,
 * a Hazelcast server or a client starts a new session on the corresponding
 * CP group when it makes its very first lock or semaphore acquire request,
 * and then periodically commits session heartbeats to this CP group in order
 * to indicate its liveliness. It means that if CP locks and semaphores are
 * distributed into multiple CP groups, there will be a session management
 * overhead on each CP group. Please see {@link CPSession} for more details.
 * For these reasons, we recommend developers to use a minimal number of CP
 * groups. For most use cases, the DEFAULT CP group should be sufficient to
 * maintain all CP data structure instances. Custom CP groups is recommended
 * only when you benchmark your deployment and decide that performance of
 * the DEFAULT CP group is not sufficient for your workload.
 * <p>
 * The CP subsystem runs a discovery process in the background on cluster
 * startup. When it is enabled by setting a positive value to
 * {@link CPSubsystemConfig#setCPMemberCount(int)}, say N, the first N members
 * in the Hazelcast cluster member list initiate the CP discovery process.
 * Other Hazelcast members skip this step. The CP subsystem discovery process
 * runs out of the box on top of Hazelcast's cluster member list without
 * requiring any custom configuration for different environments. It is
 * completed when each one of the first N Hazelcast members initializes its
 * local CP member list and commits it to the METADATA CP group.
 * <strong>A soon-to-be CP member terminates itself if any of the following
 * conditions occur before the CP discovery process is completed:</strong>
 * <ul>
 * <li>Any Hazelcast member leaves the cluster,</li>
 * <li>The local Hazelcast member commits a CP member list which is different
 * from other members' committed CP member lists,</li>
 * <li>The local Hazelcast member list fails to commit its discovered CP member
 * list for any reason.</li>
 * </ul>
 * <p>
 * <strong>The CP data structure proxies differ from the other Hazelcast data
 * structure proxies in two aspects. First, an internal commit is performed on
 * the METADATA CP group every time you fetch a proxy from this interface.
 * Hence, callers should cache returned proxies. Second, if you call
 * {@link DistributedObject#destroy()} on a CP data structure proxy, that data
 * structure is terminated on the underlying CP group and cannot be
 * reinitialized until the CP group is force-destroyed via
 * {@link CPSubsystemManagementService#forceDestroyCPGroup(String)}. For this
 * reason, please make sure that you are completely done with a CP data
 * structure before destroying its proxy.</strong>
 * <p>
 * By default, CP Subsystem works only in memory without persisting any state
 * to disk. It means that a crashed CP member will not be able to recover by
 * reloading its previous state. Therefore, crashed CP members create a danger
 * for gradually losing majority of CP groups and eventually cause the total
 * loss of availability of CP Subsystem. To prevent such situations, failed
 * CP members can be removed from CP Subsystem and replaced in CP groups with
 * other available CP members. This flexibility provides a good degree of
 * fault-tolerance at run-time. Please see {@link CPSubsystemConfig} and
 * {@link CPSubsystemManagementService} for more details.
 * <p>
 * CP Subsystem offers disk persistence as well. When it is enabled via
 * {@link CPSubsystemConfig#setPersistenceEnabled(boolean)}, CP members persist
 * their local state to stable storage and can restore their state after
 * crashes. This capability significantly improves the overall reliability of
 * CP Subsystem by enabling recovery of crashed CP members. You can restart
 * crashed CP members and they will restore their local state and resume
 * working as if they have never crashed. If you cannot restart a CP member
 * on the same machine, you can move its persistence data to another machine
 * and restart your CP member with a new address. CP Subsystem Persistence
 * enables you to handle single or multiple CP member crashes, or even whole
 * cluster crashes and guarantee that committed operations are not lost after
 * recovery. As long as majority of CP members are available after recovery,
 * CP Subsystem remains operational.
 * <p>
 * When CP Subsystem Persistence is enabled, all Hazelcast cluster members
 * create a sub-directory under the base persistence directory which is
 * specified via {@link CPSubsystemConfig#getBaseDir()}. This means that AP
 * Hazelcast members, which are the ones not marked as CP members during
 * the CP Subsystem discovery process, create their persistence directories
 * as well. Those members persist only the information that they are not CP
 * members. This is done because when a Hazelcast member starts with CP
 * Subsystem Persistence enabled, it checks if there is a CP persistence
 * directory belonging to itself. If it founds one, it skips the CP Subsystem
 * discovery process and initializes its CP member identity from the persisted
 * information. If it was an AP member before shutdown or crash, it will
 * restore this information and start as an AP member. Otherwise, it could
 * think that the CP Subsystem discovery process is not executed and initiate
 * it, which would break CP Subsystem.
 * <p>
 * <strong> In light of this information, If you have both CP and AP members
 * when CP Subsystem Persistence is enabled, and if you want to perform
 * a cluster-wide restart, you need to ensure that AP members are also
 * restarted with their persistence directories.</strong>
 * <p>
 * There is a significant behavioral difference during CP member shutdown when
 * CP Subsystem Persistence is enabled and disabled. When disabled (the default
 * mode in which CP Subsystem works only in memory), a shutting down CP member
 * is replaced with other available CP members in all of its CP groups in order
 * not to decrease or more importantly not to lose majorities of CP groups.
 * It is because CP members keep their local state only in memory when CP
 * Subsystem Persistence is disabled (the default mode) and a shut-down CP
 * member cannot join back with its CP identity and state, hence it is better
 * to remove it from its CP groups to not to damage availability. If there is
 * no other available CP member to replace a shutting down CP member in a CP
 * group, that CP group's size is reduced by 1 and its majority value is
 * recalculated. On the other hand, when CP Subsystem Persistence is enabled,
 * a shutting-down CP Member can come back with its local CP state. Therefore,
 * it is not automatically removed from its CP groups when CP Subsystem
 * Persistence is enabled. It is up to the user to remove shut-down CP members
 * via {@link CPSubsystemManagementService#removeCPMember(String)} if they will
 * not come back.
 * <p>
 * In summary, CP member shutdown behaviour is as follows:
 * <ul>
 * <li>When CP Subsystem Persistence is disabled (the default mode),
 * shutting-down CP members are removed from CP groups and CP group majority
 * values are recalculated.</li>
 * <li>When CP Subsystem Persistence is enabled, shutting-down CP members are
 * still kept in CP groups so they will be part of CP group majority
 * calculations.</li>
 * </ul>
 * <p>
 * CP Subsystem's fault tolerance capabilities are summarized below.
 * For the sake of simplicity, let's assume that both the CP member count and
 * CP group size configurations are configured as N and we use only
 * the DEFAULT CP group. <strong>In the bullet list below, "a permanent crash"
 * means that a CP member either crashes while CP Subsystem Persistence is
 * disabled, hence it cannot be recovered with its CP identity and data, or
 * it crashes while CP Subsystem Persistence is enabled but its CP data cannot
 * be recovered, for instance, due to a total server crash or a disk failure.
 * </strong>
 * <p>
 * <ul>
 * <li>
 * CP Subsystem can tolerate failure of the minority of CP members (less than
 * N / 2 + 1). If N / 2 + 1 or more CP members permanently crash, CP Subsystem
 * totally loses its availability and requires a force-reset via
 * {@link CPSubsystemManagementService#restart()}.
 * </li>
 * <li>
 * If a CP member leaves the Hazelcast cluster, it is not automatically removed
 * from CP Subsystem because CP Subsystem may not certainly determine if that
 * member has really crashed or just partitioned away. Therefore, absent CP
 * members are still considered in majority calculations and cause a danger for
 * the availability of CP Subsystem. If the user knows for sure that an absent
 * CP member is crashed, she can remove that CP member from CP Subsystem via
 * {@link CPSubsystemManagementService#removeCPMember(String)}. This API call
 * removes the given CP member from all CP groups and updates their majority
 * calculations. If there is another available CP member in CP Subsystem, the
 * removed CP member is replaced with that one, or the user can promote another
 * member of the Hazelcast cluster to the CP role via
 * {@link CPSubsystemManagementService#promoteToCPMember()} to replace
 * the crashed CP member.
 * </li>
 * <li>
 * There might be a small window of unavailability even if the majority of CP
 * members is still online after a CP member crash. For instance, if
 * the crashed CP member is the Raft leader for some of the CP groups, those
 * groups run a new round of leader election to elect a new leader among
 * the remaining CP group members. CP Subsystem API calls that internally hit
 * those CP groups are retried until they have a new Raft leader. If the failed
 * CP member has the Raft follower role, it causes a very minimal disruption
 * since Raft leaders are still able to replicate and commit operations with
 * the majority of their CP group members.
 * </li>
 * <li>
 * If a crashed CP member is restarted after it is removed from CP Subsystem,
 * its behaviour depends on if CP Subsystem Persistence is enabled or disabled.
 * If CP Subsystem Persistence is enabled, the restarted CP member is not
 * able to restore its CP data from disk because it notices that it is no
 * longer a CP member. Because of that, it fails its startup process and prints
 * an error message. The only thing to do in this case is manually delete CP
 * persistence directory because the data stored is no longer useful. On
 * the other hand, if CP Subsystem Persistence is disabled, this member cannot
 * remember anything related to its previous CP identity, hence it restarts
 * as a new AP member.
 * </li>
 * <li>
 * If a network partition occurs, behaviour of CP Subsystem depends on how
 * CP members are divided in different sides of the network partition and
 * to which sides Hazelcast clients are connected. Each CP group remains
 * available on the side that contains the majority of its CP members. If
 * a Raft leader falls into the minority side, its CP group elects a new Raft
 * leader on the other side and Hazelcast clients that are talking to
 * the majority side continues to make API calls on CP Subsystem. However,
 * Hazelcast clients that are talking to the minority side fail with operation
 * timeouts. When the network problem is resolved, CP members reconnect to each
 * other and CP groups continue their operation normally.
 * </li>
 * </ul>
 * @see CPSubsystemConfig
 * @see CPMember
 * @see CPSession
 */
public interface CPSubsystem {

    /**
     * Returns a proxy for an {@link IAtomicLong} instance created on the CP
     * subsystem. Hazelcast's {@link IAtomicLong} is a distributed version of
     * <code>java.util.concurrent.atomic.AtomicLong</code>. If no group name is
     * given within the "name" parameter, then the {@link IAtomicLong} instance
     * will be created on the DEFAULT CP group. If a group name is given, like
     * {@code .getAtomicLong("myLong@group1")}, the given group will be
     * initialized first, if not initialized already, and then the
     * {@link IAtomicLong} instance will be created on this group. Returned
     * {@link IAtomicLong} instance offers linearizability and behaves as a CP
     * register. When a network partition occurs, proxies that exist on the
     * minority side of its CP group lose availability.
     * <p>
     * <strong>Each call of this method performs a commit to the Metadata CP
     * group. Hence, callers should cache the returned proxy.</strong>
     *
     * @param name name of the {@link IAtomicLong} proxy
     * @return {@link IAtomicLong} proxy for the given name
     * @throws HazelcastException if the CP subsystem is not enabled
     */
    IAtomicLong getAtomicLong(String name);

    /**
     * Returns a proxy for an {@link IAtomicReference} instance created on
     * the CP subsystem. Hazelcast's {@link IAtomicReference} is a distributed
     * version of <code>java.util.concurrent.atomic.AtomicLong</code>. If no group
     * name is given within the "name" parameter, then
     * the {@link IAtomicReference} instance will be created on the DEFAULT CP
     * group. If a group name is given, like
     * {@code .getAtomicReference("myRef@group1")}, the given group will be
     * initialized first, if not initialized already, and then the
     * {@link IAtomicReference} instance will be created on this group.
     * Returned {@link IAtomicReference} instance offers linearizability and
     * behaves as a CP register. When a network partition occurs, proxies that
     * exist on the minority side of its CP group lose availability.
     * <p>
     * <strong>Each call of this method performs a commit to the Metadata CP
     * group. Hence, callers should cache the returned proxy.</strong>
     *
     * @param name name of the {@link IAtomicReference} proxy
     * @return {@link IAtomicReference} proxy for the given name
     * @throws HazelcastException if the CP subsystem is not enabled
     */
    <E> IAtomicReference<E> getAtomicReference(String name);

    /**
     * Returns a proxy for an {@link ICountDownLatch} instance created on
     * the CP subsystem. Hazelcast's {@link ICountDownLatch} is a distributed
     * version of <code>java.util.concurrent.CountDownLatch</code>. If no group
     * name is given within the "name" parameter, then
     * the {@link ICountDownLatch} instance will be created on the DEFAULT CP
     * group. If a group name is given, like
     * {@code .getCountDownLatch("myLatch@group1")}, the given group will be
     * initialized first, if not initialized already, and then the
     * {@link ICountDownLatch} instance will be created on this group. Returned
     * {@link ICountDownLatch} instance offers linearizability. When a network
     * partition occurs, proxies that exist on the minority side of its CP
     * group lose availability.
     * <p>
     * <strong>Each call of this method performs a commit to the Metadata CP
     * group. Hence, callers should cache the returned proxy.</strong>
     *
     * @param name name of the {@link ICountDownLatch} proxy
     * @return {@link ICountDownLatch} proxy for the given name
     * @throws HazelcastException if the CP subsystem is not enabled
     */
    ICountDownLatch getCountDownLatch(String name);

    /**
     * Returns a proxy for an {@link FencedLock} instance created on the CP
     * subsystem. Hazelcast's {@link FencedLock} is a distributed version of
     * <code>java.util.concurrent.locks.Lock</code>. If no group name is given
     * within the "name" parameter, then the {@link FencedLock} instance will
     * be created on the DEFAULT CP group. If a group name is given, like
     * {@code .getLock("myLock@group1")}, the given group will be initialized
     * first, if not initialized already, and then the {@link FencedLock}
     * instance will be created on this group. Returned {@link FencedLock}
     * instance offers linearizability. When a network partition occurs,
     * proxies that exist on the minority side of its CP group lose
     * availability.
     * <p>
     * <strong>Each call of this method performs a commit to the Metadata CP
     * group. Hence, callers should cache the returned proxy.</strong>
     *
     * @see FencedLockConfig
     *
     * @param name name of the {@link FencedLock} proxy
     * @return {@link FencedLock} proxy for the given name
     * @throws HazelcastException if the CP subsystem is not enabled
     */
    FencedLock getLock(String name);

    /**
     * Returns a proxy for an {@link ISemaphore} instance created on the CP
     * subsystem. Hazelcast's {@link ISemaphore} is a distributed version of
     * <code>java.util.concurrent.Semaphore</code>. If no group name is given
     * within the "name" parameter, then the {@link ISemaphore} instance will
     * be created on the DEFAULT CP group. If a group name is given, like
     * {@code .getSemaphore("mySemaphore@group1")}, the given group will be
     * initialized first, if not initialized already, and then the
     * {@link ISemaphore} instance will be created on this group. Returned
     * {@link ISemaphore} instance offers linearizability. When a network
     * partition occurs, proxies that exist on the minority side of its CP
     * group lose availability.
     * <p>
     * <strong>Each call of this method performs a commit to the Metadata CP
     * group. Hence, callers should cache the returned proxy.</strong>
     *
     * @see SemaphoreConfig
     *
     * @param name name of the {@link ISemaphore} proxy
     * @return {@link ISemaphore} proxy for the given name
     * @throws HazelcastException if the CP subsystem is not enabled
     */
    ISemaphore getSemaphore(String name);

    /**
     * Returns the local CP member if this Hazelcast member is part of
     * the CP subsystem, returns null otherwise.
     * <p>
     * This method is a shortcut for {@link CPSubsystemManagementService#getLocalCPMember()}
     * method. Calling this method is equivalent to calling
     * <code>getCPSubsystemManagementService().getLocalCPMember()</code>.
     *
     * @return local CP member if available, null otherwise
     * @throws HazelcastException if the CP subsystem is not enabled
     * @see CPSubsystemManagementService#getLocalCPMember()
     */
    CPMember getLocalCPMember();

    /**
     * Returns the {@link CPSubsystemManagementService} of this Hazelcast
     * instance. {@link CPSubsystemManagementService} offers APIs for managing
     * CP members and CP groups.
     *
     * @return the {@link CPSubsystemManagementService} of this Hazelcast instance
     */
    CPSubsystemManagementService getCPSubsystemManagementService();

    /**
     * Returns the {@link CPSessionManagementService} of this Hazelcast
     * instance. {@link CPSessionManagementService} offers APIs for managing CP
     * sessions.
     *
     * @return the {@link CPSessionManagementService} of this Hazelcast instance
     */
    CPSessionManagementService getCPSessionManagementService();

}
