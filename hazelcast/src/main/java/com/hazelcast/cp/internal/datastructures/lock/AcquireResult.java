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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.cp.lock.FencedLock;

import static com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus.FAILED;
import static com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus.SUCCESSFUL;
import static com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus.WAIT_KEY_ADDED;
import static com.hazelcast.cp.lock.FencedLock.INVALID_FENCE;

/**
 * Represents result of a lock() call
 */
public class AcquireResult {

    public enum AcquireStatus {
        /**
         * Denotes that a lock() call has successfully acquired the lock
         */
        SUCCESSFUL,

        /**
         * Denotes that a wait key is added to the wait queue for a lock() call
         */
        WAIT_KEY_ADDED,

        /**
         * Denotes that a lock() call has not acquired the lock, either because
         * the lock is held by someone else or the lock acquire limit is
         * reached.
         */
        FAILED
    }

    private final AcquireStatus status;

    /**
     * If the lock() call is successful, the new fencing token is provided.
     * Otherwise, it is {@link FencedLock#INVALID_FENCE}.
     */
    private final long fence;

    /**
     * If new a lock() or unlock() call is sent while there is already a wait
     * key of a previous lock() call, the wait key is cancelled. It is because
     * a lock endpoint is a single-threaded entity and a new call implies that
     * the endpoint is no longer interested in its previous lock() call.
     */
    private final LockInvocationKey cancelledWaitKey;

    AcquireResult(AcquireStatus status, long fence, LockInvocationKey cancelledWaitKey) {
        this.status = status;
        this.fence = fence;
        this.cancelledWaitKey = cancelledWaitKey;
    }

    static AcquireResult acquired(long fence) {
        return new AcquireResult(SUCCESSFUL, fence, null);
    }

    static AcquireResult failed(LockInvocationKey cancelledWaitKey) {
        return new AcquireResult(FAILED, INVALID_FENCE, cancelledWaitKey);
    }

    static AcquireResult waitKeyAdded(LockInvocationKey cancelledWaitKey) {
        return new AcquireResult(WAIT_KEY_ADDED, INVALID_FENCE, cancelledWaitKey);
    }

    public AcquireStatus status() {
        return status;
    }

    public long fence() {
        return fence;
    }

    LockInvocationKey cancelledWaitKey() {
        return cancelledWaitKey;
    }

    @Override
    public String toString() {
        return "AcquireResult{" + "status=" + status + ", fence=" + fence + ", cancelledWaitKey=" + cancelledWaitKey + '}';
    }
}
