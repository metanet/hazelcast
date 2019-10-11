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

import static com.hazelcast.cp.internal.datastructures.lock.LockOwnershipState.NOT_LOCKED;

/**
 * Represents result of an unlock() call
 */
class ReleaseResult {

    static final ReleaseResult FAILED = new ReleaseResult(false, NOT_LOCKED, null);

    /**
     * true if the unlock() call is successful
     */
    private final boolean success;

    /**
     * If the unlock() call is successful, represents new state of the lock
     * ownership. It can be {@link LockOwnershipState#NOT_LOCKED} if the lock
     * has no new owner after the release. Similarly, it is
     * {@link LockOwnershipState#NOT_LOCKED} if the unlock() call has failed.
     */
    private final LockOwnershipState ownership;

    /**
     * If the unlock() call is successful and the lock ownership is given to
     * some other endpoint, this field contains the completed wait key of
     * the new lock holder. If the unlock() call has failed, it may contain
     * cancelled wait key of the caller, if there is any.
     */
    private final LockInvocationKey completedWaitKey;

    ReleaseResult(boolean success, LockOwnershipState ownership, LockInvocationKey completedWaitKey) {
        this.success = success;
        this.ownership = ownership;
        this.completedWaitKey = completedWaitKey;
    }

    static ReleaseResult successful(LockOwnershipState ownership) {
        return new ReleaseResult(true, ownership, null);
    }

    static ReleaseResult successful(LockOwnershipState ownership, LockInvocationKey completedWaitKey) {
        return new ReleaseResult(true, ownership, completedWaitKey);
    }

    static ReleaseResult failed(LockInvocationKey completedWaitKey) {
        return new ReleaseResult(false, NOT_LOCKED, completedWaitKey);
    }

    public boolean success() {
        return success;
    }

    public LockOwnershipState ownership() {
        return ownership;
    }

    LockInvocationKey completedWaitKey() {
        return completedWaitKey;
    }

    @Override
    public String toString() {
        return "ReleaseResult{" + "success=" + success + ", ownership=" + ownership + ", completedWaitKey=" + completedWaitKey
                + '}';
    }
}
