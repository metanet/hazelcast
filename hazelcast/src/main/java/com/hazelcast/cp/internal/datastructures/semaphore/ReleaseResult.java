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

package com.hazelcast.cp.internal.datastructures.semaphore;

import java.util.Collection;
import java.util.Collections;

import static java.util.Collections.unmodifiableCollection;

/**
 * Represents result of an ISemaphore.release() call
 */
final class ReleaseResult {

    /**
     * true if the release() call is successful
     */
    private final boolean success;

    /**
     * If the release() call is successful and permits are assigned to some
     * other endpoints, contains their wait keys.
     */
    private final Collection<SemaphoreInvocationKey> acquiredWaitKeys;

    /**
     * Cancelled wait key of the caller if there is any, independent of
     * the release() call is successful or not.
     */
    private final SemaphoreInvocationKey cancelledWaitKey;

    private ReleaseResult(boolean success, Collection<SemaphoreInvocationKey> acquiredWaitKeys,
                          SemaphoreInvocationKey cancelledWaitKey) {
        this.success = success;
        this.acquiredWaitKeys = unmodifiableCollection(acquiredWaitKeys);
        this.cancelledWaitKey = cancelledWaitKey;
    }

    static ReleaseResult successful(Collection<SemaphoreInvocationKey> acquiredWaitKeys,
                                    SemaphoreInvocationKey cancelledWaitKey) {
        return new ReleaseResult(true, acquiredWaitKeys, cancelledWaitKey);
    }

    static ReleaseResult failed(SemaphoreInvocationKey cancelledWaitKey) {
        return new ReleaseResult(false, Collections.<SemaphoreInvocationKey>emptyList(), cancelledWaitKey);
    }

    public boolean success() {
        return success;
    }

    public Collection<SemaphoreInvocationKey> acquiredWaitKeys() {
        return acquiredWaitKeys;
    }

    public SemaphoreInvocationKey cancelledWaitKey() {
        return cancelledWaitKey;
    }
}
