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

package com.hazelcast.partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public enum MigrationType {

    MOVE(0), COPY(1), SHIFT_UP(2);

    private final int code;

    MigrationType(int code) {
        this.code = code;
    }

    public static void writeTo(MigrationType type, DataOutput out) throws IOException {
        out.writeByte(type.code);
    }

    public static MigrationType readFrom(DataInput in) throws IOException {
        final byte code = in.readByte();
        switch (code) {
            case 0:
                return MOVE;
            case 1:
                return COPY;
            case 2:
                return SHIFT_UP;
        }
        throw new IllegalArgumentException("Code: " + code);
    }
}
