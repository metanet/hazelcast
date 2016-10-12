package com.hazelcast.config;

public enum HotRestartClusterStartPolicy {

    FULL_START,

    PARTIAL_START_WITH_MOST_RECENT_DATA,

    PARTIAL_START_WITH_HIGHEST_NUMBER_OF_MEMBERS

}
