package com.intel.hibench.common.streaming.metrics;

import java.io.Serializable;

public interface LatencyReporter extends Serializable {
    void report(long startTime, long endTime);
}
