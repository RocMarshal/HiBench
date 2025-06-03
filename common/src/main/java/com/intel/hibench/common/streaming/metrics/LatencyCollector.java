package com.intel.hibench.common.streaming.metrics;

import java.io.Serializable;

public interface LatencyCollector extends Serializable {
    void start() throws Exception;
}
