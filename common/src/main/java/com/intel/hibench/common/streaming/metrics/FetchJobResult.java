package com.intel.hibench.common.streaming.metrics;

public class FetchJobResult {

    Long minTime;
    Long maxTime;
    Long count;

    public FetchJobResult() {
        this(Long.MAX_VALUE, Long.MIN_VALUE, 0L);
    }

    public FetchJobResult(Long minTime, Long maxTime, Long count) {
        this.count = count;
        this.minTime = minTime;
        this.maxTime = maxTime;
    }

    public Long getMinTime() {
        return minTime;
    }

    public Long getMaxTime() {
        return maxTime;
    }

    public Long getCount() {
        return count;
    }

    public void update(Long startTime, Long endTime) {
        count++;
        this.minTime = Math.min(minTime, startTime);
        this.maxTime = Math.max(maxTime, endTime);
    }
}
