package com.zlh.flume.monitor;

/**
 * 监控吞吐量
 * Author zlh
 * Date 2018-11-29
 * Version 1.0
 */
public interface MonitorConterMBean {
    public long getAvgThroughput();
    public long getCurrentThroughput();
    public long getMaxThroughput();

    public long getCountEvent();
    public void ascendingCountEvent(int value);
}
