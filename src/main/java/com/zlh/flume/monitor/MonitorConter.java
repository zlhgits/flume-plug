package com.zlh.flume.monitor;

import org.apache.flume.instrumentation.MonitoredCounterGroup;

import java.util.concurrent.TimeUnit;

/**
 * 监控source的吞吐量
 * Author zlh
 * Date 2018-11-29
 * Version 1.0
 */
public class MonitorConter extends MonitoredCounterGroup implements MonitorConterMBean {
    private long startProcessTime;

    private static final String AVG_THROUGHPUT = "avg_throughput";
    private static final String CURRENT_THROUGHPUT = "current_throughput";
    private static final String MAX_THROUGHPUT = "max_throughput";
    private static final String EVENT_COUNT = "count_event";
    private static final Type[] ENUM_TYPE = {Type.SOURCE,Type.SINK};

    private static final String[] ATTRS = {AVG_THROUGHPUT, CURRENT_THROUGHPUT, MAX_THROUGHPUT, EVENT_COUNT};

    /**
     *构造函数
     * @param name
     * @param type 0-SOURCE ,1-SINK
     */
    public MonitorConter(String name,int type) {
        super ( ENUM_TYPE[type], name, ATTRS );
    }

    public long getAvgThroughput() {
        return get ( AVG_THROUGHPUT );
    }

    public long getCurrentThroughput() {
        return get ( CURRENT_THROUGHPUT );
    }

    public long getMaxThroughput() {
        return get ( MAX_THROUGHPUT );
    }

    public long getCountEvent() {
        return get ( EVENT_COUNT );
    }

    public void ascendingCountEvent(int value) {
        addAndGet ( EVENT_COUNT,value );
    }

    public void startProcessTime(){
        startProcessTime = System.currentTimeMillis();
    }

    /**
     * 单次process吞吐量
     * @param events
     */
    public void endProcess(int events){

        //计算程序运行时间和生产时间
        long runningTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - getStartTime());
        long processTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startProcessTime);
        long throughput_rate = 0L;

        if (events > 0 && processTime > 0) {
            throughput_rate = events / processTime;
        }
        if (getMaxThroughput() < throughput_rate) {
            set ( MAX_THROUGHPUT, throughput_rate );
        }

        if (runningTime > 0 && getCountEvent() > 0) {
            set ( AVG_THROUGHPUT, (getCountEvent () / runningTime) );
        }
        set(CURRENT_THROUGHPUT,throughput_rate);
    }
}
