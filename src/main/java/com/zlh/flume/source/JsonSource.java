package com.zlh.flume.source;

import com.zlh.flume.monitor.MonitorConter;
import com.zlh.flume.utils.CommUtil;
import com.zlh.flume.utils.CommsException;
import com.zlh.flume.utils.DBUtil;
import com.zlh.flume.utils.SourceConfUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author zlh
 * Date 2018-12-03
 * Version 1.0
 */
public class JsonSource extends AbstractSource implements Configurable, PollableSource {

    private Logger LOG = LoggerFactory.getLogger ( JsonSource.class );

    private SourceConfUtil sConfUtil;
    private DBUtil dbUtil;
    private MonitorConter monitorConter;
    private PrintWriter printWriter;

    /**
     * 参数初始化
     * @param context
     */
    public void configure(Context context) {
        this.sConfUtil = new SourceConfUtil ( context,this.getName () );
        this.dbUtil = new DBUtil ( sConfUtil.getdbMaps () );
        this.monitorConter = new MonitorConter ( "SOURCE" + this.getName (),0 );
        this.printWriter = new PrintWriter ( new ChannelToWriter () );
    }

    /**
     * source初始化，在source生命周期只运行一次
     */
    @Override
    public synchronized void start() {
        LOG.info ( "source开始运行",this.getName () );
        try {
            dbUtil.dbStart ();
            monitorConter.start ();
            super.start ();
        } catch (CommsException e) {
            LOG.error ( "source初始化异常，停止运行flume",e );
            this.stop ();
        }
    }

    /**
     * process处理
     * @return
     * @throws EventDeliveryException
     */
    public Status process() throws EventDeliveryException {
        try {
            monitorConter.startProcessTime();

            List<Map<String, Object>> result = dbUtil.getResult(sConfUtil.getQuery ());
            //数据量小，休眠线程
            if (result.size() < sConfUtil.getMaxRow()) {
                try{
                    Thread.sleep(sConfUtil.getTimeDelay());
                }catch (InterruptedException e){
                    LOG.error ( "线程休眠被打断");
                }
            }
            //如果有数据，写入channel
            if (!result.isEmpty()) {
                List<Map<String, Map<String, Object>>> tableResult= CommUtil.addTableResult ( result,sConfUtil );
                CommUtil.writeAllRows(tableResult, printWriter);
                //刷出
                printWriter.flush();

                //累加event
                monitorConter.ascendingCountEvent (result.size());
                //记录增量标记
                sConfUtil.updateSignFile();
            }

            monitorConter.endProcess(result.size());

            return Status.READY;
        } catch (CommsException e) {
            LOG.error("source process异常", e);
            return Status.BACKOFF;
        }
    }

    @Override
    public synchronized void stop() {
        LOG.info ( "source停止运行",this.getName () );
        try{
            dbUtil.dbStop();
            printWriter.close ();
        }catch (Exception e){
            LOG.error ( "source停止异常！",e );
        }finally {
            monitorConter.start ();
            super.stop ();
        }
    }

    /**
     * 重写write
     */
    private class ChannelToWriter extends Writer {
        private List<Event> events;

        private ChannelToWriter() { this.events = new ArrayList (); }

        public void write(char[] cbuf, int off, int len) throws IOException {
            Event event = new SimpleEvent ();
            String str = new String(cbuf);
            event.setBody(str.substring(off, len - 1).getBytes());

            Map<String, String> headers = new HashMap<String, String> ();
            headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
            event.setHeaders(headers);
            this.events.add(event);
            if (this.events.size() >= sConfUtil.getBatchSize()){
                flush();
            }
        }

        public void flush() throws IOException{
            getChannelProcessor().processEventBatch(this.events);
            this.events.clear();
        }

        public void close() throws IOException {
            flush();
        }
    }
    //失败补偿暂停线程处理
    public long getBackOffSleepIncrement() {
        return 1000;
    }

    public long getMaxBackOffSleepInterval() {
        return 5000;
    }
}
