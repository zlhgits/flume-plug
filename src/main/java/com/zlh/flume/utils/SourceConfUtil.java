package com.zlh.flume.utils;

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.json.simple.parser.ParseException.ERROR_UNEXPECTED_EXCEPTION;

/**
 * 数据采集参数准备及维护
 * Author zlh
 * Date 2018-12-03
 * Version 1.0
 */
public class SourceConfUtil {
    private static final Logger LOG = LoggerFactory.getLogger ( SourceConfUtil.class );

    private File file;
    private File dir;
    private int timeDelay;
    private int batchSize;
    private int maxRow;
    private String startId;
    private String signId;
    private String signFilePath;
    private String signFileName;
    private String url,user,passwd;
    private String table;
    private String signField;
    private String sourceQuery;
    private String query;
    private String sourceName;

    private Map<String, String> SignFileJsonMap;
    private Map<String, String> dbMaps = new LinkedHashMap<String, String>();

    public SourceConfUtil(Context context, String sourceName) {
        this.table = context.getString ( "s.table" );

        //非必须
        this.batchSize = context.getInteger ( "batch.size", Integer.valueOf ( 100 ) ).intValue ();
        this.maxRow = context.getInteger ( "max.row", Integer.valueOf ( 10000 ) ).intValue ();
        this.signField = context.getString ( "s.field", "id" );
        this.signFilePath = context.getString ( "s.path", "/var/lib/flume" );
        this.signFileName = context.getString ( "s.file" ,this.table);
        this.timeDelay = context.getInteger ( "s.delay", Integer.valueOf ( 10000 ) ).intValue ();
        String qy = "select t."+ signField + " as _src_default_id,t.* from "+table+" t where t."+ signField +" > $@$ order by t."+ signField +" asc limit "+maxRow;
        this.sourceQuery = context.getString ( "s.query" ,qy);
        this.startId = context.getString ( "start.id", "0" );
        this.dir = new File ( this.signFilePath );

        //JDBC四要素url, user, passwd     ,driver
        this.url = context.getString ( "db.url" );
        this.user = context.getString ( "db.user" );
        this.passwd = context.getString ( "db.passwd" );
        this.dbMaps.put ( "url",url );
        this.dbMaps.put ( "user",user );
        this.dbMaps.put ( "passwd",passwd );
        this.dbMaps.put ( "driver", CommUtil.getDriver ( url ) );

        this.sourceName = sourceName;
        this.SignFileJsonMap = new LinkedHashMap ();
        //检查必要参数
        checkPrerequisite ();

        if (!this.dir.exists ()) {
            this.dir.mkdir ();
        }

        this.file = new File ( this.signFilePath + "/" + this.signFileName );

        if (isExistFile ()) {
            LOG.debug ( "标记文件存在", this.file.getPath () );
            this.signId = getSignFileId ( this.startId );
        } else {
            LOG.debug ( "标记文件不存在", this.file.getPath () );
            this.signId = this.startId;
            createFile ();
        }
        this.query = getQuery ();
    }

    /**
     * 获取query
     *
     * @return
     */
    public String getQuery() {
        //TODO debug
        LOG.error ( "查询语句："+sourceQuery +"--signId-"+signId +"---"+ startId);
        if (sourceQuery.contains ( "$@$" )) {
            return sourceQuery.replace ( "$@$", signId );
        } else {
            return sourceQuery;
        }
    }

    /**
     * 获取标记状态值
     *
     * @param startId
     * @return
     */
    private String getSignFileId(String startId) {
        if (isExistFile ()) {
            try {
                FileReader fileReader = new FileReader ( file );

                JSONParser jsonParser = new JSONParser ();
                SignFileJsonMap = (Map<String, String>) jsonParser.parse ( fileReader );
                checkSignValues ();
                return SignFileJsonMap.get ( "signId" );

            } catch (Exception e) {
                LOG.error ( "读取文件异常，备份文件并创建新的标记文件", e );
                file.renameTo ( new File ( signFilePath + "/" + signFileName + ".bak."
                        + System.currentTimeMillis () ) );
                return startId;
            }

        } else {
            LOG.debug ( "没有文件，从配置起始值开始" );
            return startId;
        }
    }

    /**
     * 检查文件的标记值
     *
     * @throws ParseException
     */
    private void checkSignValues() throws ParseException {
        // 检查持久化参数
        if (!SignFileJsonMap.containsKey ( "sName" )
                || !SignFileJsonMap.containsKey ( "url" )
                || !SignFileJsonMap.containsKey ( "signId" )) {
            LOG.error ( "文件不完整" );
            throw new ParseException ( ERROR_UNEXPECTED_EXCEPTION );
        }
        if (!SignFileJsonMap.get ( "url" ).equals ( url )) {
            LOG.error ( "文件中的url与配置属性不匹配" );
            throw new ParseException ( ERROR_UNEXPECTED_EXCEPTION );
        } else if (!SignFileJsonMap.get ( "sName" ).equals ( sourceName )) {
            LOG.error ( "文件中的sourceName与配置属性不匹配" );
            throw new ParseException ( ERROR_UNEXPECTED_EXCEPTION );
        }

        // query字段检查
        if (query != null) {
            if (!SignFileJsonMap.containsKey ( "query" )) {
                LOG.error ( "不存在query字段" );
                throw new ParseException ( ERROR_UNEXPECTED_EXCEPTION );
            }
            if (!SignFileJsonMap.get ( "query" ).equals ( sourceQuery )) {
                LOG.error ( "文件中的query与配置属性不匹配" );
                throw new ParseException ( ERROR_UNEXPECTED_EXCEPTION );
            }
        }
    }

    /**
     * 创建标记记录文件
     */
    public void createFile() {
        SignFileJsonMap.put ( "sName", sourceName );
        SignFileJsonMap.put ( "url", url );
        SignFileJsonMap.put ( "signId", signId );

        if (query != null) {
            SignFileJsonMap.put ( "query", sourceQuery );
        } else {
            SignFileJsonMap.put ( "table", table );
        }

        try {
            // 创建一个写文件
            Writer out = new FileWriter ( file, false );
            JSONValue.writeJSONString ( SignFileJsonMap, out );
            out.close ();
        } catch (IOException e) {
            LOG.error ( "创建标记文件异常", e );
        }
    }

    /**
     * 检查状态文件是否存在
     *
     * @return
     */
    private boolean isExistFile() {
        return file.exists () && !file.isDirectory () ? true : false;
    }

    /**
     * 检查必要参数
     */
    private void checkPrerequisite() {
        if (this.url == null) {
            throw new ConfigurationException ( "db.url参数未设置" );
        }
        if (this.signFileName == null) {
            throw new ConfigurationException ( "s.file参数未设置" );
        }
        if ((this.table == null) && (this.sourceQuery == null))
            throw new ConfigurationException ( "s.table参数未设置" );
    }

    /**
     * 获取db四要素
     *
     * @return
     */
    public Map<String, String> getdbMaps() {
        return dbMaps;
    }

    /**
     * @return 批量
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * @return 最大行数
     */
    public int getMaxRow() {
        return maxRow;
    }

    /**
     * @return 延迟时间
     */
    public long getTimeDelay() {
        return timeDelay;
    }

    /**
     * 更新标记
     */
    public void updateSignFile() {
        //TODO debug
        LOG.error ( "当前标记值是：" + signId );
        this.SignFileJsonMap.put ( "signId", signId );
        try {
            Writer out = new FileWriter ( file, false );
            JSONValue.writeJSONString ( SignFileJsonMap, out );
            out.close ();
        } catch (IOException e) {
            LOG.error ( "写入文件异常", e );
        }
    }

    /**
     * @return 表名
     */
    public String getTable() {
        return table;
    }

    /**
     * 标记增量字段
     * @param signId
     */
    public void setSignId(String signId) {
        this.signId = signId;
    }
    public String getSignId() {
        return signId;
    }
}
