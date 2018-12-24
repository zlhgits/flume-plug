package com.zlh.flume.utils;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 公共工具类
 * Author zlh
 * Date 2018-11-29
 * Version 1.0
 */
public class CommUtil {

    private static Logger LOG = LoggerFactory.getLogger ( CommUtil.class );
    public static final SimpleDateFormat DF = new SimpleDateFormat ( "yyyy-MM-dd HH:mm:ss" );

    /**
     * 特殊符号数据还原
     *
     * @param tmpVal
     * @return
     */
    public static String washingVal(String tmpVal) {
        //$@$:source的空字符串
        if (tmpVal.equals ( "$@$" )) {
            tmpVal = "";
        } else {

            if (tmpVal.contains ( "$@@$" )) {
                // 将原数据中的逗号还原
                tmpVal = tmpVal.replace ( "$@@$", "," );
            }
            if (tmpVal.contains ( "$@@@$" )) {
                // 将原数据中的引号还原
                tmpVal = tmpVal.replace ( "$@@@$", "\"" );
            }

        }
        return tmpVal;
    }

    /**
     * ###常用数据库连接URL地址大全###
     * 获取数据库驱动
     *
     * @param url
     * @return
     */
    public static String getDriver(String url) {
        if (url.toUpperCase ().contains ( "JDBC:ORACLE" )) {
            return "oracle.jdbc.driver.OracleDriver";
        } else if (url.toUpperCase ().contains ( "SQLSERVER" )) {
            return "com.microsoft.jdbc.sqlserver.SQLServerDriver";
        } else if (url.toUpperCase ().contains ( "JDBC:MYSQL" )) {
            return "com.mysql.jdbc.Driver";
        } else if (url.toUpperCase ().contains ( "JDBC:DB2" )) {
            return "com.ibm.db2.jdbc.app.DB2Driver";
        } else if (url.toUpperCase ().contains ( "JDBC:POSTGRESQL" )) {
            return "org.postgresql.Driver";
        } else return "PHOENIX";
    }

    /**
     * 将ResultSet转换成 List<Map>
     *
     * @param rs
     * @return
     * @throws SQLException
     */
    public static List<Map<String, Object>> rsToList(ResultSet rs) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>> ();
        //获得列集
        ResultSetMetaData rsmd = rs.getMetaData ();
        int colCount = rsmd.getColumnCount ();
        List<String> colNameList = new ArrayList<String> ();
        for (int i = 0; i < colCount; i++) {
            colNameList.add ( rsmd.getColumnLabel ( i + 1 ) );
        }
        while (rs.next ()) {
            //每行一个map
            Map map = new HashMap<String, Object> ();
            for (int i = 0; i < colCount; i++) {
                String key = colNameList.get ( i );
                Object value = rs.getString ( colNameList.get ( i ) );
                map.put ( key, value );
            }
            results.add ( map );
        }
        return results;
    }

    /**
     * 将每行数据对应一个表名
     * {t1,[{col1:v},{col2:v}]}
     * @param result
     * @param scu
     * @return
     */
    public static List<Map<String, Map<String, Object>>> addTableResult(List<Map<String, Object>> result, SourceConfUtil scu) {
        List<Map<String, Map<String, Object>>> tableResults = new ArrayList<Map<String, Map<String, Object>>> (  );
        if ((result == null) || (result.isEmpty())) {
            return null;
        }
        //记录增量值
        Map<String, Object> lastMap= result.get ( result.size ()-1 );
        Object signId = lastMap.get ( "_src_default_id" );
        scu.setSignId ( signId.toString ());
        //TODO debug
        LOG.error ( "当前查询值与已记录值：" +signId +"---"+scu.getSignId ());

        for (Map<String, Object> map : result) {
            //删除标记
            map.remove ( "_src_default_id" );
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                Object value = entry.getValue();
                //去null
                if (value == null) {
                    entry.setValue("");
                }
            }
            Map<String, Map<String, Object>> temp = new HashMap<String, Map<String, Object>> (  );
            temp.put ( scu.getTable (),map );
            tableResults.add ( temp );
        }
        return tableResults;
    }


    /**
     * 写数据
     * @param tableResult
     * @param printWriter
     */
    public static void writeAllRows(List<Map<String, Map<String, Object>>> tableResult, PrintWriter printWriter) {
        if ((tableResult == null) || (tableResult.isEmpty())) {
            return;
        }
        Gson gson = new Gson ();
        for (Map<String, Map<String, Object>> map : tableResult) {
            String json = gson.toJson(map);
            //写入io缓存
            printWriter.print(json);
        }
    }
}
