package com.zlh.flume.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DB工具类
 * Author zlh
 * Date 2018-12-03
 * Version 1.0
 */
public class DBUtil {
    private static final Logger LOG = LoggerFactory.getLogger ( DBUtil.class );

    private String url, user, passwd, driver;
    private Connection conn;
    private Statement stmt;

    public DBUtil(Map<String, String> dbMaps) {
        this.url = dbMaps.get ( "url" );
        this.user = dbMaps.get ( "user" );
        this.passwd = dbMaps.get ( "passwd" );
        this.driver = dbMaps.get ( "driver" );
    }

    /**
     * jdbc连接初始化
     *
     * @return
     * @throws CommsException
     */
    public void dbStart() throws CommsException {
        try {
            Class.forName ( driver );
            conn = DriverManager.getConnection ( url, user, passwd );
            //可以滚动的类型的Result
            stmt = conn.createStatement ( ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY );
            //关闭自动提交
            conn.setAutoCommit ( false );
        } catch (ClassNotFoundException e) {
            LOG.error ( "driver加载失败" );
            throw new CommsException ( "driver加载失败", e );
        } catch (SQLException e) {
            LOG.error ( "连接失败" );
            throw new CommsException ( "连接失败", e );
        }
    }

    /**
     * 重新连接
     */
    private void reDBStart() {
        try {
            Thread.sleep ( 30000 );
        } catch (InterruptedException e) {
            LOG.error ( "线程休眠被打断", e );
        }
        try {
            dbStop ();
            dbStart ();
        } catch (CommsException e) {
            LOG.error ( "重连异常", e );
        }
    }

    /**
     * 关闭jdbc连接
     *
     * @throws CommsException
     */
    public void dbStop() throws CommsException {
        try {
            this.closePrep ();
            this.closeConn ();
        } catch (SQLException e) {
            LOG.error ( "关闭连接失败" );
            throw new CommsException ( "关闭连接失败", e );
        }
    }

    public void closeConn() throws SQLException {
        if (conn != null) {
            conn.close ();
        }
    }

    public void closePrep() throws SQLException {
        if (stmt != null) {
            stmt.close ();
        }
    }

    public List<Map<String, Object>> getResult(String query) throws CommsException {
        List<Map<String, Object>> rows = new ArrayList ();
        //判断连接是否存在
        if (conn == null && stmt == null) {
            reDBStart ();
        } else {
            try {
                ResultSet rs = stmt.executeQuery ( query );
                //rs转换成List<Map>
                rows = CommUtil.rsToList ( rs );

            } catch (SQLException e) {
                LOG.error ( "数据查询异常！", e );
                throw new CommsException ( "数据查询异常！", e );
            }
        }
        return rows;
    }

    public void executeBatch(List<String> bodys) throws CommsException {
        //判断连接是否存在
        if (conn == null && stmt == null) {
            LOG.info ( "连接为null，已重连，请重试！" );
            reDBStart ();
            //回滚重试
            throw new CommsException ( "连接为null，已重连，请重试！");
        }
        try {
            for (String sql : bodys) {
                stmt.addBatch ( sql );
            }
            stmt.executeBatch ();
            conn.commit ();
        } catch (SQLException e) {
            LOG.error ( "批处理异常！" );
            throw new CommsException ( "批处理异常！", e );
        }
    }
}
