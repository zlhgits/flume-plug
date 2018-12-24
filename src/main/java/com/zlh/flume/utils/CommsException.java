package com.zlh.flume.utils;

/**
 * @Description:公用异常信息处理
 * @Author:	zlh
 * @CreateDate:	2017年9月21日上午11:04:19
 */
@SuppressWarnings("serial")
public class CommsException extends Exception {
    private static final long serialVersionUID = 1543822681000L;
	private String msg = "公用异常信息！";
    public CommsException() {    }
    public CommsException(String msg) {
        super(msg);
        this.msg = msg;
    }
    public CommsException(String msg,Throwable e) {
        super(msg,e);
        this.msg = msg;
    }
    public String getMsg(){
    	return msg;
    }
}
