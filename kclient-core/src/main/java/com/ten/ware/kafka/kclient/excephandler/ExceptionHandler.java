package com.ten.ware.kafka.kclient.excephandler;

/**
 * This is the pluggable interface to handle the unexpected exception when
 * handing the message in handlers.
 * <p>
 * 处理异常
 */
public interface ExceptionHandler {
    boolean support(Throwable t);

    void handle(Throwable t, String message);
}
