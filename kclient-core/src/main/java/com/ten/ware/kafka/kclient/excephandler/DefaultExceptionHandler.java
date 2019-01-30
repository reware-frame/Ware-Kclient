package com.ten.ware.kafka.kclient.excephandler;

import com.ten.ware.kafka.kclient.handlers.SafelyMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default exception handler to log the error context in the error log which can
 * be used to retry the message latter manully or by support tool.
 * <p>
 * 默认异常处理器
 */
public class DefaultExceptionHandler implements ExceptionHandler {

    // Record the error log for data recovery or wash data by the logs

    protected static Logger errorRunLog = LoggerFactory.getLogger("error.run." + SafelyMessageHandler.class);
    protected static Logger errorShutdownlog = LoggerFactory.getLogger("error.shutdown." + SafelyMessageHandler.class);

    @Override
    public boolean support(Throwable t) {
        return true;
    }

    @Override
    public void handle(Throwable t, String message) {
        if (t instanceof InterruptedException) {
            errorRunLog.error(
                    "Maybe it is shutting down. Or interruped when handing the message:\t"
                            + message, t);
        } else {
            errorShutdownlog.error(
                    "Failed to handle the message: \t" + message, t);
        }
    }
}
