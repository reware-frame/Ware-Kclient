package com.ten.ware.kafka.kclient.handlers;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ten.ware.kafka.kclient.excephandler.DefaultExceptionHandler;
import com.ten.ware.kafka.kclient.excephandler.ExceptionHandler;

/**
 * This is an abstract class which handle exception by exception handlers if it
 * happens.
 * <p>
 * 异常处理器
 */
public abstract class SafelyMessageHandler implements MessageHandler {
    protected static Logger log = LoggerFactory.getLogger(SafelyMessageHandler.class);

    private List<ExceptionHandler> excepHandlers = new ArrayList<>();

    {
        excepHandlers.add(new DefaultExceptionHandler());
    }

    public SafelyMessageHandler() {

    }

    public SafelyMessageHandler(ExceptionHandler excepHandler) {
        this.excepHandlers.add(excepHandler);
    }

    public SafelyMessageHandler(List<ExceptionHandler> excepHandlers) {
        this.excepHandlers.addAll(excepHandlers);
    }

    /**
     * 具体实现派遣给子类实现，只负责捕捉子类的异常
     */
    @Override
    public void execute(String message) {
        try {
            doExecute(message);
        } catch (Throwable t) {
            handleException(t, message);
        }
    }

    /**
     * 异常处理
     */
    protected void handleException(Throwable t, String message) {
        for (ExceptionHandler excepHandler : excepHandlers) {
            if (t.getClass() == IllegalStateException.class
                    && t.getCause() != null
                    && t.getCause().getClass() == InvocationTargetException.class
                    && t.getCause().getCause() != null) {
                t = t.getCause().getCause();
            }

            if (excepHandler.support(t)) {
                try {
                    excepHandler.handle(t, message);
                } catch (Exception e) {
                    log.error(
                            "Exception hanppens when the handler {} is handling the exception {} and the message {}. Please check if the exception handler is configured properly.",
                            excepHandler.getClass(), t.getClass(), message);
                    log.error(
                            "The stack of the new exception on exception is, ",
                            e);
                }
            }
        }

    }

    protected abstract void doExecute(String message);

    public List<ExceptionHandler> getExcepHandlers() {
        return excepHandlers;
    }

    public void setExcepHandlers(List<ExceptionHandler> excepHandlers) {
        this.excepHandlers.addAll(excepHandlers);
    }

    public void addExcepHandler(ExceptionHandler excepHandler) {
        this.excepHandlers.add(excepHandler);
    }

}
