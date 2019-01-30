package com.ten.ware.kafka.kclient.handlers;

/**
 * This is the exposed interface which the business component should implement
 * to handle the Kafka message.
 * <p>
 * 消息处理器
 */
public interface MessageHandler {
    void execute(String message);
}
