package com.ten.ware.kafka.kclient.boot;

import java.util.List;

import com.ten.ware.kafka.kclient.core.KafkaConsumer;
import com.ten.ware.kafka.kclient.core.KafkaProducer;
import com.ten.ware.kafka.kclient.excephandler.ExceptionHandler;

/**
 * The context class which stores the runtime Kafka processor reference.
 * <p>
 * 存储运行时Kafka处理器引用的上下文类。
 */
public class KafkaHandler {
    private KafkaConsumer kafkaConsumer;

    private KafkaProducer kafkaProducer;

    private List<ExceptionHandler> excepHandlers;

    private KafkaHandlerMeta kafkaHandlerMeta;

    public KafkaHandler(KafkaConsumer kafkaConsumer,
                        KafkaProducer kafkaProducer, List<ExceptionHandler> excepHandlers,
                        KafkaHandlerMeta kafkaHandlerMeta) {
        super();
        this.kafkaConsumer = kafkaConsumer;
        this.kafkaProducer = kafkaProducer;
        this.excepHandlers = excepHandlers;
        this.kafkaHandlerMeta = kafkaHandlerMeta;
    }

    public KafkaConsumer getKafkaConsumer() {
        return kafkaConsumer;
    }

    public void setKafkaConsumer(KafkaConsumer kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    public KafkaProducer getKafkaProducer() {
        return kafkaProducer;
    }

    public void setKafkaProducer(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public List<ExceptionHandler> getExcepHandlers() {
        return excepHandlers;
    }

    public void setExcepHandlers(List<ExceptionHandler> excepHandlers) {
        this.excepHandlers = excepHandlers;
    }

    public KafkaHandlerMeta getKafkaHandlerMeta() {
        return kafkaHandlerMeta;
    }

    public void setKafkaHandlerMeta(KafkaHandlerMeta kafkaHandlerMeta) {
        this.kafkaHandlerMeta = kafkaHandlerMeta;
    }

}
