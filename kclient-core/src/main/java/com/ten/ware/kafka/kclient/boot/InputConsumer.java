package com.ten.ware.kafka.kclient.boot;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is used for a method which serve as a message handler. It
 * includes the metadata for the input topic.
 * <p>
 * 此注释用于作为消息处理程序的方法。它包含输入主题的元数据。代表方法的参数来自于一个消息队列
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface InputConsumer {
    String propertiesFile() default "";

    String topic() default "";

    int streamNum() default 1;

    int fixedThreadNum() default 0;

    int minThreadNum() default 0;

    int maxThreadNum() default 0;
}
