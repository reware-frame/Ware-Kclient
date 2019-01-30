package com.ten.ware.kafka.kclient.boot;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is used in method level. It implies that the method's result
 * will be sent to the other queue. It store the metadata of the queue.
 * <p>
 * 此注释用于方法级别。这意味着方法的返回值将被发送到另一个队列。它存储队列的元数据。
 */

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OutputProducer {
    String propertiesFile() default "";

    String defaultTopic() default "";
}
