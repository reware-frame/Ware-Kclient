package com.ten.ware.kafka.kclient.boot;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is used for a method which serve as an exception handler. It
 * includes the metadata for an exception handler.
 * <p>
 * 此注释用于作为异常处理程序的方法。它包含异常处理程序的元数据。
 */

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ErrorHandler {
    Class<? extends Throwable> exception() default Throwable.class;

    String topic() default "";
}
