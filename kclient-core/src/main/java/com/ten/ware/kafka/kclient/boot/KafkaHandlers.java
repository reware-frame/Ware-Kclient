package com.ten.ware.kafka.kclient.boot;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.stereotype.Component;

/**
 * This annotation is used to declare a class to be a message handler
 * collection. This bean be also Spring @Component so that it can be
 * component-scanned by spring context.
 * <p>
 * 标记类中的消息处理器方法。此注释用于将类声明为消息处理程序集合。这个bean也是Spring @Component，这样它就可以被Spring上下文组件扫描。
 */

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface KafkaHandlers {
}
