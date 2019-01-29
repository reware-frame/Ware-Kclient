package com.ten.ware.kafka.kclient.reflection.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * This is the main class to traverse a class definition and provide any
 * declared annotation for AnnotationHandler.
 *
 * 模板回调模式，通过传入一个类即一个回调方法，会自动遍历这个类，如果发现了注解，就调用模板回调。
 *
 * @param <C> category type for the data entry 数据类型
 * @param <K> key type for the data entry 数据键值类型
 * @param <V> value type for the date entry 数据键值
 * @author Robert Lee
 * @since Aug 21, 2015
 */

public class AnnotationTranversor<C, K, V> {
    Class<?> clazz;

    public AnnotationTranversor(Class<?> clazz) {
        this.clazz = clazz;
    }

    /**
     * @param annotationHandler 接口实现类，可匿名内部类
     */
    public Map<C, Map<K, V>> tranverseAnnotation(AnnotationHandler<C, K, V> annotationHandler) {
        TranversorContext<C, K, V> ctx = new TranversorContext<>();

        // 遍历类注解
        for (Annotation annotation : clazz.getAnnotations()) {
            // 添加注解方法到类
            annotationHandler.handleClassAnnotation(clazz, annotation, ctx);
        }

        // 遍历方法注解
        for (Method method : clazz.getMethods()) {
            for (Annotation annotation : method.getAnnotations()) {
                // 添加注解方法到对象
                annotationHandler.handleMethodAnnotation(clazz, method, annotation, ctx);
            }
        }

        return ctx.getData();
    }
}
