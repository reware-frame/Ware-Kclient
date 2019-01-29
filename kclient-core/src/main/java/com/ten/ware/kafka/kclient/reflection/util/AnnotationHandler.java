package com.ten.ware.kafka.kclient.reflection.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

/**
 * The class is used to process an annotation which is passed in by
 * AnnotationTranversor main class. The annotation handler will output the
 * 3-dimension map to context.
 * <p>
 * 该类用于处理由 AnnotationTranversor主类传入的注释。注释处理程序将把 3维映射输出到上下文。
 *
 * @param <C> category type for the data entry
 * @param <K> key type for the data entry
 * @param <V> value type for the date entry
 */
public interface AnnotationHandler<C, K, V> {
    /**
     * 添加方法注解功能到对象
     *
     * @param clazz      类型信息
     * @param method     方法信息
     * @param annotation 注解
     * @param context    转换器
     */
    void handleMethodAnnotation(Class<?> clazz,
                                Method method,
                                Annotation annotation,
                                TranversorContext<C, K, V> context);

    /**
     * 添加类注解功能到类对象
     *
     * @param clazz      类型
     * @param annotation 注解
     * @param context    转换器
     */
    void handleClassAnnotation(Class<?> clazz,
                               Annotation annotation,
                               TranversorContext<C, K, V> context);
}
