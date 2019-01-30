package com.ten.ware.kafka.kclient.boot;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.StringUtils;
import org.w3c.dom.Document;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ten.ware.kafka.kclient.core.KafkaConsumer;
import com.ten.ware.kafka.kclient.core.KafkaProducer;
import com.ten.ware.kafka.kclient.excephandler.ExceptionHandler;
import com.ten.ware.kafka.kclient.handlers.BeanMessageHandler;
import com.ten.ware.kafka.kclient.handlers.BeansMessageHandler;
import com.ten.ware.kafka.kclient.handlers.DocumentMessageHandler;
import com.ten.ware.kafka.kclient.handlers.MessageHandler;
import com.ten.ware.kafka.kclient.handlers.ObjectMessageHandler;
import com.ten.ware.kafka.kclient.handlers.ObjectsMessageHandler;
import com.ten.ware.kafka.kclient.reflection.util.AnnotationHandler;
import com.ten.ware.kafka.kclient.reflection.util.AnnotationTranversor;
import com.ten.ware.kafka.kclient.reflection.util.TranversorContext;

/**
 * This is the starter of the annotated message handlers. This class is loaded
 * by any spring context. When the context is post constructed, it will read the
 * annotated message handler, and then create the related consumer or producer.
 * Finally, we start the consumer server.
 * <p>
 * 这是带注释的消息处理程序的启动器。该类由任何spring上下文加载。当构造上下文时，它将读取带注释的消息处理程序，
 * 然后创建相关的使用者或生产者。最后，我们启动消费者服务器。
 * 初始化时，读取Bean，解析kafkahandlers注解的方法，读取input，output注解，然后构造相应的生产者和消费者对象。
 */

public class KClientBoot implements ApplicationContextAware {
    protected static Logger log = LoggerFactory.getLogger(KClientBoot.class);

    private ApplicationContext applicationContext;

    private List<KafkaHandlerMeta> meta = new ArrayList<KafkaHandlerMeta>();

    private List<KafkaHandler> kafkaHandlers = new ArrayList<KafkaHandler>();

    public KClientBoot() {
        // For Spring Context
    }

    public void init() {
        meta = getKafkaHandlerMeta();

        if (meta.size() == 0) {
            throw new IllegalArgumentException(
                    "No handler method is declared in this spring context.");
        }

        for (final KafkaHandlerMeta kafkaHandlerMeta : meta) {
            createKafkaHandler(kafkaHandlerMeta);
        }
    }

    /**
     * 遍历spring环境中的所有bean，提取@kafkahandlers注解，提取其中的处理器方法并转换成kafkahandlermeta
     */
    protected List<KafkaHandlerMeta> getKafkaHandlerMeta() {
        List<KafkaHandlerMeta> meta = new ArrayList<KafkaHandlerMeta>();

        // 遍历class读取注解
        String[] kafkaHandlerBeanNames = applicationContext
                .getBeanNamesForAnnotation(KafkaHandlers.class);

        // 遍历每个@kafkahandler
        for (String kafkaHandlerBeanName : kafkaHandlerBeanNames) {
            Object kafkaHandlerBean = applicationContext
                    .getBean(kafkaHandlerBeanName);
            Class<? extends Object> kafkaHandlerBeanClazz = kafkaHandlerBean
                    .getClass();
            Map<Class<? extends Annotation>, Map<Method, Annotation>> mapData = extractAnnotationMaps(kafkaHandlerBeanClazz);

            meta.addAll(convertAnnotationMaps2Meta(mapData, kafkaHandlerBean));
        }

        return meta;
    }

    /**
     * 提取注释映射，提取@input，@output，@error等注解，把反射功能封装在{@link AnnotationTranversor}中
     */
    protected Map<Class<? extends Annotation>, Map<Method, Annotation>> extractAnnotationMaps(Class<? extends Object> clazz) {
        // 注解处理器
        AnnotationTranversor<Class<? extends Annotation>, Method, Annotation> annotationTranversor = new AnnotationTranversor<>(clazz);

        Map<Class<? extends Annotation>, Map<Method, Annotation>> data = annotationTranversor
                .tranverseAnnotation(new AnnotationHandler<Class<? extends Annotation>, Method, Annotation>() {

                    /*
                     * 匿名内部类，实现了{@link AnnotationHandler}接口
                     */

                    /**
                     * 添加注解到方法的map中
                     */
                    @Override
                    public void handleMethodAnnotation(
                            Class<?> clazz,
                            Method method,
                            Annotation annotation,
                            TranversorContext<Class<? extends Annotation>, Method, Annotation> context) {
                        // input方法注解
                        if (annotation instanceof InputConsumer) {
                            context
                                    .addEntry(InputConsumer.class,
                                            method,
                                            annotation);
                        }
                        // output方法注解
                        else if (annotation instanceof OutputProducer) {
                            context
                                    .addEntry(OutputProducer.class,
                                            method,
                                            annotation);
                        }
                        // error方法注解
                        else if (annotation instanceof ErrorHandler) {
                            context
                                    .addEntry(ErrorHandler.class,
                                            method,
                                            annotation);
                        }
                    }

                    /**
                     * 添加注解到类的map中
                     */
                    @Override
                    public void handleClassAnnotation(
                            Class<?> clazz,
                            Annotation annotation,
                            TranversorContext<Class<? extends Annotation>, Method, Annotation> context) {
                        // 类型注解
                        if (annotation instanceof KafkaHandlers) {
                            log.warn(
                                    "There is some other annotation {} rather than @KafkaHandlers in the handler class {}.",
                                    annotation.getClass().getName(),
                                    clazz.getName());
                        }
                    }
                });

        return data;
    }

    /**
     * 把提取出来的注解转换成元数据对象，供接下来创建相应的生产者和消费者对象使用
     *
     * @param mapData 方法/类 对象的注解map集合
     * @param bean    kafkahandler对象
     */
    protected List<KafkaHandlerMeta> convertAnnotationMaps2Meta(
            Map<Class<? extends Annotation>, Map<Method, Annotation>> mapData,
            Object bean) {
        // 元数据集合
        List<KafkaHandlerMeta> meta = new ArrayList<>();

        // 提取注解
        Map<Method, Annotation> inputConsumerMap = mapData
                .get(InputConsumer.class);
        Map<Method, Annotation> outputProducerMap = mapData
                .get(OutputProducer.class);
        Map<Method, Annotation> exceptionHandlerMap = mapData
                .get(ErrorHandler.class);

        // 遍历input消费者注解
        for (Map.Entry<Method, Annotation> entry : inputConsumerMap.entrySet()) {
            InputConsumer inputConsumer = (InputConsumer) entry.getValue();

            KafkaHandlerMeta kafkaHandlerMeta = new KafkaHandlerMeta();

            kafkaHandlerMeta.setBean(bean);
            kafkaHandlerMeta.setMethod(entry.getKey());

            Parameter[] kafkaHandlerParameters = entry.getKey().getParameters();
            if (kafkaHandlerParameters.length != 1) {
                throw new IllegalArgumentException(
                        "The kafka handler method can contains only one parameter.");
            }
            kafkaHandlerMeta.setParameterType(kafkaHandlerParameters[0]
                    .getType());

            kafkaHandlerMeta.setInputConsumer(inputConsumer);

            if (outputProducerMap != null
                    && outputProducerMap.containsKey(entry.getKey())) {
                kafkaHandlerMeta
                        .setOutputProducer((OutputProducer) outputProducerMap
                                .get(entry.getKey()));
            }

            if (exceptionHandlerMap != null) {
                for (Map.Entry<Method, Annotation> excepHandlerEntry : exceptionHandlerMap
                        .entrySet()) {
                    ErrorHandler eh = (ErrorHandler) excepHandlerEntry
                            .getValue();
                    if (StringUtils.isEmpty(eh.topic())
                            || eh.topic().equals(inputConsumer.topic())) {
                        kafkaHandlerMeta.addErrorHandlers((ErrorHandler) eh,
                                excepHandlerEntry.getKey());
                    }
                }
            }

            meta.add(kafkaHandlerMeta);
        }

        return meta;
    }

    /**
     * 根据刚才提取的元数据创建kafka处理器，根据处理器方法的参数选择不同的消息处理器，
     * 例如，如果声明了JSONObject类型，则创建JSON消息处理器，如果声明了document类型的参数，则创建XML消息处理器。
     */
    protected void createKafkaHandler(final KafkaHandlerMeta kafkaHandlerMeta) {
        Class<? extends Object> paramClazz = kafkaHandlerMeta.getParameterType();

        KafkaProducer kafkaProducer = createProducer(kafkaHandlerMeta);
        List<ExceptionHandler> excepHandlers = createExceptionHandlers(kafkaHandlerMeta);

        MessageHandler beanMessageHandler;
        if (paramClazz.isAssignableFrom(JSONObject.class)) {
            beanMessageHandler = createObjectHandler(kafkaHandlerMeta,
                    kafkaProducer, excepHandlers);
        } else if (paramClazz.isAssignableFrom(JSONArray.class)) {
            beanMessageHandler = createObjectsHandler(kafkaHandlerMeta,
                    kafkaProducer, excepHandlers);
        } else if (List.class.isAssignableFrom(Document.class)) {
            beanMessageHandler = createDocumentHandler(kafkaHandlerMeta,
                    kafkaProducer, excepHandlers);
        } else if (List.class.isAssignableFrom(paramClazz)) {
            beanMessageHandler = createBeansHandler(kafkaHandlerMeta,
                    kafkaProducer, excepHandlers);
        } else {
            beanMessageHandler = createBeanHandler(kafkaHandlerMeta,
                    kafkaProducer, excepHandlers);
        }

        KafkaConsumer kafkaConsumer = createConsumer(kafkaHandlerMeta,
                beanMessageHandler);
        kafkaConsumer.startup();

        KafkaHandler kafkaHandler = new KafkaHandler(kafkaConsumer,
                kafkaProducer, excepHandlers, kafkaHandlerMeta);

        kafkaHandlers.add(kafkaHandler);

    }

    /**
     * 异常处理器，在input和output处理失败时，调用异常处理器
     */
    private List<ExceptionHandler> createExceptionHandlers(
            final KafkaHandlerMeta kafkaHandlerMeta) {
        List<ExceptionHandler> excepHandlers = new ArrayList<ExceptionHandler>();

        for (final Map.Entry<ErrorHandler, Method> errorHandler : kafkaHandlerMeta
                .getErrorHandlers().entrySet()) {
            ExceptionHandler exceptionHandler = new ExceptionHandler() {
                @Override
                public boolean support(Throwable t) {
                    // We handle the exception when the classes are exactly same
                    return errorHandler.getKey().exception() == t.getClass();
                }

                @Override
                public void handle(Throwable t, String message) {

                    Method excepHandlerMethod = errorHandler.getValue();
                    try {
                        excepHandlerMethod.invoke(kafkaHandlerMeta.getBean(),
                                t, message);

                    } catch (IllegalAccessException e) {
                        // If annotated exception handler is correct, this won't
                        // happen
                        log.error(
                                "No permission to access the annotated exception handler.",
                                e);
                        throw new IllegalStateException(
                                "No permission to access the annotated exception handler. Please check annotated config.",
                                e);
                    } catch (IllegalArgumentException e) {
                        // If annotated exception handler is correct, this won't
                        // happen
                        log.error(
                                "The parameter passed in doesn't match the annotated exception handler's.",
                                e);
                        throw new IllegalStateException(
                                "The parameter passed in doesn't match the annotated exception handler's. Please check annotated config.",
                                e);
                    } catch (InvocationTargetException e) {
                        // If the exception during handling exception occurs,
                        // throw it, in SafelyMessageHandler, this will be
                        // processed
                        log.error(
                                "Failed to call the annotated exception handler.",
                                e);
                        throw new IllegalStateException(
                                "Failed to call the annotated exception handler. Please check if the handler can handle the biz without any exception.",
                                e);
                    }
                }
            };

            excepHandlers.add(exceptionHandler);
        }

        return excepHandlers;
    }

    /**
     * 创建JSON对象处理器
     */
    protected ObjectMessageHandler<JSONObject> createObjectHandler(
            final KafkaHandlerMeta kafkaHandlerMeta,
            final KafkaProducer kafkaProducer,
            List<ExceptionHandler> excepHandlers) {

        ObjectMessageHandler<JSONObject> objectMessageHandler = new ObjectMessageHandler<JSONObject>(
                excepHandlers) {
            @Override
            protected void doExecuteObject(JSONObject jsonObject) {
                invokeHandler(kafkaHandlerMeta, kafkaProducer, jsonObject);
            }

        };

        return objectMessageHandler;
    }

    /**
     * 创建JSON数组处理器
     */
    protected ObjectsMessageHandler<JSONArray> createObjectsHandler(
            final KafkaHandlerMeta kafkaHandlerMeta,
            final KafkaProducer kafkaProducer,
            List<ExceptionHandler> excepHandlers) {

        ObjectsMessageHandler<JSONArray> objectMessageHandler = new ObjectsMessageHandler<JSONArray>(
                excepHandlers) {
            @Override
            protected void doExecuteObjects(JSONArray jsonArray) {
                invokeHandler(kafkaHandlerMeta, kafkaProducer, jsonArray);
            }
        };

        return objectMessageHandler;
    }

    /**
     * 创建XML文档处理器
     */
    protected DocumentMessageHandler createDocumentHandler(
            final KafkaHandlerMeta kafkaHandlerMeta,
            final KafkaProducer kafkaProducer,
            List<ExceptionHandler> excepHandlers) {

        DocumentMessageHandler documentMessageHandler = new DocumentMessageHandler(
                excepHandlers) {
            @Override
            protected void doExecuteDocument(Document document) {
                invokeHandler(kafkaHandlerMeta, kafkaProducer, document);
            }
        };

        return documentMessageHandler;
    }

    /**
     * 创建BEAN处理器
     */
    @SuppressWarnings("unchecked")
    protected BeanMessageHandler<Object> createBeanHandler(
            final KafkaHandlerMeta kafkaHandlerMeta,
            final KafkaProducer kafkaProducer,
            List<ExceptionHandler> excepHandlers) {

        // We have to abandon the type check
        @SuppressWarnings("rawtypes")
        BeanMessageHandler beanMessageHandler = new BeanMessageHandler(
                kafkaHandlerMeta.getParameterType(), excepHandlers) {
            @Override
            protected void doExecuteBean(Object bean) {
                invokeHandler(kafkaHandlerMeta, kafkaProducer, bean);
            }

        };

        return beanMessageHandler;
    }

    /**
     * 创建BEAN集合处理器
     */
    @SuppressWarnings("unchecked")
    protected BeansMessageHandler<Object> createBeansHandler(
            final KafkaHandlerMeta kafkaHandlerMeta,
            final KafkaProducer kafkaProducer,
            List<ExceptionHandler> excepHandlers) {

        // We have to abandon the type check
        @SuppressWarnings("rawtypes")
        BeansMessageHandler beanMessageHandler = new BeansMessageHandler(
                kafkaHandlerMeta.getParameterType(), excepHandlers) {
            @Override
            protected void doExecuteBeans(List bean) {
                invokeHandler(kafkaHandlerMeta, kafkaProducer, bean);
            }

        };

        return beanMessageHandler;
    }

    /**
     * 创建生产者
     */
    protected KafkaProducer createProducer(
            final KafkaHandlerMeta kafkaHandlerMeta) {
        KafkaProducer kafkaProducer = null;

        if (kafkaHandlerMeta.getOutputProducer() != null) {
            kafkaProducer = new KafkaProducer(kafkaHandlerMeta
                    .getOutputProducer().propertiesFile(), kafkaHandlerMeta
                    .getOutputProducer().defaultTopic());
        }

        // It may return null
        return kafkaProducer;
    }

    /**
     * 通过反射调用具体的消息处理器方法并获得结果，根据结果的类型将其转换成文本信息，并通过生产者把处理结果发出去
     */
    private void invokeHandler(final KafkaHandlerMeta kafkaHandlerMeta,
                               final KafkaProducer kafkaProducer, Object parameter) {
        Method kafkaHandlerMethod = kafkaHandlerMeta.getMethod();
        try {
            Object result = kafkaHandlerMethod.invoke(
                    kafkaHandlerMeta.getBean(), parameter);

            if (kafkaProducer != null) {
                if (result instanceof JSONObject) {
                    kafkaProducer.send(((JSONObject) result).toJSONString());
                } else if (result instanceof JSONArray) {
                    kafkaProducer.send(((JSONArray) result).toJSONString());
                } else if (result instanceof Document) {
                    kafkaProducer.send(((Document) result).getTextContent());
                } else {
                    kafkaProducer.send(JSON.toJSONString(result));
                }
            }
        } catch (IllegalAccessException e) {
            // If annotated config is correct, this won't happen
            log.error("No permission to access the annotated kafka handler.", e);
            throw new IllegalStateException(
                    "No permission to access the annotated kafka handler. Please check annotated config.",
                    e);
        } catch (IllegalArgumentException e) {
            // If annotated config is correct, this won't happen
            log.error(
                    "The parameter passed in doesn't match the annotated kafka handler's.",
                    e);
            throw new IllegalStateException(
                    "The parameter passed in doesn't match the annotated kafka handler's. Please check annotated config.",
                    e);
        } catch (InvocationTargetException e) {
            // The SafeMessageHanlder has already handled the
            // throwable, no more exception goes here
            log.error("Failed to call the annotated kafka handler.", e);
            throw new IllegalStateException(
                    "Failed to call the annotated kafka handler. Please check if the handler can handle the biz without any exception.",
                    e);
        }
    }

    /**
     * 创建消费者
     */
    protected KafkaConsumer createConsumer(
            final KafkaHandlerMeta kafkaHandlerMeta,
            MessageHandler beanMessageHandler) {
        KafkaConsumer kafkaConsumer;

        if (kafkaHandlerMeta.getInputConsumer().fixedThreadNum() > 0) {
            kafkaConsumer = new KafkaConsumer(kafkaHandlerMeta
                    .getInputConsumer().propertiesFile(), kafkaHandlerMeta
                    .getInputConsumer().topic(), kafkaHandlerMeta
                    .getInputConsumer().streamNum(), kafkaHandlerMeta
                    .getInputConsumer().fixedThreadNum(), beanMessageHandler);

        } else if (kafkaHandlerMeta.getInputConsumer().maxThreadNum() > 0
                && kafkaHandlerMeta.getInputConsumer().minThreadNum() < kafkaHandlerMeta
                .getInputConsumer().maxThreadNum()) {
            kafkaConsumer = new KafkaConsumer(kafkaHandlerMeta
                    .getInputConsumer().propertiesFile(), kafkaHandlerMeta
                    .getInputConsumer().topic(), kafkaHandlerMeta
                    .getInputConsumer().streamNum(), kafkaHandlerMeta
                    .getInputConsumer().minThreadNum(), kafkaHandlerMeta
                    .getInputConsumer().maxThreadNum(), beanMessageHandler);

        } else {
            kafkaConsumer = new KafkaConsumer(kafkaHandlerMeta
                    .getInputConsumer().propertiesFile(), kafkaHandlerMeta
                    .getInputConsumer().topic(), kafkaHandlerMeta
                    .getInputConsumer().streamNum(), beanMessageHandler);
        }

        return kafkaConsumer;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    // The consumers can't be shutdown automatically by jvm shutdown hook if
    // this method is not called
    public void shutdownAll() {
        for (KafkaHandler kafkahandler : kafkaHandlers) {
            kafkahandler.getKafkaConsumer().shutdownGracefully();

            kafkahandler.getKafkaProducer().close();
        }
    }

    public List<KafkaHandler> getKafkaHandlers() {
        return kafkaHandlers;
    }

    public void setKafkaHandlers(List<KafkaHandler> kafkaHandlers) {
        this.kafkaHandlers = kafkaHandlers;
    }
}
