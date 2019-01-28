package com.ten.ware.kafka.kclient.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 这是一个生产者客户端，可用于发送消息或密钥和消息的对。
 * 可以一次发送一条消息，也可以一次发送多条消息。
 * 当多个消息时，它将在一个批处理中只发送20条消息。
 * <p>
 * 提供多种类型的发送API，例如发送领域对象、消息、JSON对象、单个对象及数组、集合等的API
 */
public class KafkaProducer {
    protected static Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    // If the number of one batch is over 20, use 20 instead
    protected static int MULTI_MSG_ONCE_SEND_NUM = 20;

    // kafka生产者
    private Producer<String, String> producer;

    private String defaultTopic;

    private String propertiesFile;
    private Properties properties;

    public KafkaProducer() {
        // For Spring context
    }

    public KafkaProducer(String propertiesFile, String defaultTopic) {
        this.propertiesFile = propertiesFile;
        this.defaultTopic = defaultTopic;

        init();
    }

    public KafkaProducer(Properties properties, String defaultTopic) {
        this.properties = properties;
        this.defaultTopic = defaultTopic;

        init();
    }

    protected void init() {
        if (properties == null) {
            properties = new Properties();
            try {
                properties.load(Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream(propertiesFile));
            } catch (IOException e) {
                log.error("The properties file is not loaded.", e);
                throw new IllegalArgumentException(
                        "The properties file is not loaded.", e);
            }
        }
        log.info("Producer properties:" + properties);

        // 加载kafka配置对象
        ProducerConfig config = new ProducerConfig(properties);
        producer = new Producer<String, String>(config);
    }

    // send string message

    public void send(String message) {
        send2Topic(null, message);
    }

    public void send2Topic(String topicName, String message) {
        if (message == null) {
            return;
        }

        if (topicName == null) {
            topicName = defaultTopic;
        }

        KeyedMessage<String, String> km = new KeyedMessage<String, String>(
                topicName, message);
        producer.send(km);
    }

    public void send(String key, String message) {
        send2Topic(null, key, message);
    }

    public void send2Topic(String topicName, String key, String message) {
        if (message == null) {
            return;
        }

        if (topicName == null) {
            topicName = defaultTopic;
        }

        KeyedMessage<String, String> km = new KeyedMessage<String, String>(
                topicName, key, message);
        producer.send(km);
    }

    public void send(Collection<String> messages) {
        send2Topic(null, messages);
    }

    public void send2Topic(String topicName, Collection<String> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        if (topicName == null) {
            topicName = defaultTopic;
        }

        List<KeyedMessage<String, String>> kms = new ArrayList<KeyedMessage<String, String>>();
        int i = 0;
        for (String entry : messages) {
            KeyedMessage<String, String> km = new KeyedMessage<String, String>(
                    topicName, entry);
            kms.add(km);
            i++;
            // Send the messages 20 at most once
            if (i % MULTI_MSG_ONCE_SEND_NUM == 0) {
                producer.send(kms);
                kms.clear();
            }
        }

        if (!kms.isEmpty()) {
            producer.send(kms);
        }
    }

    public void send(Map<String, String> messages) {
        send2Topic(null, messages);
    }

    public void send2Topic(String topicName, Map<String, String> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        if (topicName == null)
            topicName = defaultTopic;

        List<KeyedMessage<String, String>> kms = new ArrayList<KeyedMessage<String, String>>();

        int i = 0;
        for (Entry<String, String> entry : messages.entrySet()) {
            KeyedMessage<String, String> km = new KeyedMessage<String, String>(
                    topicName, entry.getKey(), entry.getValue());
            kms.add(km);
            i++;
            // Send the messages 20 at most once
            if (i % MULTI_MSG_ONCE_SEND_NUM == 0) {
                producer.send(kms);
                kms.clear();
            }
        }

        if (!kms.isEmpty()) {
            producer.send(kms);
        }
    }

    // send bean message
    // 自动进行JSON序列化

    public <T> void sendBean(T bean) {
        sendBean2Topic(null, bean);
    }

    public <T> void sendBean2Topic(String topicName, T bean) {
        send2Topic(topicName, JSON.toJSONString(bean));
    }

    public <T> void sendBean(String key, T bean) {
        sendBean2Topic(null, key, bean);
    }

    public <T> void sendBean2Topic(String topicName, String key, T bean) {
        send2Topic(topicName, key, JSON.toJSONString(bean));
    }

    public <T> void sendBeans(Collection<T> beans) {
        sendBeans2Topic(null, beans);
    }

    public <T> void sendBeans2Topic(String topicName, Collection<T> beans) {
        Collection<String> beanStrs = new ArrayList<String>();
        for (T bean : beans) {
            beanStrs.add(JSON.toJSONString(bean));
        }

        send2Topic(topicName, beanStrs);
    }

    public <T> void sendBeans(Map<String, T> beans) {
        sendBeans2Topic(null, beans);
    }

    public <T> void sendBeans2Topic(String topicName, Map<String, T> beans) {
        Map<String, String> beansStr = new HashMap<String, String>();
        for (Map.Entry<String, T> entry : beans.entrySet()) {
            beansStr.put(entry.getKey(), JSON.toJSONString(entry.getValue()));
        }

        send2Topic(topicName, beansStr);
    }

    // send JSON Object message
    // 自动进行对象JSON序列化

    public void sendObject(JSONObject jsonObject) {
        sendObject2Topic(null, jsonObject);
    }

    public void sendObject2Topic(String topicName, JSONObject jsonObject) {
        send2Topic(topicName, jsonObject.toJSONString());
    }

    public void sendObject(String key, JSONObject jsonObject) {
        sendObject2Topic(null, key, jsonObject);
    }

    public void sendObject2Topic(String topicName, String key,
                                 JSONObject jsonObject) {
        send2Topic(topicName, key, jsonObject.toJSONString());
    }

    public void sendObjects(JSONArray jsonArray) {
        sendObjects2Topic(null, jsonArray);
    }

    public void sendObjects2Topic(String topicName, JSONArray jsonArray) {
        send2Topic(topicName, jsonArray.toJSONString());
    }

    public void sendObjects(Map<String, JSONObject> jsonObjects) {
        sendObjects2Topic(null, jsonObjects);
    }

    public void sendObjects2Topic(String topicName,
                                  Map<String, JSONObject> jsonObjects) {
        Map<String, String> objectsStrs = new HashMap<String, String>();
        for (Map.Entry<String, JSONObject> entry : jsonObjects.entrySet()) {
            objectsStrs.put(entry.getKey(), entry.getValue().toJSONString());
        }

        send2Topic(topicName, objectsStrs);
    }

    public void close() {
        producer.close();
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }

    public String getPropertiesFile() {
        return propertiesFile;
    }

    public void setPropertiesFile(String propertiesFile) {
        this.propertiesFile = propertiesFile;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
