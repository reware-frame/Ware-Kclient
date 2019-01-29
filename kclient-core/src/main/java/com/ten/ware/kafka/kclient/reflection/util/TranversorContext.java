package com.ten.ware.kafka.kclient.reflection.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Used to collect the traversal result.
 *
 * @param <C> category type for the data entry
 * @param <K> key type for the data entry
 * @param <V> value type for the date entry
 */

public class TranversorContext<C, K, V> {

    private Map<C, Map<K, V>> data = new HashMap<>();

    /**
     * @param cat   注解类型
     * @param key   方法对象
     * @param value 注解对象
     */
    public void addEntry(C cat, K key, V value) {
        Map<K, V> map = data.computeIfAbsent(cat, k -> new HashMap<>());

        // 方法注解重复了
        if (map.containsKey(key)) {
            throw new IllegalArgumentException(String.format(
                    "Duplicated Annotation {} in a single handler method {}.",
                    value.getClass(), key));
        }

        map.put(key, value);
    }

    public Map<C, Map<K, V>> getData() {
        return data;
    }
}
