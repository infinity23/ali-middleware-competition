package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.Producer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class DataProducer {
    public static ConcurrentHashMap<String, CopyOnWriteArrayList<Message>> produce(){
        ConcurrentHashMap<String, CopyOnWriteArrayList<Message>> map = new ConcurrentHashMap<>();
        KeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", "E:/Major/Open-Messaging");
        Producer producer = new DefaultProducer(properties);
        final int QUAN = 10000;

        for (int i = 0; i < 50; i++) {
            String topic = "TOPIC" + i;
            CopyOnWriteArrayList<Message> list = new CopyOnWriteArrayList<>();
            for (int j = 0; j < QUAN; j++) {
                list.add(producer.createBytesMessageToTopic(topic,(topic+j).getBytes()));
            }
            map.put(topic, list);
        }

        for (int i = 0; i < 50; i++) {
            String queue = "QUEUE" + i;
            CopyOnWriteArrayList<Message> list = new CopyOnWriteArrayList<>();
            for (int j = 0; j < QUAN; j++) {
                list.add(producer.createBytesMessageToQueue(queue,(queue+j).getBytes()));
            }
            map.put(queue, list);
        }

        return map;
    }
}
