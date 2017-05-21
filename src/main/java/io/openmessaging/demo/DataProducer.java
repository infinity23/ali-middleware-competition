package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.Producer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DataProducer {
    public static ConcurrentHashMap<String, ConcurrentLinkedQueue<Message>> produce(){
        ConcurrentHashMap<String, ConcurrentLinkedQueue<Message>> map = new ConcurrentHashMap<>();
        KeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", "E:/Major/Open-Messaging");
        Producer producer = new DefaultProducer(properties);
        final int QUAN = 100000;

        for (int i = 0; i < 50; i++) {
            String topic = "TOPIC" + i;
            ConcurrentLinkedQueue<Message> queue = new ConcurrentLinkedQueue<>();
            for (int j = 0; j < QUAN; j++) {
                queue.add(producer.createBytesMessageToTopic(topic,(topic+j).getBytes()));
            }
            map.put(topic, queue);
//            System.out.println("TOPIC: "+i);
        }

        for (int i = 0; i < 50; i++) {
            String queue = "QUEUE" + i;
            ConcurrentLinkedQueue<Message> queue1 = new ConcurrentLinkedQueue<>();
            for (int j = 0; j < QUAN; j++) {
                queue1.add(producer.createBytesMessageToQueue(queue,(queue+j).getBytes()));
            }
            map.put(queue, queue1);
//            System.out.println("QUEUE: "+i);
        }

        return map;
    }
}
