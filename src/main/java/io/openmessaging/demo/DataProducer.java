package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.Producer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DataProducer {
    public static HashMap<String, List<Message>> produce(){
        HashMap<String, List<Message>> map = new HashMap<>();
        KeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", "E:/Major/Open-Messaging");
        Producer producer = new DefaultProducer(properties);

        for (int i = 0; i < 50; i++) {
            String topic = "TOPIC" + i;
            List<Message> list = new ArrayList<>(1024);
            for (int j = 0; j < 1024; j++) {
                list.add(producer.createBytesMessageToTopic(topic,(topic+j).getBytes()));
            }
            map.put(topic, list);
        }

        for (int i = 0; i < 50; i++) {
            String queue = "QUEUE" + i;
            List<Message> list = new ArrayList<>(1024);
            for (int j = 0; j < 1024; j++) {
                list.add(producer.createBytesMessageToQueue(queue,(queue+j).getBytes()));
            }
            map.put(queue, list);
        }

        return map;
    }
}
