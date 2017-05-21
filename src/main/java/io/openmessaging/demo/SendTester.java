package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.Producer;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;

public class SendTester {


    public static void main(String[] args) {
        ExecutorService executorService = Executors.newCachedThreadPool();


        KeyValue properties = new DefaultKeyValue();

        properties.put("STORE_PATH", "E:/Major/Open-Messaging");

        ConcurrentHashMap<String, ConcurrentLinkedQueue<Message>> data = DataProducer.produce();

        long start = System.currentTimeMillis();
        //发送, 实际测试时，会用多线程来发送, 每个线程发送自己的Topic和Queue

        System.out.println("测试开始");
        Iterator<Map.Entry<String, ConcurrentLinkedQueue<Message>>> it = data.entrySet().iterator();
        for (int i = 0; i < 10; i++) {
            executorService.execute(() -> {
                Producer producer1 = new DefaultProducer(properties);
                for (int j = 0; j < 10; j++) {
                    ConcurrentLinkedQueue<Message> queue = it.next().getValue();
                    for (Message message : queue) {
                        producer1.send(message);
                    }
                    System.out.println("完成百分比：" + j);
                }
            });
        }
        executorService.shutdown();
        try {
            //等待20分钟
            executorService.awaitTermination(20, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new DefaultProducer(properties).flush();
        long end = System.currentTimeMillis();

        long T1 = end - start;

        System.out.println("Send Cost: " + T1);
    }
}

