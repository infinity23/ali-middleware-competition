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

        System.out.println("测试开始");
        Iterator<Map.Entry<String, ConcurrentLinkedQueue<Message>>> it = data.entrySet().iterator();
        for (int i = 0; i < 10; i++) {
            int finalI = i;
            executorService.execute(() -> {
                        Producer producer1 = new DefaultProducer(properties);
                        for (int j = 0; j < 10; j++) {
                            ConcurrentLinkedQueue<Message> queue = it.next().getValue();
                            for (Message message : queue) {
                                producer1.send(message);
                    }
                }

                producer1.flush();
                System.out.println("线程完成： "+ finalI);
            });
        }
        executorService.shutdown();
        try {
            //等待20分钟
            executorService.awaitTermination(20, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

//        DefaultMessageFactory defaultMessageFactory = new DefaultMessageFactory();
//        DefaultProducer producer = new DefaultProducer(properties);
//        Message message1 = defaultMessageFactory.createBytesMessageToQueue("QUEUE1",new byte[]{11,22,33});
//        message1.putHeaders("header","1");
//        message1.putProperties("properties1","2");
//        message1.putProperties("properties2",2);
//        message1.putProperties("properties3",2L);
//        message1.putProperties("properties4",0.2);
//
//        Message message2 = defaultMessageFactory.createBytesMessageToQueue("TOPIC1",new byte[]{11,22,33});
//        message2.putHeaders("header","1");
//        message2.putProperties("properties","2");
//
//        producer.send(message1);
//        producer.send(message2);

//        MessageStore.getInstance(null).flush();

        long end = System.currentTimeMillis();

        long T1 = end - start;

        System.out.println("Send Cost: " + T1);
    }
}

