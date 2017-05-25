package io.openmessaging.demo;

import io.openmessaging.*;
import org.junit.Assert;

import java.util.Collections;
import java.util.concurrent.*;

public class PollTester {


    private volatile static int n;

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newCachedThreadPool();
        KeyValue properties = new DefaultKeyValue();

        properties.put("STORE_PATH", "E:/Major/Open-Messaging");

        ConcurrentHashMap<String, ConcurrentLinkedQueue<Message>> data = DataProducer.produce();

        long startConsumer = System.currentTimeMillis();
        System.out.println("测试开始");

        for (int i = 0; i < 10; i++) {
            int finalI = i;
            executorService.execute(() -> {
                PullConsumer consumer = new DefaultPullConsumer(properties);

                consumer.attachQueue("QUEUE" + finalI, Collections.singletonList("TOPIC" + finalI));

                ConcurrentLinkedQueue<Message> queueList = data.get("QUEUE" + finalI);
                ConcurrentLinkedQueue<Message> topicList = data.get("TOPIC" + finalI);


                Message message = consumer.poll();
                n++;
                while (message != null) {
                    String topic = message.headers().getString(MessageHeader.TOPIC);
                    String queue = message.headers().getString(MessageHeader.QUEUE);

                    if (topic != null) {
                        Assert.assertEquals("TOPIC" + finalI, topic);
                        Assert.assertArrayEquals(((BytesMessage) message).getBody(), ((BytesMessage) topicList.poll()).getBody());
                    } else {
                        Assert.assertEquals("QUEUE" + finalI, queue);
                        Assert.assertArrayEquals(((BytesMessage) message).getBody(), ((BytesMessage) queueList.poll()).getBody());
                    }
                    message = consumer.poll();
                    n ++;
                }
                System.out.println("线程" + finalI + "完成");

            });

        }


        executorService.shutdown();
        try {
            //等待20分钟
            executorService.awaitTermination(20, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

//        PullConsumer consumer = new DefaultPullConsumer(properties);
//        consumer.attachQueue("QUEUE1", Collections.singleton("TOPIC1"));
//        Message message1 = consumer.poll();
//        Message message2 = consumer.poll();

        long endConsumer = System.currentTimeMillis();
        long T2 = endConsumer - startConsumer;

        System.out.println("Pull: " + n);

        System.out.println("Poll Cost: " + T2);
    }

}

