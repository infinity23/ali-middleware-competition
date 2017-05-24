package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PollTester {


    public static void main(String[] args) {
        ExecutorService executorService = Executors.newCachedThreadPool();
        KeyValue properties = new DefaultKeyValue();

        properties.put("STORE_PATH", "E:/Major/Open-Messaging");

//        ConcurrentHashMap<String, ConcurrentLinkedQueue<Message>> data = DataProducer.produce();

        long startConsumer = System.currentTimeMillis();
        System.out.println("测试开始");

//        for (int i = 0; i < 10; i++) {
//            int finalI = i;
//            executorService.execute(() -> {
//                PullConsumer consumer = new DefaultPullConsumer(properties);
//                consumer.attachQueue("QUEUE" + finalI, Collections.singletonList("TOPIC" + finalI));
//
//                ConcurrentLinkedQueue<Message> queueList = data.get("QUEUE" + finalI);
//                ConcurrentLinkedQueue<Message> topicList = data.get("TOPIC" + finalI);
//
//
//                Message message = consumer.poll();
//                while (message != null) {
//                    String topic = message.headers().getString(MessageHeader.TOPIC);
//                    String queue = message.headers().getString(MessageHeader.QUEUE);
//
//                    if (topic != null) {
//                        Assert.assertEquals("TOPIC" + finalI, topic);
//                        Assert.assertArrayEquals(((BytesMessage) message).getBody(), ((BytesMessage) topicList.poll()).getBody());
//                    } else {
//                        Assert.assertEquals("QUEUE" + finalI, queue);
//                        Assert.assertArrayEquals(((BytesMessage) message).getBody(), ((BytesMessage) queueList.poll()).getBody());
//                    }
//                    message = consumer.poll();
//                }
//                System.out.println("线程" + finalI + "完成");
//
//            });
//
//        }
//
//
//        executorService.shutdown();
//        try {
//            //等待20分钟
//            executorService.awaitTermination(20, TimeUnit.MINUTES);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        PullConsumer consumer = new DefaultPullConsumer(properties);
        consumer.attachQueue("QUEUE1", Collections.singleton("TOPIC1"));
        Message message1 = consumer.poll();
        Message message2 = consumer.poll();

        long endConsumer = System.currentTimeMillis();
        long T2 = endConsumer - startConsumer;
//            System.out.println(String.format("Team1 cost:%d ms tps:%d q/ms", T2 + T1, (queue1Offset + topic1Offset)/(T1 + T2)));

        System.out.println("Poll Cost: " + T2);
    }

}

