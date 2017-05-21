package io.openmessaging.demo;

import io.openmessaging.*;
import org.junit.Assert;

import java.util.Collections;
import java.util.concurrent.*;

public class PollTester {


    public static void main(String[] args) {
        ExecutorService executorService = Executors.newCachedThreadPool();
        KeyValue properties = new DefaultKeyValue();
        /*
        //实际测试时利用 STORE_PATH 传入存储路径
        //所有producer和consumer的STORE_PATH都是一样的，选手可以自由在该路径下创建文件
         */
        properties.put("STORE_PATH", "E:/Major/Open-Messaging");

        //这个测试程序的测试逻辑与实际评测相似，但注意这里是单线程的，实际测试时会是多线程的，并且发送完之后会Kill进程，再起消费逻辑
        //构造测试数据
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
                int n = 0;
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
                    n++;
                    if(n%100 == 0) {
                        System.out.println("线程" + finalI + "完成(百条)：  " +n/100);
                    }
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
        long endConsumer = System.currentTimeMillis();
        long T2 = endConsumer - startConsumer;
//            System.out.println(String.format("Team1 cost:%d ms tps:%d q/ms", T2 + T1, (queue1Offset + topic1Offset)/(T1 + T2)));

        System.out.println("Poll Cost: " + T2);
    }

}

