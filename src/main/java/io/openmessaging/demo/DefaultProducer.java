package io.openmessaging.demo;

import io.openmessaging.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

public class DefaultProducer implements Producer {
    private static Random random = new Random(System.currentTimeMillis());
    public static final int MESS_MAX = 10000;
    public static final int BUCKET_SIZE = 1024 * 1024 * 100;
    private static final int CACHE_SIZE = 1024 * 512                                                                                                                                                                                                                                                                                                                                        ;
//    private static final int CACHE_SIZE = 1024 * 512 * (random.nextInt(5) + 1);
    private static int level = 1;
//    private static final int CACHE_SIZE = 1024 * 512 * level++;
//    private static final int CACHE_SIZE = 1024 * 1024 * 5;
    //    private static final long SLEEP_TIME = 10;
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private MessageStore messageStore;
//    private Map<String, LinkedList<Message>> resultMap = new HashMap<>(100);
//    private static Map<String, Long> position = new ConcurrentHashMap<>(100);
//    private Map<String, RandomAccessFile> randomAccessFileMap = new HashMap<>(100);
    private static String PATH;

    private KeyValue properties;
//    private ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//
//    private Map<String, MappedByteBuffer> mappedByteBufferMap = new HashMap<>();

    private Map<String, ByteArrayOutputStream> resultData = new HashMap<>(100);

    private int messNum;


    private Deflater compresser = new Deflater(Deflater.BEST_SPEED);
    private Map<String, DeflaterOutputStream> deflaterOuputStreamMap = new HashMap<>(100);
    private Map<String, byte[]> deflateMap = new HashMap<>(100);
    private Map<String,Integer> deflaterSizeMap = new HashMap<>(100);

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
        PATH = properties.getString("STORE_PATH") + "/";

        messageStore = MessageStore.getInstance(properties.getString("STORE_PATH"));


//        executorService.execute(() -> {
//            try {
//                TimeUnit.MILLISECONDS.sleep(SLEEP_TIME);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            flush();
//        });

    }


    @Override
    public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {

        return messageFactory.createBytesMessageToTopic(topic, body);
    }

    @Override
    public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {

        return messageFactory.createBytesMessageToQueue(queue, body);
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public void send(Message message) {
//        if (message == null) throw new ClientOMSException("Message should not be null");
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
//        if ((topic == null && queue == null) || (topic != null && queue != null)) {
//            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", queue, topic));
//        }

        String bucket = topic == null ? queue : topic;

//        RandomAccessFile版
//        if (!resultMap.containsKey(bucket)) {
//            resultMap.put(bucket, new LinkedList<>());
//        }
//
//        resultMap.get(bucket).add(message);
//        messNum++;
//
//        if (messNum > MESS_MAX) {
//            flush();
//        }


        //MappedByteBuffer版

//        try {
//
//            byte[] bytes = MessageUtil.write(message);
//
//            if (!mappedByteBufferMap.containsKey(bucket)) {
//                mappedByteBufferMap.put(bucket,
//                        new RandomAccessFile(PATH + bucket, "rw")
//                                .getChannel().map(FileChannel.MapMode.READ_WRITE, 0, BUCKET_SIZE));
//            }
//            MappedByteBuffer mappedByteBuffer = mappedByteBufferMap.get(bucket);
//            mappedByteBuffer.put(bytes);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//

        //交到messagestore统一处理
//        messageStore.putMessage(bucket, message);
//        messageStore.putMessage(bucket, MessageUtil.write(message));



        //缓存数据再交ms
        if (!resultData.containsKey(bucket)) {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(CACHE_SIZE);
            resultData.put(bucket, byteArrayOutputStream);
        }
        byte[] bytes = MessageUtil.write(message);
        ByteArrayOutputStream byteArrayOutputStream = resultData.get(bucket);
        if(CACHE_SIZE - byteArrayOutputStream.size() < bytes.length){
            messageStore.flush(resultData);
        }
        try {
            byteArrayOutputStream.write(bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }


        //缓存数据再交ms(压缩版)
//        if (!deflateMap.containsKey(bucket)) {
//            byte[] deflateBuf = new byte[CACHE_SIZE];
//            deflateMap.put(bucket, deflateBuf);
//            deflaterSizeMap.put(bucket, 0);
//        }
//        byte[] bytes = MessageUtil.write(message);
//        int deflaterSize = deflaterSizeMap.get(bucket);
//        if(CACHE_SIZE - deflaterSize < 100){
//            messageStore.flush(deflateMap,deflaterSizeMap);
//            deflaterSize = 0;
//        }
//        byte[] deflaterBuf = deflateMap.get(bucket);
//        compresser.setInput(bytes);
//        compresser.finish();
//        deflaterSize += compresser.deflate(deflaterBuf,deflaterSize,100);
//        compresser.reset();
//
//        deflaterSizeMap.put(bucket, deflaterSize);
    }

    @Override
    public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Promise<Void> sendAsync(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    //用于被kill之前刷新到硬盘
    @Override
    public void flush() {
        messageStore.flush(resultData);
        CyclicBarrier cyclicBarrier = messageStore.getCyclicBarrier();
        try {
            cyclicBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
//        messageStore.flush(deflateMap,deflaterSizeMap);
//        System.out.println("刷新到硬盘");
//        long start = System.currentTimeMillis();
        //原版
//        try {
//            for (String key : resultMap.keySet()) {
//                if (!randomAccessFileMap.containsKey(key)) {
//                    randomAccessFileMap.put(key, new RandomAccessFile(PATH + key, "rw"));
//                }
//                RandomAccessFile randomAccessFile = randomAccessFileMap.get(key);
//
//                while (!resultMap.get(key).isEmpty()) {
//                    Message message = resultMap.get(key).poll();
//                    byte[] bytes = MessageUtil.write(message);
//                    byteArrayOutputStream.write(bytes);
//                    message = null;
//                }
//                synchronized (MessageStore.class) {
//                    randomAccessFile.skipBytes((int) ((long) position.getOrDefault(key, 0L)));
//                    randomAccessFile.write(byteArrayOutputStream.toByteArray());
//                    position.put(key, randomAccessFile.length());
//                }
//                byteArrayOutputStream.reset();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        messNum = 0;


        //修改版
//        synchronized (MessageStore.class) {
//            try {
//                for (String key : resultMap.keySet()) {
//                    if (!randomAccessFileMap.containsKey(key)) {
//                        randomAccessFileMap.put(key, new RandomAccessFile(PATH + key, "rw"));
//                    }
//                    RandomAccessFile randomAccessFile = randomAccessFileMap.get(key);
//
//                    while (!resultMap.get(key).isEmpty()) {
//                        Message message = resultMap.get(key).poll();
//                        byte[] bytes = MessageUtil.write(message);
//                        byteArrayOutputStream.write(bytes);
//                        message = null;
//                    }
//    //                synchronized (MessageStore.class) {
//    //                    randomAccessFile.skipBytes((int) ((long) position.getOrDefault(key, 0L)));
//                        randomAccessFile.write(byteArrayOutputStream.toByteArray());
//    //                    position.put(key, randomAccessFile.length());
//    //                }
//                    byteArrayOutputStream.reset();
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            messNum = 0;
//        }


//        long end = System.currentTimeMillis();
//        System.out.println("本次硬盘刷新时间：" + (end - start));
    }
}
