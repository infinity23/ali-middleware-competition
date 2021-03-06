package io.openmessaging.demo;

import io.openmessaging.*;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.zip.Deflater;

import static io.openmessaging.demo.Constant.CACHE_SIZE;

public class DefaultProducer implements Producer {
//        private static Random random = new Random(System.currentTimeMillis());
//    public static final int MESS_MAX = 10000;
//    public static final int BUCKET_SIZE = 1024 * 1024 * 100;
    //    private static final int CACHE_SIZE = 1024 * 1024 * 2;
//        private static final int CACHE_SIZE = 1024 * 512 * (random.nextInt(5) + 1);
//    private static int level = 1;
//    private final int CACHE_SIZE = 1024 * 512 * (level++);
    //    private static final int CACHE_SIZE = 1024 * 1024 * 5;
    //    private static final long SLEEP_TIME = 10;
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private MessageStore messageStore;
    //    private Map<String, LinkedList<Message>> resultMap = new HashMap<>(100);
//    private static Map<String, Long> position = new ConcurrentHashMap<>(100);
//    private Map<String, RandomAccessFile> randomAccessFileMap = new HashMap<>(100);
//    private static String PATH;

    private KeyValue properties;
//    private ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//
//    private Map<String, MappedByteBuffer> mappedByteBufferMap = new HashMap<>();

//    private Map<String, ByteBuffer> resultData = new HashMap<>(100);
//
//    private int messNum;


    private Deflater compresser = new Deflater(Deflater.BEST_SPEED);
    private Map<String, ByteBuffer> cacheMap = new HashMap<>(100);
    private byte[] deflaterBuf = new byte[CACHE_SIZE];
//    private ExecutorService executorService = Executors.newCachedThreadPool();
//    private BlockingQueue<DeflateAndWrite> threadQueue = new LinkedBlockingQueue<>(2);
//    private Thread dispatureThread;


    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
//        PATH = properties.getString("STORE_PATH") + "/";

        messageStore = MessageStore.getInstance(properties.getString("STORE_PATH"));


//        executorService.execute(() -> {
//            try {
//                TimeUnit.MILLISECONDS.sleep(SLEEP_TIME);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            flush();
//        });


//        executorService.execute(new Runnable() {
//            @Override
//            public void run() {
//                dispatureThread = Thread.currentThread();
//                Thread.currentThread().setPriority(6);
//                try {
//                    while (true) {
//                        DeflateAndWrite deflateAndWrite = threadQueue.take();
//                        deflateAndWrite.run();
//                    }
//                } catch (InterruptedException e) {
//
//                }
//            }
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
//        if (!resultData.containsKey(bucket)) {
//            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(CACHE_SIZE);
//            resultData.put(bucket, byteBuffer);
//        }
//        byte[] bytes = MessageUtil.write(message);
//        ByteBuffer byteBuffer = resultData.get(bucket);
//        if (CACHE_SIZE - byteBuffer.position() < bytes.length) {
//            messageStore.flush(bucket,byteBuffer);
//        }
//        byteBuffer.put(bytes);


        //缓存数据再交ms(压缩版)
        if (!cacheMap.containsKey(bucket)) {
//            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(CACHE_SIZE);
            ByteBuffer byteBuffer = ByteBuffer.allocate(CACHE_SIZE);
            cacheMap.put(bucket, byteBuffer);
        }
        ByteBuffer byteBuffer = cacheMap.get(bucket);
        byte[] bytes = MessageUtil.write(message);
        if (CACHE_SIZE - byteBuffer.position() < bytes.length) {
//            try {
//                threadQueue.put(new DeflateAndWrite(byteBuffer,bucket));
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            byteBuffer = ByteBuffer.allocate(CACHE_SIZE);

            compresser.setInput(byteBuffer.array(), 0, byteBuffer.position());
            compresser.finish();
            int size = compresser.deflate(deflaterBuf);
            compresser.reset();
            messageStore.writeToFile(bucket, deflaterBuf, size);
            byteBuffer.clear();

        }
        byteBuffer.put(bytes);
    }

//    private class DeflateAndWrite implements Runnable{
////        Thread thread = Thread.currentThread();
//        private ByteBuffer finalByteBuffer;
//        private String bucket;
//
//        public DeflateAndWrite(ByteBuffer finalByteBuffer, String bucket) {
//            this.finalByteBuffer = finalByteBuffer;
//            this.bucket = bucket;
//        }
//
//        @Override
//        public void run() {
////            thread.setPriority(Thread.MAX_PRIORITY);
//            compresser.setInput(finalByteBuffer.array(),0, finalByteBuffer.position());
//            compresser.finish();
//            int size = compresser.deflate(deflaterBuf);
//            compresser.reset();
//            messageStore.writeToFile(bucket,deflaterBuf,size);
//        }
//    }

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
//        System.out.println("最后flush");
//        集中压缩版
//        messageStore.flush(resultData);
//        for(Map.Entry<String, ByteBuffer> entry : resultData.entrySet()){
//            messageStore.flush(entry.getKey(), entry.getValue());
//        }
//        CyclicBarrier cyclicBarrier = messageStore.getCyclicBarrier();
//        try {
//            cyclicBarrier.await();
//        } catch (InterruptedException | BrokenBarrierException e) {
//            e.printStackTrace();
//        }

        //分散压缩版

//        dispatureThread.interrupt();

        for (Map.Entry<String, ByteBuffer> entry : cacheMap.entrySet()) {
            ByteBuffer byteBuffer = entry.getValue();
            compresser.setInput(byteBuffer.array(), 0, byteBuffer.position());
            compresser.finish();
            int size = compresser.deflate(deflaterBuf);
            compresser.reset();
            messageStore.writeToFile(entry.getKey(), deflaterBuf, size);
        }
        CyclicBarrier cyclicBarrier = messageStore.getCyclicBarrier();
        try {
            cyclicBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }


//        messageStore.flush(cacheMap,cacheSizeMap);
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
