package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageStore {

    private static final long MAX_FREE_MEMORY = 1024 * 1024 * 1024L;
    //    private static final long MAX_MESS_NUM = 1024 * 1024 * 10;
    private static final long MAX_MESS_NUM = 50000;
    private static final long SLEEP_TIME = 10;
    public static final int CACHE_SIZE = 1024 * 1024 * 10;
    private static MessageStore instance;
    //    public static final String PATH = "E:/Major/Open-Messaging/";
    public static String PATH;
    public static final String FILE_NAME = "E:/Major/Open-Messaging/mess.dat";
    public static final String CONFIG_NAME = "congfig.dat";
    private boolean firstPull = true;
    private int finishedNum;
    //    private Map<String, Integer> topicMap = new ConcurrentHashMap<>(100);
    private Map<String, Long> position = new HashMap<>(100);
//    private AtomicInteger messNum = new AtomicInteger();
    private AtomicInteger messNum = new AtomicInteger();
    private volatile long totalNum;
    private volatile boolean flushing;
    private Map<String, RandomAccessFile> randomAccessFileMap = new ConcurrentHashMap<>(100);

    private ExecutorService executorService = Executors.newSingleThreadExecutor();


//    private Map<String, ObjectOutputStream> objectOutputStreamMap = new ConcurrentHashMap<>(100);

    private Map<String, ByteBuffer> resultData = new ConcurrentHashMap<>(100);

    private Map<String, ConcurrentLinkedQueue<Message>> resultMap = new ConcurrentHashMap<>(100);
    private Map<String, MappedByteBuffer> mappedByteBufferMap = new ConcurrentHashMap<>(100);


    private ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();


    public static MessageStore getInstance(String path) {

        if (instance == null) {
            synchronized (MessageStore.class) {
                if (instance == null) {
                    instance = new MessageStore(path);
                }
            }
        }
        return instance;
    }

    public MessageStore(String path) {
        PATH = path + "/";

    }


//    private ThreadLocal<ByteArrayOutputStream> localByteArrayOutputStream = ThreadLocal.withInitial(() -> new ByteArrayOutputStream(100));
//    private ThreadLocal<ObjectOutputStream> localObjectOutputStream = ThreadLocal.withInitial(() -> {
//        try {
//            return new ObjectOutputStream(localByteArrayOutputStream.get());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return null;
//    });


//        try {
//            fileChannel = new RandomAccessFile(FILE_NAME, "rw").getChannel();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//
//        try {
//            randomAccessFile = new RandomAccessFile(FILE_NAME,"rw");
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }


    public synchronized void putMessage(String bucket, Message message) {


        //直接缓存版本
        if (!resultMap.containsKey(bucket)) {
            resultMap.put(bucket, new ConcurrentLinkedQueue<>());
        }

//        synchronized (this) {
            ConcurrentLinkedQueue<Message> queue = resultMap.get(bucket);
            queue.add(message);
//        }

        messNum.getAndIncrement();

        while (messNum.get()> 100000) {
            synchronized (this) {
                while (messNum.get() > 100000) {
                    flush();
                }
            }
        }


//        先转换为数据，缓存数据版本

//        messNum.getAndIncrement();
//        if (!resultData.containsKey(bucket)) {
//            resultData.put(bucket, ByteBuffer.allocateDirect(CACHE_SIZE));
//        }
//
//
//        synchronized (this) {
//            ByteBuffer byteBuffer = resultData.get(bucket);
//            byteBuffer.put(MessageUtil.write(message));
//        }
//
//
//        while (messNum.get() > 100000) {
//            synchronized (this) {
//                while (messNum.get() > 100000) {
//                    flush();
//                }
//            }
//        }


        //直接写到mappedbuffer版本

//        try {
//            if (!mappedByteBufferMap.containsKey(bucket)) {
//                MappedByteBuffer mappedByteBuffer = new RandomAccessFile(PATH + bucket, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0L, 1024 * 1024 * 120);
//                mappedByteBufferMap.put(bucket, mappedByteBuffer);
//            }
//
//            byte[] bytes = MessageUtil.write(message);
////            synchronized (this) {
//                MappedByteBuffer mappedByteBuffer = mappedByteBufferMap.get(bucket);
//                mappedByteBuffer.put(bytes);
////            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }

//    public Message pullMessage(String bucket){
//        try {
//            if (!mappedByteBufferMap.containsKey(bucket)) {
//                FileChannel fileChannel = new RandomAccessFile(PATH + bucket, "r").getChannel();
//                mappedByteBufferMap.put(bucket, fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size()));
//            }
//
//            MappedByteBuffer mappedByteBuffer = mappedByteBufferMap.get(bucket);
//
//            while (mappedByteBuffer.hasRemaining()) {
//                if (mappedByteBuffer.get() == 30) {
//                    position = mappedByteBuffer.position();
//                    mappedByteBuffer.reset();
//                    byte[] bytes = new byte[position - mark];
//                    mappedByteBuffer.get(bytes);
//                    mappedByteBuffer.mark();
//                    mark = mappedByteBuffer.position();
//
////                        long readToMessageStart = System.currentTimeMillis();
//                    Message message = MessageUtil.read(bytes);
////                        long readToMessageEnd = System.currentTimeMillis();
////                        readToMessageTime += readToMessageEnd - readToMessageStart;
//
//                    return message;
//                }
//            }
//
//
//
//        }catch (IOException e){
//            e.printStackTrace();
//        }
//
//
//
//    }


    public synchronized void flush() {

//        long writeObjectTime = 0;
//        long writeFileTime = 0;
//        long writeToByteTime = 0;


        //对应直接缓存版本
        if (messNum.get() == 0) {
            return;
        }
//        System.out.println("刷新到硬盘");
//        totalNum += messNum;
//        long start = System.currentTimeMillis();
        try {
                for (String key : resultMap.keySet()) {
                    if (!randomAccessFileMap.containsKey(key)) {
                        randomAccessFileMap.put(key, new RandomAccessFile(PATH + key, "rw"));
                    }
                    RandomAccessFile randomAccessFile = randomAccessFileMap.get(key);

//                    randomAccessFile.skipBytes((int)(long)(position.getOrDefault(key, 0L)));

//                    ByteBuffer byteBuffer = ByteBuffer.allocate(1024*1024*100);
//                    long writeObjectStart = System.currentTimeMillis();
                    while (!resultMap.get(key).isEmpty()) {
                        Message message = resultMap.get(key).poll();
//                        long writeToByteStart = System.nanoTime();
                        byte[] bytes = MessageUtil.write(message);
//                        long writeToByteEnd = System.nanoTime();
                        byteArrayOutputStream.write(bytes);
                        message = null;
//                        writeToByteTime += (writeToByteStart - writeToByteEnd);
                    }
//                    long writeObjectEnd = System.currentTimeMillis();

                    randomAccessFile.write(byteArrayOutputStream.toByteArray());

//                    long writeFileEnd = System.currentTimeMillis();


//                    position.put(key, randomAccessFile.length());

                    byteArrayOutputStream.reset();
//                localObjectOutputStream.close();
//                randomAccessFile.close();
//                 writeObjectTime += writeObjectEnd - writeObjectStart;
//                writeFileTime += writeFileEnd - writeObjectEnd;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        messNum.set(0);


//        long end = System.currentTimeMillis();
//        System.out.println("本次硬盘刷新时间：" + (end - start));
//        System.out.println("发送数目：" + totalNum);
//        System.out.println("WriteObjectTime ：" + (writeObjectTime));
//        System.out.println("WriteFileTime ：" + (writeFileTime));
//        System.out.println("writeToByteTime ：" + (TimeUnit.NANOSECONDS.toMillis(writeToByteTime)));


        // 对应缓存数据版本
////        System.out.println("刷新到硬盘");
////        long start = System.currentTimeMillis();
////        Map<String, ByteBuffer> copyMap = resultData;
////        resultData = new ConcurrentHashMap<>(100);
//        messNum.set(0);
//        try {
//            for (String key : resultData.keySet()) {
//                if (!mappedByteBufferMap.containsKey(key)) {
//                    mappedByteBufferMap.put(key, new RandomAccessFile(PATH + key, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE,0,1024*1024*500));
//                }
//                MappedByteBuffer mappedByteBuffer  = mappedByteBufferMap.get(key);
////                randomAccessFile.skipBytes((int)(long)position.getOrDefault(key, 0L));
//                ByteBuffer byteBuffer = resultData.get(key);
//
//                mappedByteBuffer.put(byteBuffer);
//
//                //清理bytebuffer
//                ((DirectBuffer)byteBuffer).cleaner().clean();
//
//                resultData.put(key,ByteBuffer.allocateDirect(CACHE_SIZE));
//
////                position.put(key, randomAccessFile.length());
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
////        long end = System.currentTimeMillis();
////        System.out.println("本次硬盘刷新时间：" + (end - start));




    }

}

//    public void setBuckets(List<String> topicList) {
//        for (String topic : topicList){
//            topicMap.put(topic,topicMap.get(topic) == null ? 0 : topicMap.get(topic) + 1);
//        }
//
//
//
//
//        try {
//            FileInputStream fileInputStream = new FileInputStream(PATH + bucket);
//            System.out.println(fileInputStream.available());
//            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
//            System.out.println(objectInputStream.available());
//            while (fileInputStream.available() > 0) {
//                resultList.add((Message) objectInputStream.readObject());
//            }
//            objectInputStream.close();
//            fileInputStream.close();
//        } catch (IOException | ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//
//


//
//    private void cleanCache(){
//        if (!flushing && messNum > MAX_MESS_NUM) {
//            synchronized (this) {
//                if(!flushing) {
//                    flushing = true;
//                    executorService.execute(() -> {
//                        flush();
//                        flushing = false;
//                    });
//                }
//            }
//        }
//    }


//class CleanCache implements Runnable {
//    private static CleanCache instance;
//    private Map<String, CopyOnWriteArrayList<Message>> resultMap;
//    private Map<String, Long> position = new HashMap<>(100);
//
//    public static CleanCache getInstance(Map<String, CopyOnWriteArrayList<Message>> resultMap) {
//        if (instance == null) {
//            synchronized (CleanCache.class) {
//                if (instance == null) {
//                    instance = new CleanCache(resultMap);
//                }
//            }
//        }
//        return instance;
//    }
//
//
//    public CleanCache(Map<String, CopyOnWriteArrayList<Message>> resultMap) {
//        this.resultMap = resultMap;
//    }
//
//    @Override
//    public void run() {
//
//    }
//}

