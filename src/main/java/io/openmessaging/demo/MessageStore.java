package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class MessageStore {

    private static final long MAX_FREE_MEMORY = 1024 * 1024 * 1024L;
//    private static final long MAX_MESS_NUM = 1024 * 1024 * 10;
    private static final long MAX_MESS_NUM = 50000;
    private static final long SLEEP_TIME = 10;
    private static MessageStore instance;
    //    public static final String PATH = "E:/Major/Open-Messaging/";
    public static String PATH;
    public static final String FILE_NAME = "E:/Major/Open-Messaging/mess.dat";
    public static final String CONFIG_NAME = "congfig.dat";
    private boolean firstPull = true;
    private int finishedNum;
    private Map<String, Integer> topicMap = new ConcurrentHashMap<>(100);
    private Map<String, Long> position = new HashMap<>(100);
    private volatile long messNum;
    private volatile boolean flushing;
    private Map<String, RandomAccessFile> randomAccessFileMap = new ConcurrentHashMap<>(100);

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

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

//        缓存清理线程
//        while (true) {
//            try {
//                TimeUnit.MILLISECONDS.sleep(SLEEP_TIME);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            if (messNum > MAX_MESS_NUM) {
//                flush();
//            }
//        }
//        for (int i = 0; i < 5; i++) {
            executorService.execute(() -> {
                while (messNum == 0) {
                    while(messNum > 0) {
                        flush();
//                        synchronized (this) {
//                            this.notifyAll();
//                        }
//                        try {
//                            TimeUnit.MILLISECONDS.sleep(SLEEP_TIME);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
                    }
                }
            });
//        }


//        executorService.execute(this::flush);
    }

    //queue或topic大小，10M
    private static final long BUCKET_SIZE = 1024 * 1024 * 10;

    //message最大长度，256k
    private static final int MESSAGE_SIZE = 256 * 1024;

    //bucket指针
    private int bucketIdx;

    //message地址，指向结尾,从1开始记录
    private Map<String, CopyOnWriteArrayList<Long>> messAddr = new ConcurrentHashMap<>();

    //记录message指针
    private Map<String, Integer> messIdx = new HashMap<>(100);


    //queue或topics的文件起始位置
    private Map<String, Long> bucketAddr = new ConcurrentHashMap<>(100);

    private static MappedByteBuffer mappedByteBuffer;

    private static FileChannel fileChannel;

    private Map<String, FileChannel> fileChannelPool = new ConcurrentHashMap<>(100);

    private Map<String, ConcurrentLinkedQueue<Message>> resultMap = new ConcurrentHashMap<>(100);

    private ArrayList<Message> resultList = new ArrayList<>();

    private String bucket;

    private int consumerNum;

    private Map<String, ObjectOutputStream> objectOutputStreamMap = new ConcurrentHashMap<>(100);
    private ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    private Map<String, ByteArrayOutputStream> resultData = new ConcurrentHashMap<>(100);


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


//    public void storeConfig() throws IOException {
//        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
//        objectOutputStream.writeObject(messAddr);
//        objectOutputStream.writeObject(bucketAddr);
//        objectOutputStream.close();
//
////        MappedByteBuffer mappedByteBuffer = new RandomAccessFile(CONFIG_NAME, "rw").getChannel()
////                .map(FileChannel.MapMode.READ_WRITE, 100*BUCKET_SIZE, byteArrayOutputStream.size());
////        mappedByteBuffer.put(byteArrayOutputStream.toByteArray(), (int) (101*BUCKET_SIZE),byteArrayOutputStream.size());
//
//        long position = 100*BUCKET_SIZE;
//
////        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, byteArrayOutputStream.size());
////
////        mappedByteBuffer.put(byteArrayOutputStream.toByteArray());
//
//        randomAccessFile.seek(position);
//        randomAccessFile.write(byteArrayOutputStream.toByteArray());
//    }
//
//    @SuppressWarnings("unchecked")
//    public void loadConfig() throws IOException, ClassNotFoundException {
//
//        FileChannel fc = new RandomAccessFile(CONFIG_NAME, "r").getChannel();
//        MappedByteBuffer mappedByteBuffer = fc.map(FileChannel.MapMode.READ_ONLY, 0L, fc.size());
//        byte[] buffer = new byte[(int) fc.size()];
//        while (mappedByteBuffer.hasRemaining()) {
//            mappedByteBuffer.get(buffer);
//        }
//        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer);
//        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
//        messAddr = (Map<String, CopyOnWriteArrayList<Long>>) objectInputStream.readObject();
//        bucketAddr = (Map<String, Long>) objectInputStream.readObject();
//
//    }


    public void putMessage(String bucket, Message message) throws IOException {

//        synchronized (this) {
//            if (!objectOutputStreamMap.containsKey(bucket)) {
//                ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(PATH + bucket));
//                objectOutputStreamMap.put(bucket, objectOutputStream);
//            }
//        }
//
//        ObjectOutputStream objectOutputStream = objectOutputStreamMap.get(bucket);
//        objectOutputStream.writeObject(message);
//        objectOutputStream.flush();

        messNum++;

        if(!resultMap.containsKey(bucket)){
            resultMap.put(bucket, new ConcurrentLinkedQueue<>());
        }

        ConcurrentLinkedQueue<Message> queue = resultMap.get(bucket);

        queue.add(message);

//        while(messNum > 100000){
//            try {
//                synchronized (this) {
//                    while(messNum > 100000) {
//                        this.wait();
//                    }
//                }
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }




//        boolean success = false;
//        while(!success) {
//            try {
//                success = queue.add(message);
//            } catch (NullPointerException ignored) {
//            }
//        }

//        if(!resultData.containsKey(bucket)){
//            resultData.put(bucket, new ByteArrayOutputStream(100));
//        }
//
//        ByteArrayOutputStream byteArrayOutputStream = resultData.get(bucket);
//
//        if(!objectOutputStreamMap.containsKey(bucket)){
//            objectOutputStreamMap.put(bucket, new ObjectOutputStream(byteArrayOutputStream));
//        }
//
//        ObjectOutputStream objectOutputStream = objectOutputStreamMap.get(bucket);
//
//        objectOutputStream.writeObject(message);









//            if(Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory() < MAX_FREE_MEMORY) {
//                executorService.execute(() -> {
//                    try {
//                        for (String key : resultMap.keySet()) {
//                            RandomAccessFile randomAccessFile = new RandomAccessFile(PATH + key, "rw");
//                            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
//                            randomAccessFile.seek(position.getOrDefault(bucket, 0L));
//                            for (Message m : resultMap.get(bucket)) {
//                                objectOutputStream.writeObject(m);
//                            }
//                            randomAccessFile.write(byteArrayOutputStream.toByteArray());
//                            position.put(bucket, randomAccessFile.length());
//                            objectOutputStream.close();
//                            randomAccessFile.close();
//                        }
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                    resultMap.clear();
//                });
//            }


//        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(MESSAGE_SIZE);
//        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
//        objectOutputStream.writeObject(message);
//        objectOutputStream.close();

//        long size = byteArrayOutputStream.size();

//        if (!bucketAddr.containsKey(bucket)) {
//            bucketAddr.put(bucket, bucketIdx++ * BUCKET_SIZE);
//        }

//        if (!fileChannelPool.containsKey(bucket)) {
//            FileChannel fileChannel = new FileOutputStream(PATH + bucket).getChannel();
//            fileChannelPool.put(bucket,fileChannel);
//        }
//
//        fileChannel = fileChannelPool.get(bucket);
//
//        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
//
//        fileChannel.write(byteBuffer);


//        fileChannel.force(false);


//        CopyOnWriteArrayList<Long> messList = messAddr.get(bucket);
//        if (messList == null) {
//            messList = new CopyOnWriteArrayList<>(Collections.singleton(bucketAddr.get(bucket)));
//            messAddr.put(bucket, messList);
//        }

//        Integer index = messIdx.getOrDefault(bucket, 0);

//        long position = messList.get(index);

//        MappedByteBuffer mappedByteBuffer = new RandomAccessFile(FILE_NAME, "rw").getChannel()
//                .map(FileChannel.MapMode.READ_WRITE, position, size);
//        mappedByteBuffer.put(byteArrayOutputStream.toByteArray(),(int)position,(int)size);


//        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, size);
//
//        mappedByteBuffer.put(byteArrayOutputStream.toByteArray());

//        randomAccessFile.seek(position);
//        randomAccessFile.write(byteArrayOutputStream.toByteArray());
//
//        messList.add(position + size);
//
//        messIdx.put(bucket, index + 1);
//
//        messAddr.put(bucket, messList);

        //写入配置信息
//        storeConfig();

    }

//    public synchronized List<Message> pullMessage(String bucket,boolean finished) throws IOException, ClassNotFoundException {
//
//        //初始化mess指针，测试用
//         /* if (firstPull) {
//            for (Map.Entry<String, Integer> entry : messIdx.entrySet()) {
//                entry.setValue(0);
//            }
//            firstPull = false;
//        }*/
//
//         //初始化参数
////         if(firstPull) {
////             loadConfig();
////             firstPull = false;
////         }
//
//        consumerNum ++;
//
//        if(this.bucket==null){
//            this.bucket = bucket;
//        }
//
//        if (!this.bucket.equals(bucket)) {
//            return null;
//        }
//
//        //第一次读取bucket，缓存整个bucket
//        if (resultList.size() == 0) {
////            FileChannel fc = new RandomAccessFile(PATH + bucket,"r").getChannel();
////            MappedByteBuffer mappedByteBuffer = fc.map(FileChannel.MapMode.READ_ONLY,0L,fc.size());
//            FileInputStream fileInputStream = new FileInputStream(PATH + bucket);
//            System.out.println(fileInputStream.available());
//            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
//            System.out.println(objectInputStream.available());
////            Message message = (Message) objectInputStream.readObject();
////            while(message != null) {
////                resultList.add(message);
////                message = (Message) objectInputStream.readObject();
////            }
//            while (fileInputStream.available() > 0) {
//                resultList.add((Message) objectInputStream.readObject());
//            }
//            objectInputStream.close();
//            fileInputStream.close();
//        }
//
////        if(finished){
////            bucket = null;
////            resultList.clear();
////            notifyAll();
////            return null;
////        }
//
//        if (finished) {
//            consumerNum --;
//            return null;
//        }
//
//        if(consumerNum == 0){
//            this.bucket = null;
//            resultList.clear();
//            this.notifyAll();
//            return null;
//        }
//
//        return resultList;
////        return resultList.get(index);
//
//
//
//
//
//
////        CopyOnWriteArrayList<Long> messList = messAddr.get(bucket);
////
////        if (index == (messList.size() - 1)) {
////            return null;
////        }
////
////        long size = messList.get(index + 1) - messList.get(index);
////        long position = messList.get(index);
////
////        MappedByteBuffer mappedByteBuffer = new RandomAccessFile(FILE_NAME, "r").getChannel()
////                .map(FileChannel.MapMode.READ_ONLY, position, size);
////
////        byte[] buffer = new byte[(int) (size)];
////        while (mappedByteBuffer.hasRemaining()) {
////            mappedByteBuffer.get(buffer);
////        }
////
////        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer);
////
////        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
////
////        return (Message) objectInputStream.readObject();
//
//    }

    public synchronized void flush() {
        if(messNum == 0){
            return;
        }
        System.out.println("刷新到硬盘");
        long start = System.currentTimeMillis();
//        resultMap = new ConcurrentHashMap<>();
        try {
            while(messNum > 0) {
                for (String key : resultMap.keySet()) {
                    if (!randomAccessFileMap.containsKey(key)) {
                        randomAccessFileMap.put(key, new RandomAccessFile(PATH + key, "rw"));
                    }
                    RandomAccessFile randomAccessFile = randomAccessFileMap.get(key);

//                    if (!objectOutputStreamMap.containsKey(key)) {
//                        objectOutputStreamMap.put(key, new ObjectOutputStream(byteArrayOutputStream));
//                    }
//                    ObjectOutputStream objectOutputStream = objectOutputStreamMap.get(key);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
                    randomAccessFile.seek(position.getOrDefault(key, 0L));

//                for (Message m : copyMap.get(key)) {
//                    objectOutputStream.writeObject(m);
//                }

                    while (!resultMap.get(key).isEmpty()) {
                        Message message = resultMap.get(key).poll();
                        objectOutputStream.writeObject(message);
                        messNum--;
                    }

                    objectOutputStream.close();
                    randomAccessFile.write(byteArrayOutputStream.toByteArray());
                    position.put(key, randomAccessFile.length());

                    byteArrayOutputStream = new ByteArrayOutputStream(100);
//                objectOutputStream.close();
//                randomAccessFile.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("本次硬盘刷新时间："+ (end - start));


//        Map<String, ByteArrayOutputStream> copyMap = resultData;
//        resultData = new ConcurrentHashMap<>(100);
//        messNum = 0;
//        try {
//            for (String key : copyMap.keySet()) {
//                if(!randomAccessFileMap.containsKey(key)){
//                    randomAccessFileMap.put(key, new RandomAccessFile(PATH + key, "rw"));
//                }
//                RandomAccessFile randomAccessFile = randomAccessFileMap.get(key);
//                randomAccessFile.seek(position.getOrDefault(key, 0L));
//                randomAccessFile.write(copyMap.get(key).toByteArray());
//                position.put(key, randomAccessFile.length());
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        long end = System.currentTimeMillis();
//        System.out.println("本次硬盘刷新时间："+ (end - start));
//    }

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
    }



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


}

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

