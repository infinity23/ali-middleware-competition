package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    //    private Map<String, Integer> topicMap = new ConcurrentHashMap<>(100);
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

    public MessageStore(String path){
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
//            executorService.execute(() -> {
//                while (messNum == 0) {
//                    while(messNum > 0) {
//                        flush();
////                        synchronized (this) {
////                            this.notifyAll();
////                        }
////                        try {
////                            TimeUnit.MILLISECONDS.sleep(SLEEP_TIME);
////                        } catch (InterruptedException e) {
////                            e.printStackTrace();
////                        }
//                    }
//                }
//            });
//        }


//        executorService.execute(this::flush);

        byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    private Map<String, ObjectOutputStream> objectOutputStreamMap = new ConcurrentHashMap<>(100);

//    private Map<String, ByteArrayOutputStream> resultData = new ConcurrentHashMap<>(100);
    private Map<String, ConcurrentLinkedQueue<Message>> resultMap = new ConcurrentHashMap<>(100);
//    private Map<String, MappedByteBuffer> mappedByteBufferMap = new ConcurrentHashMap<>(100);



    private ByteArrayOutputStream byteArrayOutputStream;
    private ObjectOutputStream objectOutputStream;


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


//    public void storeConfig() throws IOException {
//        ByteArrayOutputStream localByteArrayOutputStream = new ByteArrayOutputStream();
//        ObjectOutputStream localObjectOutputStream = new ObjectOutputStream(localByteArrayOutputStream);
//        localObjectOutputStream.writeObject(messAddr);
//        localObjectOutputStream.writeObject(bucketAddr);
//        localObjectOutputStream.close();
//
////        MappedByteBuffer mappedByteBuffer = new RandomAccessFile(CONFIG_NAME, "rw").getChannel()
////                .map(FileChannel.MapMode.READ_WRITE, 100*BUCKET_SIZE, localByteArrayOutputStream.size());
////        mappedByteBuffer.put(localByteArrayOutputStream.toByteArray(), (int) (101*BUCKET_SIZE),localByteArrayOutputStream.size());
//
//        long position = 100*BUCKET_SIZE;
//
////        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, localByteArrayOutputStream.size());
////
////        mappedByteBuffer.put(localByteArrayOutputStream.toByteArray());
//
//        randomAccessFile.seek(position);
//        randomAccessFile.write(localByteArrayOutputStream.toByteArray());
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


    public void putMessage(String bucket, Message message) {

//        synchronized (this) {
//            if (!objectOutputStreamMap.containsKey(bucket)) {
//                ObjectOutputStream localObjectOutputStream = new ObjectOutputStream(new FileOutputStream(PATH + bucket));
//                objectOutputStreamMap.put(bucket, localObjectOutputStream);
//            }
//        }
//
//        ObjectOutputStream localObjectOutputStream = objectOutputStreamMap.get(bucket);
//        localObjectOutputStream.writeObject(message);
//        localObjectOutputStream.flush();


        //直接缓存版本
        messNum++;

        if (!resultMap.containsKey(bucket)) {
            resultMap.put(bucket, new ConcurrentLinkedQueue<>());
        }

        ConcurrentLinkedQueue<Message> queue = resultMap.get(bucket);

        queue.add(message);

        while (messNum > 1000000) {
            synchronized (this) {
                while (messNum > 1000000) {
                    flush();
                }
            }
        }

        //先转换为数据，缓存数据版本

//        messNum++;
//        try {
//            if (!resultData.containsKey(bucket)) {
//                resultData.put(bucket, new ByteArrayOutputStream());
//            }
//
//            ByteArrayOutputStream localByteArrayOutputStream = resultData.get(bucket);
//
//            if (!objectOutputStreamMap.containsKey(bucket)) {
//                objectOutputStreamMap.put(bucket, new ObjectOutputStream(localByteArrayOutputStream));
//            }
//
//            ObjectOutputStream localObjectOutputStream = objectOutputStreamMap.get(bucket);
//
//            localObjectOutputStream.writeObject(message);
//
//            while (messNum > 100000) {
//                synchronized (this) {
//                    while (messNum > 100000) {
//                        flush();
//                    }
//                }
//            }
//        }catch (IOException e){
//            e.printStackTrace();
//        }


        //直接写到mappedbuffer版本
//            localObjectOutputStream.get().writeObject(message);
//
//            if(!mappedByteBufferMap.containsKey(bucket)){
//                MappedByteBuffer mappedByteBuffer = new RandomAccessFile(PATH+bucket,"rw").getChannel().map(FileChannel.MapMode.READ_WRITE,0L,1024*1024*100);
//                mappedByteBufferMap.put(bucket,mappedByteBuffer);
//            }
//            MappedByteBuffer mappedByteBuffer = mappedByteBufferMap.get(bucket);
//
//            mappedByteBuffer.put(localByteArrayOutputStream.get().toByteArray());
//
//            localByteArrayOutputStream.get().reset();
//
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


//        ByteArrayOutputStream localByteArrayOutputStream = new ByteArrayOutputStream(MESSAGE_SIZE);
//        ObjectOutputStream localObjectOutputStream = new ObjectOutputStream(localByteArrayOutputStream);
//        localObjectOutputStream.writeObject(message);
//        localObjectOutputStream.close();

//        long size = localByteArrayOutputStream.size();

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
//        ByteBuffer byteBuffer = ByteBuffer.wrap(localByteArrayOutputStream.toByteArray());
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
//        mappedByteBuffer.put(localByteArrayOutputStream.toByteArray(),(int)position,(int)size);


//        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, size);
//
//        mappedByteBuffer.put(localByteArrayOutputStream.toByteArray());

//        randomAccessFile.seek(position);
//        randomAccessFile.write(localByteArrayOutputStream.toByteArray());
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

        long writeObjectTime = 0;


        //对应直接缓存版本
        if (messNum == 0) {
            return;
        }
        System.out.println("刷新到硬盘");
        long start = System.currentTimeMillis();
        try {
                for (String key : resultMap.keySet()) {
                    if (!randomAccessFileMap.containsKey(key)) {
                        randomAccessFileMap.put(key, new RandomAccessFile(PATH + key, "rw"));
                    }
                    RandomAccessFile randomAccessFile = randomAccessFileMap.get(key);

//                    if (!objectOutputStreamMap.containsKey(key)) {
//                        objectOutputStreamMap.put(key, new ObjectOutputStream(byteArrayOutputStream));
//                    }
//                    ObjectOutputStream objectOutputStream = objectOutputStreamMap.get(key);

                    randomAccessFile.skipBytes(Math.toIntExact(position.getOrDefault(key, 0L)));

//                for (Message m : copyMap.get(key)) {
//                    localObjectOutputStream.writeObject(m);
//                }

//                    long writeObjectStart = System.currentTimeMillis();
//                    while (!resultMap.get(key).isEmpty()) {
//                        Message message = resultMap.get(key).poll();
//                        objectOutputStream.writeObject(message);
//                        messNum--;
//                        message = null;
//                    }
//                    objectOutputStream.flush();
//                    long writeObjectEnd = System.currentTimeMillis();

//                    writeObjectTime += writeObjectEnd - writeObjectStart;

//                    ByteBuffer byteBuffer = ByteBuffer.allocate(1024*1024*100);

                    while (!resultMap.get(key).isEmpty()) {
                        Message message = resultMap.get(key).poll();
                        byteArrayOutputStream.write(((DefaultBytesMessage)message).getBytes());
                        messNum--;
                        message = null;
                    }

                    randomAccessFile.write(byteArrayOutputStream.toByteArray());

                    position.put(key, randomAccessFile.length());

                    byteArrayOutputStream.reset();
//                localObjectOutputStream.close();
//                randomAccessFile.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("本次硬盘刷新时间：" + (end - start));
//        System.out.println("WriteObjectTime ：" + (writeObjectTime));
    }
}


       // 对应缓存数据版本
//        System.out.println("刷新到硬盘");
//        long start = System.currentTimeMillis();
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
//        copyMap = null;
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

