package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.Deflater;

public class MessageStore {

    private static final long MAX_FREE_MEMORY = 1024 * 1024 * 1024L;
    //    private static final long MAX_MESS_NUM = 1024 * 1024 * 10;
    private static final long MAX_MESS_NUM = 50000;
    private static final long SLEEP_TIME = 10;
    public static final int CACHE_SIZE = 1024 * 512;
    //    public static final int FILE_BLOCK = 1024 * 1024 * 40;
    public static final int FILE_BLOCK = 1024 * 1024 * 10;
    public static final int DEFLATE_BLOCK = FILE_BLOCK / 5;
    private static final int APPEND_BLOCK = 1024 * 1024 * 10;
    private static MessageStore instance;
    //    public static final String PATH = "E:/Major/Open-Messaging/";
    public static String PATH;
    //    private Map<String, Integer> topicMap = new ConcurrentHashMap<>(100);
//    private Map<String, Long> position = new HashMap<>(100);
//    private AtomicInteger messNum = new AtomicInteger();
    //    private volatile long totalNum;
//    private volatile boolean flushing;
//
//    private ExecutorService executorService = Executors.newSingleThreadExecutor();


//    private Map<String, ObjectOutputStream> objectOutputStreamMap = new ConcurrentHashMap<>(100);

    //    private Map<String, ByteArrayOutputStream> resultData = new ConcurrentHashMap<>(100);
//    private Map<String, ConcurrentLinkedQueue<Message>> resultMap = new ConcurrentHashMap<>(100);
//    private Map<String, MappedByteBuffer> mappedByteBufferMap = new ConcurrentHashMap<>(100);

    private Map<String, RandomAccessFile> randomAccessFileMap = new ConcurrentHashMap<>(100);
    private Map<String, ByteArrayOutputStream> cacheMap = new HashMap<>(100);
    private Deflater deflater = new Deflater(1);
    private Map<String, ArrayList<Integer>> deflatePosition = new HashMap<>(100);

//    private ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();


    //    private Map<String, Integer> sortedMap = new HashMap<>(100);
//    private Set<String> bucketSet = new CopyOnWriteArraySet<>();

    //    private String bucket;
//    private Iterator<String> it;
//    private MappedByteBuffer mappedByteBuffer;
//    private FileChannel fileChannel;
//    private ArrayList<Message> messList = new ArrayList<>();

    private static AtomicInteger threadNum = new AtomicInteger(0);
    private static CyclicBarrier cyclicBarrier;
    //    private boolean done;
//    private ArrayList<byte[]> bytesList = new ArrayList<>();

    public synchronized CyclicBarrier getCyclicBarrier() {
        if (cyclicBarrier == null) {
            cyclicBarrier = new CyclicBarrier(threadNum.get(), new Runnable() {
                @Override
                public void run() {
                    end();
                }
            });
        }
        return cyclicBarrier;
    }
//    private byte[] cache;
//    private boolean hasFlush;
//    public boolean isDone() {
//        return done;
//    }
//
//    public byte[] getCache() {
//        return cache;
//    }

    //    public ArrayList<Message> getMessList() {
//        return messList;
//    }
//
//    public ArrayList<byte[]> getBytesList() {
//        return bytesList;
//    }
//
//    public String getBucket() {
//        return bucket;
//    }

//    public void setBucket(Collection<String> collection){
//        bucketSet.addAll(collection);
//        it = bucketSet.iterator();
//        treadNum.getAndIncrement();
//    }

//
//    public synchronized CyclicBarrier pollInit(Collection<String> collection){
//        bucketSet.addAll(collection);
//        it = bucketSet.iterator();
//        if(cyclicBarrier == null){
//            cyclicBarrier = new CyclicBarrier(treadNum, new Runnable() {
//                @Override
//                public void run() {
//                    if(messList!=null) {
////                        messList.clear();
//                        bytesList.clear();
//                    }
//                    if(!read()){
//                        done = true;
//                        cyclicBarrier.reset();
//                    }
//
//                }
//            });
//        }
//        return cyclicBarrier;
//    }


    public static MessageStore getInstance(String path) {

        threadNum.getAndIncrement();

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
//        readQueueNum();
    }

//    private void readQueueNum(){
//        File[] files = new File(PATH).listFiles();
//        for(File file : files){
//            if(file.getName().contains("QUEUE")){
//                treadNum ++;
//            }
//        }
//
//    }


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

//    public synchronized void putMessage(String bucket, byte[] bytes){
//        if (!resultData.containsKey(bucket)) {
//            resultData.put(bucket, new ByteArrayOutputStream(FILE_BLOCK));
//        }
////        synchronized (this) {
//            try {
//                ByteArrayOutputStream byteArrayOutputStream = resultData.get(bucket);
//                if(FILE_BLOCK - byteArrayOutputStream.size() < bytes.length){
//                    writeToFile(bucket,byteArrayOutputStream);
//                }
//                byteArrayOutputStream.write(bytes);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
////        }
//    }


    public void putMessage(String bucket, Message message) {


        //直接缓存版本
//        if (!resultMap.containsKey(bucket)) {
//            resultMap.put(bucket, new ConcurrentLinkedQueue<>());
//        }
//
////        synchronized (this) {
//            ConcurrentLinkedQueue<Message> queue = resultMap.get(bucket);
//            queue.add(message);
////        }
//
//        messNum.getAndIncrement();
//
//        while (messNum.get()> 100000) {
//            synchronized (this) {
//                while (messNum.get() > 100000) {
//                    flush();
//                }
//            }
//        }


//        先转换为数据，缓存数据版本

//        messNum.getAndIncrement();
//        if (!resultData.containsKey(bucket)) {
//            resultData.put(bucket, new ByteArrayOutputStream(FILE_BLOCK));
//        }
//
//
//        synchronized (this) {
//            try {
//                byte[] bytes = MessageUtil.write(message);
//                ByteArrayOutputStream byteArrayOutputStream = resultData.get(bucket);
//                if(FILE_BLOCK - byteArrayOutputStream.size() < bytes.length){
//                        writeToFile(bucket,byteArrayOutputStream);
//                }
//                byteArrayOutputStream.write(bytes);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }


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
//                MappedByteBuffer mappedByteBuffer = new RandomAccessFile(PATH + bucket, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0L, 1024 * 1024 * 40);
//                mappedByteBufferMap.put(bucket, mappedByteBuffer);
//            }
//
//            mappedByteBuffer = mappedByteBufferMap.get(bucket);
//            byte[] bytes = MessageUtil.write(message);
//            if ((mappedByteBuffer.capacity() - mappedByteBuffer.position()) < bytes.length) {
//                long lastPosition = position.getOrDefault(bucket,0L);
//                position.put(bucket, lastPosition + mappedByteBuffer.position());
//                FileChannel fileChannel = new RandomAccessFile(PATH + bucket, "rw").getChannel();
//                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, lastPosition + mappedByteBuffer.position(), 1024 * 1024 * 40);
//                mappedByteBufferMap.put(bucket, mappedByteBuffer);
//            }
//            mappedByteBuffer.put(bytes);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }


//    public synchronized boolean read(String bucket ,long position) {
//
//
////        try {
////            if(it.hasNext()) {
////                bucket = it.next();
////                fileChannel = new RandomAccessFile(PATH + bucket, "r").getChannel();
////                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
//////                mappedByteBuffer.load();
//////                mappedByteBuffer.mark();
////
////                int position = 0;
////                int mark = 0;
////
////                //读到cache
////                cache = new byte[(int) fileChannel.size()];
////                mappedByteBuffer.get(cache);
////
////
////                //读到messList
//////                while (mappedByteBuffer.hasRemaining()) {
//////                    if (mappedByteBuffer.get() == 29) {
//////                        position = mappedByteBuffer.position();
//////                        mappedByteBuffer.reset();
//////                        byte[] bytes = new byte[position - mark];
//////                        mappedByteBuffer.get(bytes);
//////                        mappedByteBuffer.mark();
//////                        mark = mappedByteBuffer.position();
//////                        messList.add(MessageUtil.read(bytes));
//////                    }
//////
//////                }
////
////                fileChannel.close();
////                ((DirectBuffer)mappedByteBuffer).cleaner().clean();
////                mappedByteBuffer = null;
////                return true;
////
////            }
////        } catch (IOException e) {
////            e.printStackTrace();
////        }
////        return false;
//
//
//
//        return  false;
//    }
//


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
//                if (mappedByteBuffer.get() == 29) {
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


    public synchronized void flush(Map<String, ByteArrayOutputStream> resultData) {
//
////        long writeObjectTime = 0;
////        long writeFileTime = 0;
////        long writeToByteTime = 0;
//
//
//        //对应直接缓存版本
////        if (messNum.get() == 0) {
////            return;
////        }
//////        System.out.println("刷新到硬盘");
//////        totalNum += messNum;
//////        long start = System.currentTimeMillis();
////        try {
////                for (String key : resultMap.keySet()) {
////                    if (!randomAccessFileMap.containsKey(key)) {
////                        randomAccessFileMap.put(key, new RandomAccessFile(PATH + key, "rw"));
////                    }
////                    RandomAccessFile randomAccessFile = randomAccessFileMap.get(key);
////
//////                    randomAccessFile.skipBytes((int)(long)(position.getOrDefault(key, 0L)));
////
//////                    ByteBuffer byteBuffer = ByteBuffer.allocate(1024*1024*100);
//////                    long writeObjectStart = System.currentTimeMillis();
////                    while (!resultMap.get(key).isEmpty()) {
////                        Message message = resultMap.get(key).poll();
//////                        long writeToByteStart = System.nanoTime();
////                        byte[] bytes = MessageUtil.write(message);
//////                        long writeToByteEnd = System.nanoTime();
////                        byteArrayOutputStream.write(bytes);
////                        message = null;
//////                        writeToByteTime += (writeToByteStart - writeToByteEnd);
////                    }
//////                    long writeObjectEnd = System.currentTimeMillis();
////
////                    randomAccessFile.write(byteArrayOutputStream.toByteArray());
////
//////                    long writeFileEnd = System.currentTimeMillis();
////
////
//////                    position.put(key, randomAccessFile.length());
////
////                    byteArrayOutputStream.reset();
//////                localObjectOutputStream.close();
//////                randomAccessFile.close();
//////                 writeObjectTime += writeObjectEnd - writeObjectStart;
//////                writeFileTime += writeFileEnd - writeObjectEnd;
////            }
////        } catch (IOException e) {
////            e.printStackTrace();
////        }
////        messNum.set(0);
//
//
////        long end = System.currentTimeMillis();
////        System.out.println("本次硬盘刷新时间：" + (end - start));
////        System.out.println("发送数目：" + totalNum);
////        System.out.println("WriteObjectTime ：" + (writeObjectTime));
////        System.out.println("WriteFileTime ：" + (writeFileTime));
////        System.out.println("writeToByteTime ：" + (TimeUnit.NANOSECONDS.toMillis(writeToByteTime)));
//
//
//        // 对应缓存数据版本
////        System.out.println("刷新到硬盘");
////        long start = System.currentTimeMillis();
//
////        messNum.set(0);
//        try {
//            for (String bucket : resultData.keySet()) {
//                if (!mappedByteBufferMap.containsKey(bucket)) {
//                    MappedByteBuffer mappedByteBuffer = new RandomAccessFile(PATH + bucket, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0L, FILE_BLOCK);
//                    mappedByteBufferMap.put(bucket, mappedByteBuffer);
//                }
//                mappedByteBuffer = mappedByteBufferMap.get(bucket);
//                if ((mappedByteBuffer.capacity() - mappedByteBuffer.position()) < CACHE_SIZE) {
//                    FileChannel fileChannel = new RandomAccessFile(PATH + bucket, "rw").getChannel();
//                    mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, fileChannel.size(), FILE_BLOCK);
//                    mappedByteBufferMap.put(bucket, mappedByteBuffer);
//                }
//
//                ByteArrayOutputStream byteArrayOutputStream = resultData.get(bucket);
//                mappedByteBuffer.put(byteArrayOutputStream.toByteArray());
//                byteArrayOutputStream.reset();
//            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
////        long end = System.currentTimeMillis();
////        System.out.println("本次硬盘刷新时间：" + (end - start));
//
//
//    }


//        // 对应缓存数据版本(压缩版)
        try {
            for (String bucket : resultData.keySet()) {
//                if (!mappedByteBufferMap.containsKey(bucket)) {
//                    MappedByteBuffer mappedByteBuffer = new RandomAccessFile(PATH + bucket, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0L, FILE_BLOCK);
//                    mappedByteBufferMap.put(bucket, mappedByteBuffer);
//                }
                if (!cacheMap.containsKey(bucket)) {
                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(FILE_BLOCK);
                    cacheMap.put(bucket, byteArrayOutputStream);
                }
                ByteArrayOutputStream cache = cacheMap.get(bucket);
                if (FILE_BLOCK - cache.size() < CACHE_SIZE) {
                    writeToFile(bucket, cache);
                }
                ByteArrayOutputStream byteArrayOutputStream = resultData.get(bucket);
                cache.write(byteArrayOutputStream.toByteArray());
                byteArrayOutputStream.reset();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
//
    }

    private void writeToFile(String bucket, ByteArrayOutputStream cache) throws IOException {
        if (!randomAccessFileMap.containsKey(bucket)) {
            randomAccessFileMap.put(bucket, new RandomAccessFile(PATH + bucket, "rw"));
            deflatePosition.put(bucket, new ArrayList<>());
        }
        RandomAccessFile randomAccessFile = randomAccessFileMap.get(bucket);
        ArrayList<Integer> list = deflatePosition.get(bucket);
//        MappedByteBuffer mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, randomAccessFile.length(), DEFLATE_BLOCK);
        byte[] deflateBuf = new byte[FILE_BLOCK];
        deflater.setInput(cache.toByteArray());
        deflater.finish();
        int deflateSize = deflater.deflate(deflateBuf);
        deflater.reset();
        randomAccessFile.write(deflateBuf,0,deflateSize);
        list.add(deflateSize);
//        mappedByteBuffer.put(deflateBuf);
//        randomAccessFile.close();
        cache.reset();
    }

    private void end() {

        for (Map.Entry<String, ByteArrayOutputStream> entry : cacheMap.entrySet()) {
            try {
                writeToFile(entry.getKey(), entry.getValue());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            FileOutputStream fileOutputStream = new FileOutputStream(PATH + "index");
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(deflatePosition);
            objectOutputStream.close();
            fileOutputStream.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //压缩版本
//    public synchronized void flush(Map<String, byte[]> deflateMap, Map<String, Integer> deflateSizeMap){
//        try {
//            for (String bucket : deflateMap.keySet()) {
//                if (!mappedByteBufferMap.containsKey(bucket)) {
//                    MappedByteBuffer mappedByteBuffer = new RandomAccessFile(PATH + bucket, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0L, FILE_BLOCK);
//                    mappedByteBufferMap.put(bucket, mappedByteBuffer);
//                }
//                mappedByteBuffer = mappedByteBufferMap.get(bucket);
//                if ((mappedByteBuffer.capacity() - mappedByteBuffer.position()) < CACHE_SIZE) {
//                    FileChannel fileChannel = new RandomAccessFile(PATH + bucket, "rw").getChannel();
//                    mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, fileChannel.size(), FILE_BLOCK);
//                    mappedByteBufferMap.put(bucket, mappedByteBuffer);
//                }
//
//                int deflateSize = deflateSizeMap.get(bucket);
//                byte[] deflateBuf = deflateMap.get(bucket);
//                mappedByteBuffer.put(deflateBuf,0,deflateSize);
//            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

}







