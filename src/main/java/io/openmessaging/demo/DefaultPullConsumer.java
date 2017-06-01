package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class DefaultPullConsumer implements PullConsumer {
    //    private static final int FILE_BLOCK = 1024 * 1024 * 40;
    private static final int FILE_BLOCK = 1024 * 1024 * 10;
    public static final int DEFLATE_BLOCK = FILE_BLOCK / 5;
    private static final int BYTES_CACHE = FILE_BLOCK * 2;
    private static final int APPEND_BLOCK = 1024 * 1024 * 10;
    public static final int MESS_CACHE = 50000;
    private KeyValue properties;
    private String queue;
    //    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();
    //    private HashMap<String, Integer> messIdx = new HashMap<>();
//
//    private int lastIndex = 0;
//    private List<Message> resultList;
    private String bucket;
    private Iterator<String> it;
    //    private int finishedNum;
//    private boolean first;
//    private List<String> topicList;
    private MappedByteBuffer mappedByteBuffer;
    private int mark = 0;
    private int position = 0;
    private int lastPositin = -1;

    private String PATH;
    private FileChannel fileChannel;

    private MessageStore messageStore;

    private Map<String, ArrayList<Integer>> deflatePosition;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private BlockingQueue<byte[]> cacheQueue = new LinkedBlockingQueue<>(2);
//    private BlockingQueue<byte[]> messQueue = new LinkedBlockingQueue<>(MESS_CACHE);

//    private ArrayList<Message> messList;
//    private ArrayList<byte[]> bytesList = new ArrayList<>();
//    private String bucket;

    private RandomAccessFile randomAccessFile;
    private ArrayList<Integer> positionList;
//    private Thread thisThread;

    private int n;

    private byte[] cache;
    private byte[] cache2;

    private int cached;


    private Inflater uncompressor = new Inflater();


    //    private CyclicBarrier cyclicBarrier;
//    private boolean done;
//    private Map<String, Long> positionMap = new HashMap<>(100);
    private long mPosition;
//
//    public void setCyclicBarrier(CyclicBarrier cyclicBarrier) {
//        this.cyclicBarrier = cyclicBarrier;
//    }

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        PATH = properties.getString("STORE_PATH") + "/";
        messageStore = MessageStore.getInstance(PATH);
//        thisThread = Thread.currentThread();
    }


    @Override
    public KeyValue properties() {
        return properties;
    }

    long readToMessageTime = 0;

    private int index;

    private byte[] thisCache;
    private boolean done;

    @Override
    public Message poll() {
        //对应MMP线程分离
//        try {
//            if(!done) {
//                return MessageUtil.read(messQueue.take());
//            }
//                return MessageUtil.read(messQueue.poll());
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        return null;

        //对应RAF分块线程分离
//        while(position < thisCache.length && thisCache[position] != 0){
////            //用于非一块大小
////                if(position%FILE_BLOCK == 0){
////                    lastPositin = position - 1;
////                }
//            if(thisCache[position] == 29){
//                byte[] bytes = new byte[position - lastPositin];
//                System.arraycopy(thisCache,lastPositin + 1,bytes,0,position - lastPositin);
//                lastPositin = position++;
//                return MessageUtil.read(bytes);
//            }
//            position ++;
//        }
//
//        try {
//            if(!done){
//                thisCache = cacheQueue.take();
//            }else {
//                thisCache = cacheQueue.poll();
//            }
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        if(thisCache != null){
//            position = 0;
//            lastPositin = -1;
//            while(position < thisCache.length){
//                if(thisCache[position] == 29){
//                    byte[] bytes = new byte[position - lastPositin];
//                    System.arraycopy(thisCache,lastPositin + 1,bytes,0,position - lastPositin);
//                    lastPositin = position++;
//                    return MessageUtil.read(bytes);
//            }
//            position++;
//            }
//        }
//
//        return null;


        //对应分段mmp
//        while (true) {
//            while (mappedByteBuffer.hasRemaining()) {
//                //用于非整数倍块大小
////                if(mappedByteBuffer.position()%FILE_BLOCK == 0){
////                    mappedByteBuffer.mark();
////                    mark = mappedByteBuffer.position();
////                }
//                if (mappedByteBuffer.get() == 29) {
//                    position = mappedByteBuffer.position();
//                    mappedByteBuffer.reset();
//                    byte[] bytes = new byte[position - mark];
//                    mappedByteBuffer.get(bytes);
//                    mappedByteBuffer.mark();
//                    mark = position;
//
//                    return MessageUtil.read(bytes);
//                }
//            }
//
//            if (!read()) {
//                break;
//            }
//            mark = 0;
//        }
//
//        return null;


        //对应集中式
//        try {
//
//            if (cyclicBarrier == null) {
//                cyclicBarrier = messageStore.pollInit(bucketList);
//                cyclicBarrier.await();
//            }
//
//            while (!messageStore.isDone()) {
//                while (!bucketList.contains(messageStore.getBucket())) {
//                    cyclicBarrier.await();
//                }
////                messList = messageStore.getMessList();
//                cache = messageStore.getCache();
//                while (position < cache.length) {
//                    if (cache[position] == 29) {
//                        byte[] bytes = new byte[position - lastPositin];
//                        System.arraycopy(cache, lastPositin + 1, bytes, 0, position - lastPositin);
//                        lastPositin = position++;
//                        return MessageUtil.read(bytes);
//                    }
//                    position++;
//                }
//                cyclicBarrier.await();
////                index = 0;
//                position = 0;
//                lastPositin = -1;
//            }
//
//
//        } catch (InterruptedException | BrokenBarrierException e) {
////            e.printStackTrace();
//        }
//        return null;


        //对应mmp读缓存
//        if(!bytesList.isEmpty()){
//            return MessageUtil.read(bytesList.poll());
//        }
//
//        if(read()){
//            return MessageUtil.read(bytesList.poll());
//        }
//
//        if(!bytesList.isEmpty()){
//            return MessageUtil.read(bytesList.poll());
//        }
//
//        return null;


        //缓存版,从byte[]读取

        while (position < cache.length && cache[position] != 0) {
            //用于非一块大小
//                if(position%FILE_BLOCK == 0){
//                    lastPositin = position - 1;
//                }
            if (cache[position] == 29) {
                byte[] bytes = new byte[position - lastPositin];
                System.arraycopy(cache, lastPositin + 1, bytes, 0, position - lastPositin);
                lastPositin = position++;
                return MessageUtil.read(bytes);
            }
            position++;
        }

        if (read()) {
            position = 0;
            lastPositin = -1;
            while (position < cache.length) {
                if (cache[position] == 29) {
                    byte[] bytes = new byte[position - lastPositin];
                    System.arraycopy(cache, lastPositin + 1, bytes, 0, position - lastPositin);
                    lastPositin = position++;
                    return MessageUtil.read(bytes);
                }
                position++;
            }
        }

        return null;


//      mmp每次读一个
//////
//        try {
//
//            if (mappedByteBuffer == null) {
//
//                fileChannel = new RandomAccessFile(PATH + it.next(), "r").getChannel();
//                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
//
//                mappedByteBuffer.mark();
//
//            }
//
//            while (true) {
//                while (mappedByteBuffer.hasRemaining()) {
//                    if (mappedByteBuffer.get() == 29) {
//                        position = mappedByteBuffer.position();
//                        mappedByteBuffer.reset();
//                        byte[] bytes = new byte[position - mark];
//                        mappedByteBuffer.get(bytes);
//                        mappedByteBuffer.mark();
//                        mark = mappedByteBuffer.position();
//
////                        long readToMessageStart = System.currentTimeMillis();
//                        //long readToMessageEnd = System.currentTimeMillis();
////                        readToMessageTime += readToMessageEnd - readToMessageStart;
//
//                        return MessageUtil.read(bytes);
//                    }
//                }
//                fileChannel.close();
//
//                if (it.hasNext()) {
//                    fileChannel = new RandomAccessFile(PATH + it.next(), "r").getChannel();
//                    mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
//
//                    mappedByteBuffer.mark();
//                    mark = 0;
//                    position = 0;
//                } else {
//                    break;
//                }
//
//            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
////        System.out.println("readToMessageTime: " + readToMessageTime);
//        return null;
    }

    private boolean read() {

////        RAF分段读到cache
//        try {
//            if (cached != 0 && cached < randomAccessFile.length()){
////                if (cached < randomAccessFile.length() - FILE_BLOCK) {
//                    cache = new byte[FILE_BLOCK];
//                    randomAccessFile.read(cache);
//                    cached += FILE_BLOCK;
////                    return true;
////                }
////
////                cache = new byte[(int) (randomAccessFile.length() - cached)];
////                randomAccessFile.read(cache);
////                cached = (int) randomAccessFile.length();
////                return true;
//            }
//
//            if (it.hasNext()) {
//                cached = 0;
//                randomAccessFile = new RandomAccessFile(PATH + it.next(), "r");
//                cache = new byte[FILE_BLOCK];
//                randomAccessFile.read(cache);
//                cached += FILE_BLOCK;
//
//                return true;
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        return false;
//

        //RAF分段读到cache(带解压缩)
//        try {
//            if (cached != 0 && cached < randomAccessFile.length()) {
////                if (cached < randomAccessFile.length() - FILE_BLOCK) {
//                cache = new byte[DEFLATE_BLOCK];
//                randomAccessFile.read(cache);
//                byte[] out = new byte[FILE_BLOCK];
//                uncompressor.setInput(cache);
//                uncompressor.inflate(out);
//                uncompressor.reset();
//                cache = out;
//                cached += DEFLATE_BLOCK;
//                return true;
////                }
//
////                cache = new byte[(int) (randomAccessFile.length() - cached)];
////                randomAccessFile.read(cache);
////                cached = (int) randomAccessFile.length();
////                return true;
//            }
//
//            if (it.hasNext()) {
//                cached = 0;
//                randomAccessFile = new RandomAccessFile(PATH + it.next(), "r");
//                cache = new byte[DEFLATE_BLOCK];
//                randomAccessFile.read(cache);
//                byte[] out = new byte[FILE_BLOCK];
//                uncompressor.setInput(cache);
//                uncompressor.inflate(out);
//                uncompressor.reset();
//                cache = out;
//                cached += DEFLATE_BLOCK;
//
//                return true;
//            }
//        } catch (IOException | DataFormatException e) {
//            e.printStackTrace();
//        }
//
//        return false;



        //RAF压缩分段读到cache(带解压缩)
        try {
            if (index != 0 && index < positionList.size()) {
                int position = positionList.get(index++);
                cache = new byte[position];
                randomAccessFile.read(cache);
                byte[] out = new byte[FILE_BLOCK];
                uncompressor.setInput(cache);
                uncompressor.inflate(out);
                uncompressor.reset();
                cache = out;
                cached += DEFLATE_BLOCK;
                return true;
            }

            if (it.hasNext()) {
                cached = 0;
                index = 0;
                String bucket = it.next();
                randomAccessFile = new RandomAccessFile(PATH + bucket, "r");
                positionList = deflatePosition.get(bucket);
                int position = positionList.get(index++);
                cache = new byte[position];
                randomAccessFile.read(cache);
                byte[] out = new byte[FILE_BLOCK];
                uncompressor.setInput(cache);
                uncompressor.inflate(out);
                uncompressor.reset();
                cache = out;
                cached += DEFLATE_BLOCK;

                return true;
            }
        } catch (IOException | DataFormatException e) {
            e.printStackTrace();
        }

        return false;

        //RAF读超过一个块大小(未完)

//        try {
//            if (cached != 0 && cached < randomAccessFile.length()){
//                if (cached < randomAccessFile.length() - BYTES_CACHE) {
//                    cache = new byte[BYTES_CACHE];
//                    randomAccessFile.read(cache);
//                    cached += BYTES_CACHE;
//                    return true;
//                }
//
//                if (it.hasNext()) {
//                    byte[] temp = new byte[(int) (randomAccessFile.length() - cached)];
//                    randomAccessFile.read(temp);
//
//                    cached = 0;
//                    randomAccessFile = new RandomAccessFile(PATH + it.next(), "r");
//                    System.arraycopy(temp,0,cache,cached,temp.length);
//
//                    byte[] temp2 = new byte[]
//
//
//                    return true;
//                    cache = new byte[BYTES_CACHE];
//                    randomAccessFile.read(cache);
//                    cached += BYTES_CACHE;
//
//                    return true;
//                }
//
//                byte[] temp = new byte[(int) (randomAccessFile.length() - cached)];
//                randomAccessFile.read(temp);
//                cached = (int) randomAccessFile.length();
//                return true;
//            }
//
//            if (it.hasNext()) {
//                cached = 0;
//                randomAccessFile = new RandomAccessFile(PATH + it.next(), "r");
//                cache = new byte[BYTES_CACHE];
//                randomAccessFile.read(cache);
//                cached += BYTES_CACHE;
//
//                return true;
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        return false;


        //RAF一次读一个bucket(加解压)
//        try{
//            if(it.hasNext()){
//                RandomAccessFile randomAccessFile = new RandomAccessFile(PATH + it.next(),"r");
//                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//                InflaterOutputStream inflaterOutputStream = new InflaterOutputStream(byteArrayOutputStream);
//
//                cache = new byte[(int) randomAccessFile.length()];
//                randomAccessFile.read(cache);
//
//                inflaterOutputStream.write(cache);
//                inflaterOutputStream.close();
//                cache = byteArrayOutputStream.toByteArray();
//                randomAccessFile.close();
//                return true;
//            }
//
//        }catch (IOException e){
//            e.printStackTrace();
//        }
//
//        return false;


//        mmp分段读
//        try {
//            if (mappedByteBuffer == null) {
//                bucket = it.next();
//                fileChannel = new RandomAccessFile(PATH + bucket, "r").getChannel();
//                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, FILE_BLOCK);
//                mappedByteBuffer.load();
//                mappedByteBuffer.mark();
//                return true;
//            }
//
////            positionMap.put(bucket,positionMap.getOrDefault(bucket,0L) + mappedByteBuffer.position());
////            long nowPosition = positionMap.get(bucket);
////
//            mPosition += FILE_BLOCK;
//            //用于大于一个块大小
////            if(fileChannel.size() != mPosition) {
////                if (fileChannel.size() - mPosition > FILE_BLOCK) {
////                    mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, mPosition, FILE_BLOCK);
////                }else {
////                    mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, mPosition, fileChannel.size() - mPosition);
////
////                }
////                mappedByteBuffer.load();
////                mappedByteBuffer.mark();
////                return true;
////            }
//
//            //等于一个块大小
//            if (fileChannel.size() != mPosition) {
//                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, mPosition, FILE_BLOCK);
//                mappedByteBuffer.load();
//                mappedByteBuffer.mark();
//                return true;
//            }
//
//
//            if (it.hasNext()) {
//                bucket = it.next();
//                fileChannel = new RandomAccessFile(PATH + bucket, "r").getChannel();
//                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, FILE_BLOCK);
//                mPosition = 0;
//                mappedByteBuffer.load();
//                mappedByteBuffer.mark();
//                return true;
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return false;


        //mmp一次读一个bucket

//        try {
//            if (it.hasNext()) {
//                bucket = it.next();
//                fileChannel = new RandomAccessFile(PATH + bucket, "r").getChannel();
//                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
//                mappedByteBuffer = mappedByteBuffer.load();
//                mappedByteBuffer.mark();
//                return true;
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return false;

    }

    @Override
    public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    //只能绑定一个queue和多个topics
    @Override
    public void attachQueue(String queueName, Collection<String> topics) {
//        if (queue != null && !queue.equals(queueName)) {
//            throw new ClientOMSException("You have already attached to a queue " + queue);
//        }
        queue = queueName;
        bucketList.add(queueName);
        bucketList.addAll(topics);
        it = bucketList.iterator();

        try {
            FileInputStream fileInputStream = new FileInputStream(PATH + "index");
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            deflatePosition = (Map<String, ArrayList<Integer>>) objectInputStream.readObject();
            objectInputStream.close();
            fileInputStream.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }


//        读到缓存
//        try {
//            fileChannel = new RandomAccessFile(PATH + it.next(), "r").getChannel();
//            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
//            mappedByteBuffer.mark();
//        }catch (IOException e){
//            e.printStackTrace();
//        }

        read();


        //对应线程分离RAF
//        executorService.execute(() -> {
//            try {
//                while(read()) {
//                    cacheQueue.put(cache);
//                }
//                done = true;
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        });
//
//        try {
//            thisCache = cacheQueue.take();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }


        //对应MMP线程分离
//        executorService.execute(new Runnable() {
//            @Override
//            public void run() {
//                read();
//                while (true) {
//                    while (mappedByteBuffer.hasRemaining()) {
//                        //用于非整数倍块大小
////                if(mappedByteBuffer.position()%FILE_BLOCK == 0){
////                    mappedByteBuffer.mark();
////                    mark = mappedByteBuffer.position();
////                }
//                        if (mappedByteBuffer.get() == 29) {
//                            position = mappedByteBuffer.position();
//                            mappedByteBuffer.reset();
//                            byte[] bytes = new byte[position - mark];
//                            mappedByteBuffer.get(bytes);
//                            mappedByteBuffer.mark();
//                            mark = position;
//
//                            try {
//                                messQueue.put(bytes);
//                            } catch (InterruptedException e) {
//                                e.printStackTrace();
//                            }
//                        }
//                    }
//
//                    if (!read()) {
//                        break;
//                    }
//                    mark = 0;
//                }
//
//                done = true;
//
//            }
//        });

    }

}
