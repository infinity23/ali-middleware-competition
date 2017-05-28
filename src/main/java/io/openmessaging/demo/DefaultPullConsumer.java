package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.CyclicBarrier;

public class DefaultPullConsumer implements PullConsumer {
    public static final int FILEBLOCK = 1024 * 1024 * 40;
    public static final int MESS_CACHE = 50000;
    private KeyValue properties;
    private String queue;
    //    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();
    //    private HashMap<String, Integer> messIdx = new HashMap<>();
//
//    private int lastIndex = 0;
//    private List<Message> resultList;
//    private String bucket;
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

    private ArrayList<Message> messList;
    private ArrayList<byte[]> bytesList = new ArrayList<>();
    private String bucket;

    private RandomAccessFile randomAccessFile;

    private int n;

    private byte[] cache;

    private int cached;

    private CyclicBarrier cyclicBarrier;
    private boolean done;
    private Map<String, Long> positionMap = new HashMap<>(100);
    private long mPosition;

    public void setCyclicBarrier(CyclicBarrier cyclicBarrier) {
        this.cyclicBarrier = cyclicBarrier;
    }

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        PATH = properties.getString("STORE_PATH") + "/";
        messageStore = MessageStore.getInstance(PATH);
    }


    @Override
    public KeyValue properties() {
        return properties;
    }

    long readToMessageTime = 0;

    private int index;

    @Override
    public Message poll() {

        //对应分段mmp
//        while (true) {
//            while (mappedByteBuffer.hasRemaining()) {
//                //用于非整数倍块大小
////                if(mappedByteBuffer.position()%FILEBLOCK == 0){
////                    mappedByteBuffer.mark();
////                    mark = mappedByteBuffer.position();
////                }
//                if (mappedByteBuffer.get() == 30) {
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
//                    if (cache[position] == 30) {
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


        //缓存版，先读到一个byte[]

        while(position < cache.length){
            if(cache[position] == 30){
                byte[] bytes = new byte[position - lastPositin];
                System.arraycopy(cache,lastPositin + 1,bytes,0,position - lastPositin);
                lastPositin = position++;
                return MessageUtil.read(bytes);
            }
            position ++;
        }

        if(read()){
            position = 0;
            lastPositin = -1;
            while(position < cache.length && cache[position] != 0){
                if(cache[position] == 30){
                    byte[] bytes = new byte[position - lastPositin];
                    System.arraycopy(cache,lastPositin + 1,bytes,0,position - lastPositin);
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
//                    if (mappedByteBuffer.get() == 30) {
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

//        int head;
////        读到cache
        try {
            if (cached != 0 && cached < randomAccessFile.length()){
                if (cached < randomAccessFile.length() - FILEBLOCK) {
                    cache = new byte[FILEBLOCK];
                    randomAccessFile.read(cache);
//                    int p = FILEBLOCK;
//                    int b;
//                    while ((b = randomAccessFile.read()) != 30) {
//                        cache[p++] = (byte) b;
//                    }
//                    cache[p++] = (byte) b;
//                    cached += p;
                    cached += FILEBLOCK;
                    return true;
                }

                cache = new byte[(int) (randomAccessFile.length() - cached)];
                randomAccessFile.read(cache);
                cached = (int) randomAccessFile.length();
                return true;
            }

            if (it.hasNext()) {
                //mappedByteBuffer读
//                fileChannel = new RandomAccessFile(PATH + it.next(), "r").getChannel();
//                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
//                cache = new byte[(int) fileChannel.size()];
//                mappedByteBuffer.get(cache);

                //RandomAccessFile读
//                randomAccessFile = new RandomAccessFile(PATH + it.next(), "r");
//                cache = new byte[(int) randomAccessFile.length()];
//                randomAccessFile.read(cache);
//                randomAccessFile.close();
//                return true;

                //先读到一定量的cache
//                cached = 0;
                randomAccessFile = new RandomAccessFile(PATH + it.next(), "r");
                cache = new byte[FILEBLOCK];
                randomAccessFile.read(cache);
                cached += FILEBLOCK;

//                int p = FILEBLOCK;
//                int b;
//                while ((b = randomAccessFile.read()) != 30) {
//                    cache[p++] = (byte) b;
//                }
//                cache[p++] = (byte) b;
//                cached += p;
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;


//        mmp分段读
//        try {
//            if (mappedByteBuffer == null) {
//                bucket = it.next();
//                fileChannel = new RandomAccessFile(PATH + bucket, "r").getChannel();
//                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, FILEBLOCK);
//                mappedByteBuffer.load();
//                mappedByteBuffer.mark();
//                return true;
//            }
//
////            positionMap.put(bucket,positionMap.getOrDefault(bucket,0L) + mappedByteBuffer.position());
////            long nowPosition = positionMap.get(bucket);
////
//            mPosition += FILEBLOCK;
//
//            //用于大于一个块大小
////            if(fileChannel.size() != mPosition) {
////                if (fileChannel.size() - mPosition > FILEBLOCK) {
////                    mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, mPosition, FILEBLOCK);
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
//            if(fileChannel.size() != mPosition){
//                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, mPosition, FILEBLOCK);
//                mappedByteBuffer.load();
//                mappedByteBuffer.mark();
//                return true;
//            }
//
//
//            if (it.hasNext()) {
//                bucket = it.next();
//                fileChannel = new RandomAccessFile(PATH + bucket, "r").getChannel();
//                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, FILEBLOCK);
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
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have already attached to a queue " + queue);
        }
        queue = queueName;
        bucketList.add(queueName);
        bucketList.addAll(topics);
        it = bucketList.iterator();


//        读到缓存
//        try {
//            fileChannel = new RandomAccessFile(PATH + it.next(), "r").getChannel();
//            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
//            mappedByteBuffer.mark();
//        }catch (IOException e){
//            e.printStackTrace();
//        }

        read();


    }
}
