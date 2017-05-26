package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class DefaultPullConsumer implements PullConsumer {
    public static final int CACHE_SIZE = 1024 * 1024 * 10;
    public static final int MESS_CACHE = 50000;
    //    private final MessageStore messageStore = MessageStore.getInstance();
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

    private LinkedList<Message> messList = new LinkedList<>();
    private LinkedList<byte[]> bytesList = new LinkedList<>();
    private String bucket;

    private RandomAccessFile randomAccessFile;

    private int n;

    private byte[] cache;

    private int cached;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        PATH = properties.getString("STORE_PATH") + "/";

    }


    @Override
    public KeyValue properties() {
        return properties;
    }

    long readToMessageTime = 0;



    @Override
    public Message poll() {

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
            while(position < cache.length){
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




//      正式版，每次读一个
////
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
//                        Message message = MessageUtil.read(bytes);
////                        long readToMessageEnd = System.currentTimeMillis();
////                        readToMessageTime += readToMessageEnd - readToMessageStart;
//
//                        return message;
//                    }
//                }
//                fileChannel.close();
//
//                if(it.hasNext()) {
//                    fileChannel = new RandomAccessFile(PATH + it.next(), "r").getChannel();
//                    mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
//
//                    mappedByteBuffer.mark();
//                    mark = 0;
//                    position = 0;
//                }else {
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

    public boolean read() {

        //读到cache
        try {
            if(cached != 0 && cached < randomAccessFile.length()){
                if(cached < randomAccessFile.length() - CACHE_SIZE){
                    cache = new byte[CACHE_SIZE + 1024];
                    randomAccessFile.read(cache,0,CACHE_SIZE);
                    int p = CACHE_SIZE;
                    int b;
                    while((b = randomAccessFile.read()) != 30){
                        cache[p++] = (byte) b;
                    }
                    cache[p++] = (byte) b;
                    cached += p;
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
                cached = 0;
                randomAccessFile = new RandomAccessFile(PATH + it.next(), "r");
                cache = new byte[CACHE_SIZE + 1024];
                randomAccessFile.read(cache,0,CACHE_SIZE);
                int p = CACHE_SIZE;
                int b;
                while((b = randomAccessFile.read()) != 30){
                    cache[p++] = (byte) b;
                }
                cache[p++] = (byte) b;
                cached += p;
                return true;
            }
        }catch (IOException e){
            e.printStackTrace();
        }

        return false;



        //读到messList
//        try {
//            if (mappedByteBuffer == null) {
//                bucket = it.next();
//
//                fileChannel = new RandomAccessFile(PATH + bucket, "r").getChannel();
//                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
//                mappedByteBuffer.mark();
//            }
//
//            while (true) {
//                while (mappedByteBuffer.hasRemaining()) {
//                    if (bytesList.size() > MESS_CACHE) {
//                        return true;
//                    }
//                    if (mappedByteBuffer.get() == 30) {
//                        position = mappedByteBuffer.position();
//                        mappedByteBuffer.reset();
//                        byte[] bytes = new byte[position - mark];
//                        mappedByteBuffer.get(bytes);
//                        mappedByteBuffer.mark();
//                        mark = mappedByteBuffer.position();
//                        bytesList.add(bytes);
//                    }
//                }
//                fileChannel.close();
//
//                if (it.hasNext()) {
//                    bucket = it.next();
//                    fileChannel = new RandomAccessFile(PATH + bucket, "r").getChannel();
//                    mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
//                    mappedByteBuffer.mark();
//                    mark = 0;
//                    position = 0;
//                } else {
//                    break;
//                }
//            }
//
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
