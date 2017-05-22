package io.openmessaging.demo;

import io.openmessaging.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class DefaultProducer implements Producer {
//    private static final long SLEEP_TIME = 10;
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private MessageStore messageStore;
//    private Map<String, LinkedList<Message>> resultMap = new HashMap<>(100);
//    private static Map<String, Long> position = new ConcurrentHashMap<>(100);
//    private Map<String, RandomAccessFile> randomAccessFileMap = new HashMap<>(100);
//    private Map<String, ObjectOutputStream> objectOutputStreamMap = new HashMap<>(100);
//    private static String PATH;
//    private static ExecutorService executorService = Executors.newCachedThreadPool();

    private KeyValue properties;
    private ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(100);
    private ObjectOutputStream objectOutputStream;

    private int messNum;

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
//        PATH = properties.getString("STORE_PATH") + "/";

//        executorService.execute(() -> {
//            try {
//                TimeUnit.MILLISECONDS.sleep(SLEEP_TIME);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            flush();
//        });
        try {
            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        messageStore = MessageStore.getInstance(properties.getString("STORE_PATH"));
    }


    @Override
    public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        System.out.println("调用createBytesMessageToTopic");
        DefaultBytesMessage bytesMessage = (DefaultBytesMessage) messageFactory.createBytesMessageToTopic(topic, body);
        try {
            objectOutputStream.writeObject(bytesMessage);
            objectOutputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        bytesMessage.setBytes(byteArrayOutputStream.toByteArray());
        byteArrayOutputStream.reset();

        bytesMessage.setBody(null);
        return bytesMessage;
    }

    @Override
    public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {

        DefaultBytesMessage bytesMessage = (DefaultBytesMessage) messageFactory.createBytesMessageToQueue(queue, body);
        try {
            objectOutputStream.writeObject(bytesMessage);
            objectOutputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        bytesMessage.setBytes(byteArrayOutputStream.toByteArray());
        byteArrayOutputStream.reset();

        bytesMessage.setBody(null);
        return bytesMessage;
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
        System.out.println("开始发送");
        if (message == null) throw new ClientOMSException("Message should not be null");
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        if ((topic == null && queue == null) || (topic != null && queue != null)) {
            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", queue, topic));
        }
//        String bucket = topic == null ? queue : topic;
//
//        if (!resultMap.containsKey(bucket)) {
//            resultMap.put(bucket, new LinkedList<>());
//        }
//
//        resultMap.get(bucket).add(message);
//        messNum++;
//
//        if(messNum > 100000){
//            flush();
//        }

            messageStore.putMessage(topic != null ? topic : queue, message);
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
        messageStore.flush();
//        System.out.println("刷新到硬盘");
//        long start = System.currentTimeMillis();
//            try {
//                for (String key : resultMap.keySet()) {
//                    if (!randomAccessFileMap.containsKey(key)) {
//                        randomAccessFileMap.put(key, new RandomAccessFile(PATH + key, "rw"));
//                    }
//                    RandomAccessFile randomAccessFile = randomAccessFileMap.get(key);
//
//                    if (!objectOutputStreamMap.containsKey(key)) {
//                        objectOutputStreamMap.put(key, new ObjectOutputStream(byteArrayOutputStream));
//                    }
//                    ObjectOutputStream objectOutputStream = objectOutputStreamMap.get(key);
//                    randomAccessFile.seek(position.getOrDefault(key, 0L));
//
//
//                    while (!resultMap.get(key).isEmpty()) {
//                        Message message = resultMap.get(key).poll();
//                        messNum--;
//                        objectOutputStream.writeObject(message);
//                    }
//
//                    objectOutputStream.flush();
//
//                    position.put(key, randomAccessFile.length());
//                    randomAccessFile.write(byteArrayOutputStream.toByteArray());
//
//                    byteArrayOutputStream.reset();
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        long end = System.currentTimeMillis();
//        System.out.println("本次硬盘刷新时间：" + (end - start));
    }
}
