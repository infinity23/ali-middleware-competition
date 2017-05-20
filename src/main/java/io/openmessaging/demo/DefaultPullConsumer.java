package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;

public class DefaultPullConsumer implements PullConsumer {
//    private final MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;
    private String queue;
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();
    private HashMap<String, Integer> messIdx = new HashMap<>();

    private int lastIndex = 0;
    private List<Message> resultList;
    private String bucket;
    private Iterator<String> it;
    private int finishedNum;
    private boolean first;
    private List<String> topicList;
    private ObjectInputStream objectInputStream;
    private FileInputStream fileInputStream;

    private String PATH;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        PATH = properties.getString("STORE_PATH")+"/";
    }


    @Override
    public KeyValue properties() {
        return properties;
    }


    @Override
    public synchronized Message poll() {

//        while (finishedNum != bucketList.size()) {
//            try {
//                if (resultList != null) {
//                    if (lastIndex == resultList.size()) {
//                        resultList = messageStore.pullMessage(bucket, true);
//                        lastIndex = 0;
//                        finishedNum++;
//                    } else {
//                        return resultList.get(lastIndex++);
//                    }
//                } else {
//                    if (it.hasNext()) {
//                        this.bucket = it.next();
//                        resultList = messageStore.pullMessage(bucket, false);
//                    } else {
//                        synchronized (messageStore){
//                            while(!it.hasNext()){
//                                messageStore.wait();
//                            }
//                        }
//                    }
//                }
//            }catch (Exception e){
//                e.printStackTrace();
//            }
//        }

//        if (first) {
//            messageStore.setBuckets(topicList);
//        }

        try {
            if (fileInputStream == null) {
                fileInputStream = new FileInputStream(PATH + it.next());
                objectInputStream = new ObjectInputStream(fileInputStream);
            }
            if (fileInputStream.available() > 0) {
                return (Message) objectInputStream.readObject();
            }
            objectInputStream.close();
            fileInputStream.close();
            if (it.hasNext()) {
                fileInputStream = new FileInputStream(PATH + it.next());
                objectInputStream = new ObjectInputStream(fileInputStream);
                if(fileInputStream.available() == 0)
                    return null;
                return (Message) objectInputStream.readObject();
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;


//        if (buckets.size() == 0 || queue == null) {
//            return null;
//        }
//        //use Round Robin
//        int checkNum = 0;
//        while (++checkNum <= bucketList.size()) {
//            String bucket = bucketList.get((++lastIndex) % (bucketList.size()));
//            Message message = null;
//            int index = messIdx.getOrDefault(bucket,0);
//            try {
//                message = messageStore.pullMessage(bucket,index);
//                messIdx.put(bucket,index+1);
//            } catch (IOException e) {
//                throw new ClientOMSException(String.format("Bucket:%s poll occurs an io exception", bucket));
//            } catch (ClassNotFoundException e) {
//                throw new ClientOMSException(String.format("Bucket:%s poll occurs a classNotFoundException exception", bucket));
//            }
//            if (message != null) {
//                return message;
//            }
//        }
//        return null;
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
    public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have already attached to a queue " + queue);
        }
        queue = queueName;
        buckets.add(queueName);
        buckets.addAll(topics);
        bucketList.clear();
        bucketList.addAll(buckets);
        it = bucketList.iterator();
    }


}
