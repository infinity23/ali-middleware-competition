package io.openmessaging.demo;

import io.openmessaging.KeyValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DefaultKeyValue implements KeyValue {

    private final Map<String, Object> kvs = new HashMap<>(2);

//    private static final int SIZE = 1024 * 10;
//
//    private byte[] bytes = new byte[SIZE];
//
//    private int position = 0;
//
//    private static final Charset charset = Charset.forName("US-ASCII");


    @Override
    public KeyValue put(String key, int value) {
        kvs.put(key, value);
//        byte[] keyBytes = key.getBytes(charset);
//        System.arraycopy(keyBytes,0,bytes,position,keyBytes.length);
//        position += keyBytes.length;
//
//        byte[] valueBytes = MessageUtil.intToByte(value);
//        System.arraycopy(keyBytes,0,bytes,position,valueBytes.length);
//
//        position += 4;
//
//        //ASCII 单元分隔符31
//        bytes[position++] = 31;
        return this;
    }

    @Override
    public KeyValue put(String key, long value) {
        kvs.put(key, value);
//        byte[] keyBytes = key.getBytes(charset);
//        System.arraycopy(keyBytes,0,bytes,position,keyBytes.length);
//        position += keyBytes.length;
//
//        byte[] valueBytes = MessageUtil.longToByte(value);
//        System.arraycopy(keyBytes,0,bytes,position,valueBytes.length);
//        position += 8;
//
//        //ASCII 单元分隔符31
//        bytes[position++] = 31;
        return this;
    }

    @Override
    public KeyValue put(String key, double value) {
        kvs.put(key, value);
//        byte[] keyBytes = key.getBytes(charset);
//        System.arraycopy(keyBytes,0,bytes,position,keyBytes.length);
//        position += keyBytes.length;
//
//        byte[] valueBytes = MessageUtil.doubleToByte(value);
//        System.arraycopy(keyBytes,0,bytes,position,valueBytes.length);
//        position += 8;
//
//        //ASCII 单元分隔符31
//        bytes[position++] = 31;
        return this;
    }

    @Override
    public KeyValue put(String key, String value) {
        kvs.put(key, value);
//        byte[] keyBytes = key.getBytes(charset);
//        System.arraycopy(keyBytes,0,bytes,position,keyBytes.length);
//        position += keyBytes.length;
//
//        byte[] valueBytes = value.getBytes(charset);
//        System.arraycopy(keyBytes,0,bytes,position,valueBytes.length);
//        position += valueBytes.length;
//
//        //ASCII 单元分隔符31
//        bytes[position++] = 31;
        return this;
    }

    @Override
    public int getInt(String key) {

        return (Integer)kvs.getOrDefault(key, 0);
    }

    @Override
    public long getLong(String key) {
        return (Long)kvs.getOrDefault(key, 0L);
    }

    @Override
    public double getDouble(String key) {
        return (Double)kvs.getOrDefault(key, 0.0d);
    }

    @Override
    public String getString(String key) {
        return (String)kvs.getOrDefault(key, null);
    }

    @Override
    public Set<String> keySet() {
        return kvs.keySet();
    }

    @Override
    public boolean containsKey(String key) {
        return kvs.containsKey(key);
    }

    @Override
    public String toString() {
        return kvs.toString();
    }
}
