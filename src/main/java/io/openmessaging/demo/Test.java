package io.openmessaging.demo;

import java.io.IOException;
import java.io.RandomAccessFile;

public class Test {
    @org.junit.Test
    public void mapTest(){
//        Map<String, Double> map = new HashMap<>();
//        map.put("hello", 6.32);
//        map.put("nihao", 7.14);
//        System.out.println(map);

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile("D:/test", "rw");
            System.out.println(randomAccessFile.read());
            System.out.println(randomAccessFile.read());
//            randomAccessFile.write(2);
            randomAccessFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        char c = 49;
//
//        byte b = (byte) c;
//
//        System.out.println((char) b);
    }
}
