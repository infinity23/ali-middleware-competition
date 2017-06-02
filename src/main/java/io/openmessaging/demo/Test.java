package io.openmessaging.demo;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Test {
    @org.junit.Test
    public void mapTest(){
//        Map<String, Double> map = new HashMap<>();
//        map.put("hello", 6.32);
//        map.put("nihao", 7.14);
//        System.out.println(map);

//        try {
//            RandomAccessFile randomAccessFile = new RandomAccessFile("D:/test", "rw");
//            System.out.println(randomAccessFile.read());
//            System.out.println(randomAccessFile.read());
////            randomAccessFile.write(2);
//            randomAccessFile.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        char c = 31;
//        System.out.println(c);

        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(5);
        byte[] bytes = new byte[]{1,2,3,4,5};
        byteBuffer.put(bytes);
        ByteBuffer byteBuffer1 = ByteBuffer.allocate(5);
        byteBuffer1.put(byteBuffer);
        System.out.println(Arrays.toString(byteBuffer1.array()));
        System.out.println(Arrays.toString(bytes));
//
//        byte b = (byte) c;
//
//        System.out.println((char) b);
    }
}
