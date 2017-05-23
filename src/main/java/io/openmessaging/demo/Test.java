package io.openmessaging.demo;

import java.util.HashMap;
import java.util.Map;

public class Test {
    @org.junit.Test
    public void mapTest(){
        Map<String, Double> map = new HashMap<>();
        map.put("hello", 6.32);
        map.put("nihao", 7.14);
        System.out.println(map);
//        char c = 49;
//
//        byte b = (byte) c;
//
//        System.out.println((char) b);
    }
}
