package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

public class MessageUtil {

    private static final Charset CHARSET = Charset.forName("US-ASCII");
    private static final char RS = 31;
    private static final char US = 30;
    private static final char GS = 29;
//    private static final char COM = ',';
    private static final char EQU = '=';

    private static final Deflater compressor = new Deflater(1);
    private static final Inflater uncompressor = new Inflater();

    public static byte[] write(Message message) {
        BytesMessage bytesMessage = (BytesMessage) message;
        StringBuilder sb = new StringBuilder();
        DefaultKeyValue header = (DefaultKeyValue) bytesMessage.headers();
        DefaultKeyValue properties = (DefaultKeyValue) bytesMessage.properties();

        if(header != null) {
            for (Map.Entry<String, Object> entry : header.getMap().entrySet()) {
                sb.append(entry.getKey());
                sb.append(EQU);
                sb.append(entry.getValue());
                sb.append(RS);
            }
            sb.delete(sb.length() - 1, sb.length());
        }
        sb.append(US);

        if(properties != null) {
            for (Map.Entry<String, Object> entry : properties.getMap().entrySet()) {
                sb.append(entry.getKey());
                sb.append(EQU);
                sb.append(entry.getValue());
                sb.append(RS);
            }
            sb.delete(sb.length() - 1, sb.length());
        }
        sb.append(US);

        String s = sb.toString();
        byte[] pre = s.getBytes(CHARSET);
        byte[] body = bytesMessage.getBody();
        byte[] bytes = new byte[pre.length + body.length + 1];

        System.arraycopy(pre,0,bytes,0,pre.length);
        System.arraycopy(body,0,bytes,pre.length,body.length);
        bytes[bytes.length-1] = GS;

        return bytes;





        //未优化版本
//        byte[] header = bytesMessage.headers() == null ? new byte[0] : bytesMessage.headers().toString().getBytes(CHARSET);
//        byte[] properties = bytesMessage.properties() == null ? new byte[0] : bytesMessage.properties().toString().getBytes(CHARSET);
//
//        byte[] body = bytesMessage.getBody();
//        byte[] bytes = new byte[header.length + properties.length + body.length + 3];
//
//        System.arraycopy(header, 0, bytes, 0, header.length);
//        //ASCII 单元分隔符31
//        bytes[header.length] = 31;
//
//        System.arraycopy(properties, 0, bytes, header.length + 1, properties.length);
//
//        //ASCII 单元分隔符31
//        bytes[header.length + properties.length + 1] = 31;
//
//        System.arraycopy(body, 0, bytes, header.length + properties.length + 2, body.length);
//
//        //ASCII 记录分隔符30
//        bytes[header.length + properties.length + body.length + 2] = 30;
//
//
//        return bytes;

    }

    public static Message read(byte[] bytes) {

        //未优化版本
        int hp = -1;
        int pp = -1;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == US) {
                if (hp == -1) {
                    hp = i;
                } else {
                    pp = i;
                    break;
                }
            }
        }

        byte[] body = new byte[bytes.length - pp - 2];

        System.arraycopy(bytes, pp + 1, body, 0, bytes.length - pp - 2);

        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);

        if (hp > 1) {
            String header = new String(bytes, 0, hp, CHARSET);
            defaultBytesMessage.setHeaders(readMap(header));
        }

        if (pp - hp > 1) {
            String properties = new String(bytes, hp + 1, pp - hp - 1, CHARSET);
            defaultBytesMessage.setProperties(readMap(properties));
        }

        return defaultBytesMessage;

    }



    private static DefaultKeyValue readMap(String s) {
        DefaultKeyValue keyValue = new DefaultKeyValue();

        int indexOfRS;
        int lastIndexOfRS = s.indexOf(RS);
        int indexOfEqu = s.indexOf(EQU);

        if (lastIndexOfRS == -1) {
            keyValue.put(s.substring(0, indexOfEqu), s.substring(indexOfEqu + 1, s.length()));
            return keyValue;
        }

        keyValue.put(s.substring(0, indexOfEqu), s.substring(indexOfEqu + 1, lastIndexOfRS));

        while ((indexOfRS = s.indexOf(RS, lastIndexOfRS + 1)) != -1) {
            indexOfEqu = s.indexOf(EQU, lastIndexOfRS + 1);
            keyValue.put(s.substring(lastIndexOfRS + 1, indexOfEqu), s.substring(indexOfEqu + 1, indexOfRS));
            lastIndexOfRS = indexOfRS;
        }

        indexOfEqu = s.indexOf(EQU, lastIndexOfRS);
        keyValue.put(s.substring(lastIndexOfRS + 1, indexOfEqu), s.substring(indexOfEqu + 1, s.length()));

        return keyValue;
    }



    public static void compress(byte[] input, ByteArrayOutputStream byteArrayOutputStream){
//        DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream,compressor);
//        try {
//            deflaterOutputStream.write(input);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }



    }

    public static void uncompress(byte[] input, ByteArrayOutputStream byteArrayOutputStream){
        InflaterOutputStream inflaterOutputStream = new InflaterOutputStream(byteArrayOutputStream, uncompressor);
        try {
            inflaterOutputStream.write(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }







    //HashMap排序
    public static <K, V extends Comparable<? super V>> Map<K, V>
    sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list =
                new LinkedList<>(map.entrySet());
        list.sort(Comparator.comparing(o -> (o.getValue())));

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }


//  //整数到字节数组的转换
//    public static byte[] intToByte(int number) {
//        int temp = number;
//        byte[] b=new byte[4];
//        for (int i=b.length-1;i>-1;i--){
//            b[i] = new Integer(temp&0xff).byteValue();      //将最高位保存在最低位
//            temp = temp >> 8;       //向右移8位
//        }
//        return b;
//    }
//
//    //字节数组到整数的转换
//    public static int byteToInt(byte[] b) {
//        int s = 0;
//        for (int i = 0; i < 3; i++) {
//            if (b[i] >= 0)
//                s = s + b[i];
//            else
//
//                s = s + 256 + b[i];
//            s = s << 8;
//        }
//        if (b[3] >= 0)       //最后一个之所以不乘，是因为可能会溢出
//            s = s + b[3];
//        else
//            s = s + 256 + b[3];
//        return s;
//    }
//  //long到字节数组的转换
//    public static byte[] longToByte(long number) {
//        long temp = number;
//        byte[] b=new byte[8];
//        for (int i=b.length-1;i>-1;i--){
//            b[i] = new Long(temp&0xff).byteValue();      //将最高位保存在最低位
//            temp = temp >> 8;       //向右移8位
//        }
//        return b;
//    }
//
//    //字节数组到long的转换
//    public static long byteToLong(byte[] b) {
//        long s = 0;
//        for (int i = 0; i < 7; i++) {
//            if (b[i] >= 0)
//                s = s + b[i];
//            else
//                s = s + 256 + b[i];
//            s = s << 8;
//        }
//        if (b[7] >= 0)       //最后一个之所以不乘，是因为可能会溢出
//            s = s + b[7];
//        else
//            s = s + 256 + b[7];
//        return s;
//    }
//
//    //字符到字节转换
//    public static byte[] charToByte(char ch){
//        int temp=(int)ch;
//        byte[] b=new byte[2];
//        for (int i=b.length-1;i>-1;i--){
//            b[i] = new Integer(temp&0xff).byteValue();      //将最高位保存在最低位
//            temp = temp >> 8;       //向右移8位
//        }
//        return b;
//    }
//
//    //字节到字符转换
//
//    public static char byteToChar(byte[] b){
//        int s=0;
//        if(b[0]>0)
//            s+=b[0];
//        else
//            s+=256+b[0];
//        s*=256;
//        if(b[1]>0)
//            s+=b[1];
//        else
//            s+=256+b[1];
//        return (char)s;
//    }
//
//    //浮点到字节转换
//    public static byte[] doubleToByte(double d){
//        byte[] b=new byte[8];
//        long l=Double.doubleToLongBits(d);
//        for(int i=0;i<b.length;i++){
//            b[i]=new Long(l).byteValue();
//            l=l>>8;
//
//        }
//        return b;
//    }
//
//    //字节到浮点转换
//    public static double byteToDouble(byte[] b){
//        long l;
//
//        l=b[0];
//        l&=0xff;
//        l|=((long)b[1]<<8);
//        l&=0xffff;
//        l|=((long)b[2]<<16);
//        l&=0xffffff;
//        l|=((long)b[3]<<24);
//        l&= 0xffffffffL;
//        l|=((long)b[4]<<32);
//        l&= 0xffffffffffL;
//
//        l|=((long)b[5]<<40);
//        l&= 0xffffffffffffL;
//        l|=((long)b[6]<<48);
//
//        l|=((long)b[7]<<56);
//        return Double.longBitsToDouble(l);
//    }

}


