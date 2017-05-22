package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class MessageUtil {



    public static byte[] write(Message message){
        BytesMessage bytesMessage = (BytesMessage) message;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(100);
        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(bytesMessage.headers());
            objectOutputStream.writeObject(bytesMessage.properties());
            objectOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] body = bytesMessage.getBody();
        int size = byteArrayOutputStream.size() + body.length;
        byte[] bytes = new byte[size + 4];



        System.arraycopy(byteArrayOutputStream.toByteArray(),0,bytes,0,byteArrayOutputStream.size());
        System.arraycopy(body,0,bytes,byteArrayOutputStream.size(),body.length);
        return bytes;
    }

    public static Message read(){

        return null;
    }




    /**
     * 注释：int到字节数组的转换！
     *
     * @param number
     * @return
     */
    public static byte[] intToByte(int number) {
        int temp = number;
        byte[] b = new byte[4];
        for (int i = 0; i < b.length; i++) {
            b[i] = new Integer(temp & 0xff).byteValue();
            //将最低位保存在最低位
                    temp = temp >> 8; // 向右移8位
        }
        return b;
    }

    /**
     * 注释：字节数组到int的转换！
     *
     * @param b
     * @return
     */
    public static int byteToInt(byte[] b) {
        int s = 0;
        int s0 = b[0] & 0xff;// 最低位
        int s1 = b[1] & 0xff;
        int s2 = b[2] & 0xff;
        int s3 = b[3] & 0xff;
        s3 <<= 24;
        s2 <<= 16;
        s1 <<= 8;
        s = s0 | s1 | s2 | s3;
        return s;
    }


}


