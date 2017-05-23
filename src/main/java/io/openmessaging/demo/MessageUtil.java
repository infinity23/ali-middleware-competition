package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;

public class MessageUtil {


    public static byte[] write(Message message){
        BytesMessage bytesMessage = (BytesMessage) message;

        try {
            byte[] header = bytesMessage.headers() == null ? new byte[0] : bytesMessage.headers().toString().getBytes("US-ASCII");
            byte[] properties = bytesMessage.properties() == null ? new byte[0] : bytesMessage.properties().toString().getBytes("US-ASCII");

            byte[] body = bytesMessage.getBody();
            byte[] bytes = new byte[header.length + properties.length + body.length + 3];

            System.arraycopy(header,0,bytes,0,header.length);
            //ASCII 单元分隔符31
            bytes[header.length] = 31;

            System.arraycopy(properties,0,bytes,header.length + 1,properties.length);

            //ASCII 单元分隔符31
            bytes[header.length + properties.length + 1] = 31;

            System.arraycopy(body,0,bytes,header.length + properties.length + 2,body.length);

            //ASCII 记录分隔符30
            bytes[header.length + properties.length + body.length + 2] = 30;


            return bytes;


        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return null;

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


    public static void messaggToBytes(Message message){
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(100);
        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}


