package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

public class DefaultBytesMessage implements BytesMessage{

    private KeyValue headers;
    private KeyValue properties;
    private byte[] body;

    public void setHeaders(KeyValue headers) {
        this.headers = headers;
    }

    public void setProperties(KeyValue properties) {
        this.properties = properties;
    }

    public DefaultBytesMessage(byte[] body) {
        this.body = body;
    }
    @Override public byte[] getBody() {
        return body;
    }

    @Override public BytesMessage setBody(byte[] body) {
        this.body = body;
        return this;
    }

    @Override public KeyValue headers() {
        return headers;
    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message putHeaders(String key, int value) {
        if (headers == null) headers = new DefaultKeyValue();
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, long value) {
        if (headers == null) headers = new DefaultKeyValue();
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, double value) {
        if (headers == null) headers = new DefaultKeyValue();
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, String value) {
        if (headers == null) headers = new DefaultKeyValue();
        headers.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, int value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, long value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, double value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, String value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

//    @Override
//    public void writeExternal(ObjectOutput out) throws IOException {
//        out.writeObject(headers);
//        out.writeObject(properties);
//        out.write(body);
//    }
//
//    @Override
//    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
//        headers = (KeyValue) in.readObject();
//        properties = (KeyValue) in.readObject();
//        in.read(body);
//    }
}
