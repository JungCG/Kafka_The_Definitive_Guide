package com.tacademy;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomerSerializer implements Serializer<Customer> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // 구성할 것 없음
    }

    @Override
    /*
        Customer의 직렬화는 다음과 같이 한다
        customerID 를 나타내는 4바이트 정수(int)
        customerName의 길이를 나타내는 4바이트의 정수(int)로 UTF-8 인코딩 사용(만일 고객 이름이 Null이면 0)
        customerName을 나타내는 N바이트의 문자열(String)로 UTF-8 인코딩 사용
    */
    public byte[] serialize(String topic, Customer data) {
        try{
            byte[] serializedName;
            int stringSize;
            if(data == null)
                return null;
            else{
                if(data.getName() != null){
                    serializedName = data.getName().getBytes("UTF-8");
                    stringSize = serializedName.length;
                }else{
                    serializedName = new byte[0];
                    stringSize = 0;
                }
            }

            // 4(id) + 4(name) + stringSize
            ByteBuffer buffer = ByteBuffer.allocate(4+4+stringSize);
            buffer.putInt(data.getID());
            buffer.putInt(stringSize);
            buffer.put(serializedName);
            return buffer.array();
        }catch(Exception e){
            throw new SerializationException("Error when serializing Customer to byte[] "+e);
        }
    }

    @Override
    public void close() {
        // close 할것이 없다.
    }
}
