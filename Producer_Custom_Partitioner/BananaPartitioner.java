package com.tacademy;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class BananaPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) { }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numParitions = partitions.size();

        // 키가 Null이거나 String 타입이 아닐 경우 예외
        if(keyBytes == null || (!(key instanceof String)))
            throw new InvalidRecordException("We expect all messages to hava customer name ad key");

        // key 가 Banana일 경우 마지막 파티션에 저장
        // 하드코딩 대신 configure 메소드를 사용해서 값을 넣는 것이 더 바람직
        if(((String) key).equals("Banana"))
            return numParitions -1;

        // 그 외의 레코드들은 해시 값을 구하여 나머지 파티션들 중 하나에 대응시킨다.
        // (numPartitions-1) : Banana 레코드가 저장되는 마지막 파티션 제외
        return (Math.abs(Utils.murmur2(keyBytes)) % (numParitions-1));
    }

    @Override
    public void close() { }
}
