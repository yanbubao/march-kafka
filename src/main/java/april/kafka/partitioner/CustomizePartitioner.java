package april.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自定义分区器.
 * <p>
 * 消息在通过 send() 方法发往 broker 的过程中，有可能需要经过拦截器（Interceptor）、
 * 序列化器（Serializer）和分区器（Partitioner）的一系列作用之后才能被真正地发往 broker。
 *
 * @author yanzx
 */
public class CustomizePartitioner implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);

        int numPartitions = partitions.size();

        if (null == keyBytes) {
            return counter.getAndIncrement() % numPartitions;
        }

        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
