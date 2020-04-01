package april.kafka.faststart;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Kafka FastStart.
 *
 * @author v_yanzixuan
 */
public class ConsumerFastStart {

    private static final Logger log = LoggerFactory.getLogger(ConsumerFastStart.class);

    public static final String brokerList = "localhost:9092";

    public static final String topicName = "topic-demo";

    public static final String groupId = "group.demo";

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        properties.put("bootstrap.servers", brokerList);
        // 消费者组名称
        properties.put("group.id", groupId);

        // 创建消费者客户端实例
        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(properties);

        // 订阅主题
        consumer.subscribe(Collections.singletonList(topicName));

        // 消费消息
        while (true) {
            ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(10000));

            for (ConsumerRecord<Object, Object> record : records) {

                log.info("topic: {}, partition: {}, message: {}", record.topic(), record.partition(), record.value());
            }
        }

    }
}
