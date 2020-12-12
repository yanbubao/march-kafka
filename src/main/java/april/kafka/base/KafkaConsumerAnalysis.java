package april.kafka.base;

import april.kafka.faststart.ConsumerFastStart;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author yanzx
 */
public class KafkaConsumerAnalysis {

    private static final Logger log = LoggerFactory.getLogger(ConsumerFastStart.class);

    public static final String brokerList = "localhost:9092";

    public static final String topicName = "topic-demo";

    public static final String groupId = "group.demo";

    private static Properties initConfig() {

        Properties properties = new Properties();

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return properties;
    }

    public static void main(String[] args) {

        Properties properties = initConfig();

        // 创建消费者客户端实例
        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(properties);

        // 订阅主题
        consumer.subscribe(Collections.singletonList(topicName));

        // 消费消息
        do {
            ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(10000));

            for (ConsumerRecord<Object, Object> record : records) {

                log.info("topic: {}, partition: {}, message: {}",
                        record.topic(), record.partition(), record.value());
            }
        } while (true);

    }
}
