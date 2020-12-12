package april.kafka.faststart;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Kafka FastStart.
 *
 * @author yanzx
 */
public class ProducerFastStart {

    public static final String brokerList = "localhost:9092";

    public static final String topicName = "topic-demo";

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        properties.put("bootstrap.servers", brokerList);

        // 创建Kafka生产者实例
        KafkaProducer<Object, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<Object, String> record = new ProducerRecord<>(topicName, "hello kafka!");

        try {
            producer.send(record);

        } catch (Exception e) {
            e.printStackTrace();

        }

        producer.close();

    }

}
