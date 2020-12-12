package april.kafka.base;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author yanzx
 */
public class KafkaProducerAnalysis {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerAnalysis.class);

    private static final String brokerList = "localhost:9092";

    private static final String topicName = "topic-demo";

    private static Properties initConfig() {

        Properties properties = new Properties();

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");

        properties.put(ProducerConfig.RETRIES_CONFIG, 10);

        return properties;
    }

    public static void main(String[] args) {

        Properties properties = initConfig();

        // 创建Kafka生产者实例
        KafkaProducer<Object, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<Object, String> record = new ProducerRecord<>(topicName, "hello kafka! im async!");

        // sync(producer, record);

        async(producer, record);

        producer.close();

    }

    private static void sync(KafkaProducer<Object, String> producer, ProducerRecord<Object, String> record) {

        try {
            // send函数本身是异步的，链式调用get函数阻塞并等待kafka的响应
            // eg1.
            // producer.send(record).get();

            // eg2.
            Future<RecordMetadata> future = producer.send(record);

            RecordMetadata metadata = future.get();

            log.info(metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());

        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();

        }
    }

    private static void async(KafkaProducer<Object, String> producer, ProducerRecord<Object, String> record) {

        /**
         * onCompletion() 方法的两个参数是互斥的，
         * 消息发送成功时，metadata 不为 null 而 exception 为 null；
         * 消息发送异常时，metadata 为 null 而 exception 不为 null
         */

        producer.send(record, (metadata, exception) -> {

            if (exception != null) {

                exception.printStackTrace();
            } else {

                log.info(metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());
            }
        });

    }


}
