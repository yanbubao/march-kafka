package april.kafka.base.customize;

import april.kafka.partitioner.CustomizePartitioner;
import april.kafka.pojo.Company;
import april.kafka.serializer.CompanySerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 自定义分区Producer.
 *
 * @author yanzx
 */
public class ProducerCustomizePartitioner {

    private static final Logger log = LoggerFactory.getLogger(ProducerCustomizePartitioner.class);

    private static final String brokerList = "localhost:9092";

    private static final String topicName = "topic-demo";

    private static Properties initConfig() {

        Properties properties = new Properties();

        /**
         * 注意，示例中消息的 key 对应的序列化器还是 StringSerializer，这个并没有改动。其实 key.serializer
         * 和 value.serializer 并没有太大的区别，读者可以自行修改 key 对应的序列化器，看看会不会有不一样的效果
         */

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");

        properties.put(ProducerConfig.RETRIES_CONFIG, 10);

        // 设置自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomizePartitioner.class.getName());

        return properties;
    }

    public static void main(String[] args) {

        Properties pros = initConfig();

        Company company = Company.builder().name("Lzm").address("Who learn").build();

        KafkaProducer<String, Company> producer = new KafkaProducer<>(pros);

        ProducerRecord<String, Company> record = new ProducerRecord<>(topicName, company);

        producer.send(record, (metadata, exception) -> {

            if (exception != null) {

                exception.printStackTrace();
            } else {

                log.info(metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());
            }
        });

        producer.close();
    }
}
