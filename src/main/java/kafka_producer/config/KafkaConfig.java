package kafka_producer.config;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConfig {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private KafkaProducer<String, String> producer;
    public KafkaConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.2.152:9092,192.168.2.153:9092,192.168.2.154:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("linger.ms", 0);
        properties.put("retries", 1);
        producer = new KafkaProducer<String, String>(properties);
    }


    public void send(Map<Long, List<String>> datas)  {

        long count = 0;
        Integer flag = 0;

        ProducerRecord<String, String> messageList ;
        //获取需要发送的数量：
        Integer total = 0;
        for(Map.Entry<Long,List<String>> entry : datas.entrySet()){
            List<String> value = entry.getValue();
            if (value != null && value.size()>0){
                total += entry.getValue().size();
            }
        }
        for(Map.Entry<Long,List<String>> entry : datas.entrySet()){
            List<String> value = entry.getValue();
            if (value != null && value.size()>0){
                for (String s : value) {
                    String data = s;
                    messageList = new ProducerRecord<String, String>("calc", entry.getKey() + "", data);
                    count++;
                    flag ++;
                    producer.send(messageList, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            System.out.println(recordMetadata);
                            System.out.println(e);
                        }
                    });
                    producer.flush();

                }
            }
        }
        logger.info("*******Sent {}/{} units.", count, total);
    }
}
