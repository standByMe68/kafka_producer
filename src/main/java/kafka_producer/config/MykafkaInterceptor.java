package kafka_producer.config;

import kafka_producer.service.KafkaSendTest;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class MykafkaInterceptor implements ProducerInterceptor<String,String> {

    int success = 0;
    int error = 0;

    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        System.out.println(producerRecord.toString());
        System.out.println("发送前拦截");
        int key = Integer.valueOf(String.valueOf(producerRecord.key()));
        KafkaSendTest.count++;
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e != null){
            error++;
        }else {
            success++;
        }
    }

    @Override
    public void close() {

        System.out.println("失败数量"+error);
        System.out.println("成功数量:"+success);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
