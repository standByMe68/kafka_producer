package kafka_producer.service;

import kafka_producer.config.KafkaConfig;
import lombok.SneakyThrows;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class KafkaSendTest implements ApplicationListener<ContextRefreshedEvent> {

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        KafkaConfig kafkaConfig = new KafkaConfig();
        Map<Long, List<String>> datas = new HashMap<>();

        datas.put(1000L, Arrays.asList("123", "123"));
        datas.put(101L, Arrays.asList("234", "123"));

        kafkaConfig.send(datas);
    }
}
