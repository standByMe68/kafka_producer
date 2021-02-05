package kafka_producer.service;

import kafka_producer.config.KafkaConfig;
import lombok.SneakyThrows;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import scala.Int;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class KafkaSendTest implements ApplicationListener<ContextRefreshedEvent> {

    public static Integer count=0;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        KafkaConfig kafkaConfig = new KafkaConfig();
        Map<Long, List<String>> datas = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            datas.put(Long.valueOf(i), Arrays.asList("\"{\\\"mpointId\\\":94940,\\\"expression\\\":\\\"max\\\",\\\"saveDT\\\":\\\"2021-01-29 23:00:00\\\",\\\"endDT\\\":\\\"2021-01-30 00:00:00\\\",\\\"type\\\":\\\"task\\\",\\\"cycle\\\":3600,\\\"point\\\":\\\"CALC-94940\\\",\\\"shiftName\\\":\\\"\\\",\\\"ftype\\\":\\\"aggregation\\\",\\\"shiftsTypeName\\\":\\\"\\\",\\\"startDT\\\":\\\"2021-01-29 23:00:00\\\",\\\"calcParms\\\":[{\\\"mpointid\\\":94772,\\\"code\\\":\\\"mpoint\\\",\\\"defaultval\\\":0,\\\"datasource\\\":\\\"AUTO\\\",\\\"id\\\":533,\\\"mpointName\\\":\\\"西北角-网关连接状态\\\",\\\"point\\\":\\\"1000037538-A1\\\",\\\"taskid\\\":523}],\\\"initialValue\\\":0,\\\"taskid\\\":523}\""));
        }

        kafkaConfig.send(datas);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(count);
    }
}
