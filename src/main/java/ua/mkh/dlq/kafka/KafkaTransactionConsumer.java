package ua.mkh.dlq.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransactionConsumer {

    @KafkaListener(
            topics = "transactions",
            groupId = "groupId"
    )
    public void consume(String data) {
        System.out.println("Consume data: " + data);
    }

}
