package ua.mkh.dlq.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ua.mkh.dlq.dto.TransactionDto;

@Component
public class KafkaTransactionConsumer {

    @KafkaListener(
            topics = "transactions",
            groupId = "groupId",
            containerFactory = "listenerContainerFactory",
            errorHandler = "consumerAwareListenerErrorHandler"
    )
    public void consume(TransactionDto data) {
        if (data.counter() % 2 == 0) {
            throw new RuntimeException("Consumer exception");
        }
        System.out.println("Consume data: " + data);
    }

}
