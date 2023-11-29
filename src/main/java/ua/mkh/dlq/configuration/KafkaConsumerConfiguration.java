package ua.mkh.dlq.configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;
import ua.mkh.dlq.dto.TransactionDto;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfiguration {

    private static final long INTERVAL = 10_000;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, TransactionDto>> listenerContainerFactory(
            ConsumerFactory<String, TransactionDto> consumerFactory) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, TransactionDto>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setObservationEnabled(true);
        var defaultErrorHandler =
                new DefaultErrorHandler(new FixedBackOff(INTERVAL, FixedBackOff.UNLIMITED_ATTEMPTS));

        defaultErrorHandler.setAckAfterHandle(true);
        factory.setCommonErrorHandler(defaultErrorHandler);

        return factory;
    }

    @Bean
    public ConsumerFactory<String, TransactionDto> consumerFactory() {
        var jsonDeserializer =
                new ErrorHandlingDeserializer<>(new JsonDeserializer<>(TransactionDto.class));

        return new DefaultKafkaConsumerFactory<>(consumerConfig(), new StringDeserializer(), jsonDeserializer);
    }

    @Bean
    public ConsumerAwareListenerErrorHandler consumerAwareListenerErrorHandler() {
        return (message, exception, consumer) -> {
            System.out.println("Kafka ErrorHandler:" + exception.getCause().getMessage());
            throw exception;
        };
    }

    public Map<String, Object> consumerConfig() {
        return new HashMap<>() {{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }};
    }
}
