package ua.mkh.dlq.configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;
import ua.mkh.dlq.dto.TransactionDto;

import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfiguration {

    private static final long INTERVAL = 10_000;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, TransactionDto>> listenerContainerFactory(
            ConsumerFactory<String, TransactionDto> consumerFactory, DefaultErrorHandler defaultErrorHandler) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, TransactionDto>();

        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setObservationEnabled(true);
        factory.setCommonErrorHandler(defaultErrorHandler);
        return factory;
    }

    @Bean
    public DefaultErrorHandler defaultErrorHandler(DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
        var defaultErrorHandler = new DefaultErrorHandler(deadLetterPublishingRecoverer);

        defaultErrorHandler.addRetryableExceptions(RuntimeException.class);
        defaultErrorHandler.addNotRetryableExceptions(SocketException.class);
        defaultErrorHandler.setAckAfterHandle(true);

        defaultErrorHandler.setBackOffFunction((record, exception) -> {
            if (exception instanceof RuntimeException) {
                return new FixedBackOff(0, 10L);
            }
            return new FixedBackOff(INTERVAL, FixedBackOff.UNLIMITED_ATTEMPTS);
        });

        return defaultErrorHandler;
    }

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<String, TransactionDto> kafkaDlqTemplate) {

        return new DeadLetterPublishingRecoverer(kafkaDlqTemplate,
                (consumerRecord, exception) -> new TopicPartition(consumerRecord.topic() + "_dlq", consumerRecord.partition()));
    }

    @Bean
    public KafkaTemplate<String, TransactionDto> kafkaDlqTemplate() {
        return new KafkaTemplate<>(dlqProducerFactory());
    }

    @Bean
    public ProducerFactory<String, TransactionDto> dlqProducerFactory() {
        return new DefaultKafkaProducerFactory<>(dlqProducerConfig());
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

    public Map<String, Object> dlqProducerConfig() {
        return new HashMap<>() {{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        }};
    }
}
