package ua.mkh.dlq;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import ua.mkh.dlq.dto.TransactionDto;

@SpringBootApplication
public class DlqApplication {
    public static void main(String[] args) {
        SpringApplication.run(DlqApplication.class, args);
    }

    @Bean
    CommandLineRunner commandLineRunner(KafkaTemplate<String, TransactionDto> kafkaTemplate) {
        return args -> kafkaTemplate.send("transactions", new TransactionDto("main message", 10));
    }

}
