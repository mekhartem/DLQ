package ua.mkh.dlq.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ua.mkh.dlq.dto.TransactionDto;

@RestController
@RequiredArgsConstructor
@RequestMapping("api/v1/transactions")
public class TransactionController {

    private final KafkaTemplate<String, TransactionDto> kafkaTemplate;

    @PostMapping
    public void publish(@RequestBody TransactionDto transaction) {
        kafkaTemplate.send("transactions", transaction);
    }
}
