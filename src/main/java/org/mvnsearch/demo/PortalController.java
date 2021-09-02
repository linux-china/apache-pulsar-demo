package org.mvnsearch.demo;

import com.github.lhotari.reactive.pulsar.adapter.MessageSpec;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageSender;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Portal controller
 *
 * @author linux_china
 */
@RestController
public class PortalController {
    private ReactiveMessageSender<String> messageSender;

    public PortalController(ReactivePulsarClient reactivePulsarClient) {
        messageSender = reactivePulsarClient
                .messageSender(Schema.STRING)
                .topic("test-topic")
                .maxInflight(100)
                .build();
    }

    @GetMapping("/")
    public String index() {
        return "Hello world!";
    }

    @PostMapping("/send")
    public Mono<String> send(@RequestBody String body) {
        return messageSender
                .sendMessage(Mono.just(MessageSpec.of(body)))
                .map(messageId -> "Message: " + messageId);
    }
}
