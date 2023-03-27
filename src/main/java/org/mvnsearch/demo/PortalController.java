package org.mvnsearch.demo;

import org.apache.pulsar.reactive.client.api.MessageSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
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
    private final ReactiveMessageSender<String> testTopicSender;

    public PortalController(ReactiveMessageSender<String> testTopicSender) {
        this.testTopicSender = testTopicSender;
    }

    @GetMapping("/")
    public String index() {
        return "Hello world!";
    }

    @PostMapping("/send")
    public Mono<String> send(@RequestBody String body) {
        return testTopicSender
                .sendOne(MessageSpec.of(body))
                .map(messageId -> "Message: " + messageId);
    }
}
