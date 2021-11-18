package org.mvnsearch.demo;

import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageSender;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Pulsar configuration
 *
 * @author linux_china
 */
@Configuration
public class PulsarConfiguration {
    @Value("${pulsar.url}")
    private String pulsarUrl;

    @Bean
    public PulsarClient pulsarClient() throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(pulsarUrl).build();
    }

    @Bean
    public ReactivePulsarClient reactivePulsarClient(@Autowired PulsarClient pulsarClient) {
        return ReactivePulsarClient.create(pulsarClient);
    }

    @Bean
    public ReactiveMessageSender<String> testTopicSender(@Autowired ReactivePulsarClient reactivePulsarClient) {
        return reactivePulsarClient
                .messageSender(Schema.STRING)
                .topic("test-topic")
                .maxInflight(100)
                .build();
    }
}
