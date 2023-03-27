package org.mvnsearch.demo;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
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
        return AdaptedReactivePulsarClientFactory.create(pulsarClient);
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
