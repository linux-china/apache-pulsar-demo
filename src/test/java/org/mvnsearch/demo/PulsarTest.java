package org.mvnsearch.demo;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumer;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

/**
 * Apache Pulsar test
 *
 * @author linux_china
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PulsarTest {
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC_NAME = "test-topic";
    private PulsarClient client;
    private ReactivePulsarClient reactivePulsarClient;

    @BeforeAll
    public void setUp() throws Exception {
        client = PulsarClient.builder().serviceUrl(SERVICE_URL).build();
        reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(client);
    }

    @AfterAll
    public void tearDown() throws Exception {
        client.close();
    }

    @Test
    public void testProduce() throws Exception {
        Producer<byte[]> producer = client.newProducer()
                .topic(TOPIC_NAME)
                .compressionType(CompressionType.LZ4)
                .create();
        IntStream.range(1, 5).forEach(i -> {
            String content = String.format("hi-pulsar-%d", i);
            try {
                MessageId msgId = producer.send(content.getBytes());
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testSubscribe() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        ReactiveMessageConsumer<String> messageConsumer = reactivePulsarClient
                .messageConsumer(Schema.STRING)
                .topic(TOPIC_NAME)
                .subscriptionName("sub-1")
                .build();
        messageConsumer.consumeMany(messageFlux -> messageFlux.map(message -> {
                    System.out.println(message.getProperties());
                    return MessageResult.acknowledge(message.getMessageId(), message.getValue());
                }))
                .subscribe(System.out::println);
        latch.await();
    }
}
