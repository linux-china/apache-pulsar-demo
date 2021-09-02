package org.mvnsearch.demo;

import com.github.lhotari.reactive.pulsar.adapter.MessageResult;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumer;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import org.apache.pulsar.client.api.*;
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
        reactivePulsarClient = ReactivePulsarClient.create(client);
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
                .consumerConfigurer(consumerBuilder -> consumerBuilder.subscriptionName("sub-1"))
                .build();
        messageConsumer.consumeMessages(messageFlux -> messageFlux.map(message -> MessageResult.acknowledge(message.getMessageId(), message.getValue())))
                .subscribe(System.out::println);
        latch.await();
    }
}
