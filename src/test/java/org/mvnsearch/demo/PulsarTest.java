package org.mvnsearch.demo;

import org.apache.pulsar.client.api.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

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

    @BeforeAll
    public void setUp() throws Exception {
        client = PulsarClient.builder().serviceUrl(SERVICE_URL).build();
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
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(TOPIC_NAME)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("Demo-1")
                .subscribe();
        while (true) {
            consumer.receiveAsync().thenAccept(message -> {
                System.out.println(new String(message.getData()));
                try {
                    consumer.acknowledge(message);
                } catch (Exception ignore) {

                }
            });
        }
    }
}
