package org.mvnsearch.demo;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.EncryptionContext;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Optional;

/**
 * Pulsar Message wrapper: add acknowledgeAsync and negativeAcknowledge
 *
 * @author linux_china
 */
public class PulsarMessageWrapper<T> implements Message<T> {
    private Consumer<T> consumer;
    private Message<T> delegate;

    public PulsarMessageWrapper(Consumer<T> consumer, Message<T> message) {
        this.consumer = consumer;
        this.delegate = message;
    }

    @Override
    public Map<String, String> getProperties() {
        return this.delegate.getProperties();
    }

    @Override
    public boolean hasProperty(String name) {
        return this.delegate.hasProperty(name);
    }

    @Override
    public String getProperty(String name) {
        return this.delegate.getProperty(name);
    }

    @Override
    public byte[] getData() {
        return this.delegate.getData();
    }

    @Override
    public T getValue() {
        return this.delegate.getValue();
    }

    @Override
    public MessageId getMessageId() {
        return this.delegate.getMessageId();
    }

    @Override
    public long getPublishTime() {
        return this.delegate.getPublishTime();
    }

    @Override
    public long getEventTime() {
        return this.delegate.getEventTime();
    }

    @Override
    public long getSequenceId() {
        return this.delegate.getSequenceId();
    }

    @Override
    public String getProducerName() {
        return this.delegate.getProducerName();
    }

    @Override
    public boolean hasKey() {
        return this.delegate.hasKey();
    }

    @Override
    public String getKey() {
        return this.delegate.getKey();
    }

    @Override
    public boolean hasBase64EncodedKey() {
        return this.delegate.hasBase64EncodedKey();
    }

    @Override
    public byte[] getKeyBytes() {
        return this.delegate.getKeyBytes();
    }

    @Override
    public boolean hasOrderingKey() {
        return this.delegate.hasOrderingKey();
    }

    @Override
    public byte[] getOrderingKey() {
        return this.delegate.getOrderingKey();
    }

    @Override
    public String getTopicName() {
        return this.delegate.getTopicName();
    }

    @Override
    public Optional<EncryptionContext> getEncryptionCtx() {
        return this.delegate.getEncryptionCtx();
    }

    @Override
    public int getRedeliveryCount() {
        return this.delegate.getRedeliveryCount();
    }

    @Override
    public byte[] getSchemaVersion() {
        return this.delegate.getSchemaVersion();
    }

    @Override
    public boolean isReplicated() {
        return this.delegate.isReplicated();
    }

    @Override
    public String getReplicatedFrom() {
        return this.delegate.getReplicatedFrom();
    }

    public Mono<Void> acknowledgeAsync() {
        return Mono.fromFuture(consumer.acknowledgeAsync(delegate.getMessageId()));
    }

    public void negativeAcknowledge() {
        consumer.negativeAcknowledge(delegate.getMessageId());
    }
}
