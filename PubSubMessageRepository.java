package com.homedepot.di.dc.osc.commons.pubsub;

import com.google.common.base.MoreObjects;
import com.google.pubsub.v1.PubsubMessage;
import com.homedepot.di.dc.osc.commons.spanner.GoogleCloudProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

@Repository
@ConditionalOnProperty(name = "google.cloud.pubsub.topic.name")
public class PubSubMessageRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubMessageRepository.class);
    private static final Integer DEFAULT_QUEUE_CAPACITY = 10000;

    private Queue<PubsubMessage> queue;

    @Autowired
    public PubSubMessageRepository(GoogleCloudProperties googleCloudConfig) {
        queue = new ArrayBlockingQueue<>(
                MoreObjects.firstNonNull(googleCloudConfig.getPubsub().getCapacity(), DEFAULT_QUEUE_CAPACITY));
    }

    private boolean hasNext() {
        return !queue.isEmpty();
    }

    private PubsubMessage next() {
        return queue.poll();
    }

    public void consumeAll(Consumer consumer) {
        if (hasNext()) {

            synchronized(this) {
                while (hasNext()) {
                    consumer.consume(next());
                }
            }

        }
    }

    public void addMessage(PubsubMessage message) {
        queue.offer(message);
    }

    public static interface Consumer {
        void consume(PubsubMessage message);
    }

}

