package com.sen.di.dc.osc.commons.pubsub;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.resourcemanager.ResourceManager;
import com.google.cloud.resourcemanager.ResourceManagerOptions;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PushConfig;
import com.sen.di.dc.osc.commons.spanner.GoogleCloudProperties;
import com.sen.di.dc.osc.commons.stackdriver.StackDriverErrorReportingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
@ConditionalOnClass(GoogleCloudProperties.Pubsub.class)

public class PubSubClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubClient.class);

    private TopicAdminClient topicAdminClient;
    private SubscriptionAdminClient subscriptionAdminClient;
    private String projectId;
    private Boolean pubSubAdmin;

    @Autowired
    public PubSubClient(TopicAdminClient topicAdminClient, SubscriptionAdminClient subscriptionAdminClient, String projectId){
        this.topicAdminClient = topicAdminClient;
        this.subscriptionAdminClient = subscriptionAdminClient;
        this.projectId = projectId;

    }

    /**
     * Checks if the service's default credentials are permitted to perform CREATE and DELETE
     * actions on Pub/Sub resources using the Google Cloud ResourceManager API.
     * @return whether the service has editor permissions
     */
    public boolean isPubSubEditor() {
        if (pubSubAdmin == null) {
            List<String> permissions = Arrays.asList(
                    "pubsub.topics.create",
                    "pubsub.topics.delete",
                    "pubsub.subscriptions.create",
                    "pubsub.subscriptions.delete"
            );
            ResourceManager resourceManager = getResourceManager();
            List<Boolean> results = resourceManager.testPermissions(this.projectId, permissions);
            pubSubAdmin = results.stream().allMatch(o -> o);
        }
        return pubSubAdmin;
    }

    public ResourceManager getResourceManager(){
        return ResourceManagerOptions.getDefaultInstance().getService();
    }

    /**
     * Checks if the topic exists. If not, create the topic if the service has editor permissions.
     * If the topic cannot be created, a Runtime Exception will be thrown and the service should
     * terminate immediately. Do not attempt to recover from this error.
     * @param topicName the exact topic name to be created
     */
    public void provisionTopic(ProjectTopicName topicName) {
        // Don't create the topic if it already exists
        if (topicExists(topicName)) {
            return;
        }
        // Check if authorized to create the topic
        if (!isPubSubEditor()) {
            String message = "Topic does not exist and user lacks permission to create it";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }
        // Create the topic
        this.topicAdminClient.createTopic(topicName);
    }

    boolean topicExists(ProjectTopicName topicName) {
        try {
            this.topicAdminClient.getTopic(topicName);
            return true;
        } catch (NotFoundException e) {
            StackDriverErrorReportingHelper.logCustomErrorEvent(e);
            return false;
        }
    }

    /**
     * Checks if the subscription exists. If not, create the subscription if the service has editor permissions.
     * If the subscription cannot be created, a Runtime Exception will be thrown and the service should
     * terminate immediately. Do not attempt to recover from this error.
     * @param subscriptionName the exact subscription name to be created
     * @param topicName the topic that the subscription should belong to
     */
    public void provisionSubscription(ProjectSubscriptionName subscriptionName, ProjectTopicName topicName) {
        // Don't create the subscription if it already exists
        if (subscriptionExists(subscriptionName)) {
            return;
        }
        // Check if authorized to create the subscription
        if (!isPubSubEditor()) {
            String message = "Subscription does not exist and user lacks permission to create it";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }
        this.subscriptionAdminClient.createSubscription(subscriptionName,
                topicName, PushConfig.getDefaultInstance(), 60);
    }

    boolean subscriptionExists(ProjectSubscriptionName subscriptionName) {
        try {
            this.subscriptionAdminClient.getSubscription(subscriptionName);
            return true;
        } catch (NotFoundException e) {
            StackDriverErrorReportingHelper.logCustomErrorEvent(e);
            return false;
        }
    }
}
