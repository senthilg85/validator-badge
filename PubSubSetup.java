package com.homedepot.di.dc.osc.commons.pubsub;

import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.homedepot.di.dc.osc.commons.spanner.GoogleCloudProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Configuration
@ConditionalOnClass(GoogleCloudProperties.Pubsub.class)

public class PubSubSetup {
    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubSetup.class);

    private static final String  LOCAL_PREFIX = "local";
    private PubSubClient pubSubClient;
    private AppMetaRepoData appMetaRepoData;
    private List<ProjectTopicName> topics = new ArrayList<>();
    private List<ProjectSubscriptionName> subscriptions = new ArrayList<>();
    private String projectId;

    @Autowired
    public PubSubSetup(AppMetaRepoData appMetaRepoData,
                          PubSubClient pubSubClient, String projectId) {
        this.appMetaRepoData = appMetaRepoData;
        this.pubSubClient = pubSubClient;
        this.projectId = projectId;
    }

    public List<ProjectTopicName> getTopics() {
        return topics;
    }

    public List<ProjectSubscriptionName> getSubscriptions() {
        return subscriptions;
    }

    public PubSubClient getPubSubClient() {
        return pubSubClient;
    }

    public AppMetaRepoData getAppMetaRepoData() {
        return appMetaRepoData;
    }

    /**
     * Generates a subscription for the given topic, with a base name of {@code <topic>-<client-name>},
     * where {@code <client-name>} is the client name as defined in {@link AppMetaRepoData#getClientId()}.
     * The resulting subscription will be context-aware, containing a branch name for feature branches
     * and a machine-specific client tag during local development.
     *
     * If the topic or subscription does not exist, the service will attempt to create it.
     * @param topic the topic to create a subscription for.
     * @return an appropriately contextualized subscription
     */
    public ProjectSubscriptionName getOrCreateSubscription(String topic) {
        LOGGER.info("getting or creating subscription for topic {}", topic);
        // Ensure that the topic exists
        ProjectTopicName topicName = getOrCreateTopic(topic, true);
        // Base name follows <topic>-<client>, e.g. inventory-core-asn-core
        String name = String.format("%s-%s", topicName.getTopic(), appMetaRepoData.getClientId());
        // Add context to the subscription
        name = contextualize(name);
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(this.projectId, name);

        if(pubSubClient.isPubSubEditor()){
            pubSubClient.provisionSubscription(subscriptionName, topicName);
        }
        subscriptions.add(subscriptionName);
        LOGGER.info("subscription exists and ready to receive, final name is {}", name);
        return subscriptionName;
    }

    /**
     * Generates a topic based on the client name as definened in {@link AppMetaRepoData#getClientId()}.
     * The resulting topic will be context-aware, containing a branch name for feature branches and a
     * machine-specific client tag during local development.
     *
     * If the topic does not exist, the service will attempt to create it.
     * @return an appropriately contextualized topic
     */
    public ProjectTopicName getOrCreateTopic() {
        return getOrCreateTopic(appMetaRepoData.getClientId(), false);
    }

    /**
     * Generates a topic based on the given name. The resulting topic will be context-aware,
     * containing a branch name for feature branches and a machine-specific client tag
     * during local development.
     *
     * If the topic does not exist, the service will attempt to create it.
     * @param name the base name of the generated topic
     * @return an appropriately contextualized topic
     */
    public ProjectTopicName getOrCreateTopic(String name) {
        return getOrCreateTopic(name, false);
    }

    /**
     * Generates a topic based on the given name. The resulting topic will be context-aware,
     * containing a branch name for feature branches and a machine-specific client tag
     * during local development.
     *
     * If the topic does not exist, the service will attempt to create it.
     * @param name the base name of the generated topic
     * @param force if true, do not add context to the topic name
     * @return an appropriately contextualized topic
     */
    public ProjectTopicName getOrCreateTopic(String name, boolean force) {
        LOGGER.info("getting or creating topic for {}", name);
        if (!force) {
            name = contextualize(name);
        } else {
            LOGGER.info("force = true, not adding context to {}", name);
        }
        ProjectTopicName topicName = ProjectTopicName.of(this.projectId, name);

        if(pubSubClient.isPubSubEditor()){
            pubSubClient.provisionTopic(topicName);
        }
        topics.add(topicName);
        LOGGER.info("topic exists and ready to publish, final name is {}", name);
        return topicName;
    }

    /**
     * Applies uniqueness to the given Pub/Sub resource. This ensures that multiple instances
     * of a given service do not consume each other's messages in lower lifecycles and local development.
     * The produces resources will conform to Pub/Sub resource naming conventions:
     * https://cloud.google.com/pubsub/docs/overview#names
     * @param name the base resource name
     * @return a modified variation of the resource name
     */
    public String contextualize(String name) {
        // Append the branch name if not master
        LOGGER.info("adding context to the following resource: {}", name);
        if (!appMetaRepoData.getBranchName().equalsIgnoreCase("master")) {
            String branchName = appMetaRepoData.getBranchName();
            name = String.format("%s-%s", name, branchName);
        }
        // Append the local prefix and a unique client ID if running locally
        if (appMetaRepoData.isLocal()) {
            name = String.format("%s-%s-%s", LOCAL_PREFIX, name, appMetaRepoData.getClientTag());
        }
        // Remove illegal characters
        name = name.replaceAll("[#/]", "-").toLowerCase();
        name = name.replaceAll("[^a-zA-Z0-9_.%~-]", "").toLowerCase();
        // Must start with a letter
        name = name.replaceAll("^[^a-zA-Z]+", "");
        // Ensure that the resource name doesn't exceed 255 characters
        if (name.length() > 255) {
            LOGGER.warn("the resulting resource name exceeded length limits, trimming: {}", name);
            name = name.substring(0, 255);
        }
        LOGGER.info("resource after adding context: {}", name);
        return name;
    }

}
