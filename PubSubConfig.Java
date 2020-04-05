
package com.sen.di.dc.osc.commons.pubsub;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.sen.di.dc.osc.commons.spanner.GoogleCloudProperties;
import com.sen.di.dc.osc.commons.stackdriver.StackDriverErrorReportingHelper;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Configuration
@ConditionalOnClass(GoogleCloudProperties.Pubsub.class)
public class PubSubConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubConfig.class);

    @Bean
    @ConditionalOnMissingBean
    public PubSubSetup getPubSubSetup(@Value("${spring.application.name}") String applicationName, PubSubClient pubSubClient, String projectId) {
        return new PubSubSetup(new AppMetaRepoData(applicationName), pubSubClient, projectId);
    }

    @Bean
    public TransportChannelProvider getTransportChannelProvider(GoogleCloudProperties googleCloudProperties) {
        if (StringUtils.isNotBlank(googleCloudProperties.getPubsub().getPubSubEmulatorHost())) {
            return FixedTransportChannelProvider.create(GrpcTransportChannel.create(NettyChannelBuilder.forTarget(googleCloudProperties.getPubsub().getPubSubEmulatorHost())
                    .negotiationType(NegotiationType.PLAINTEXT)
                    .build()));
        } else {
            return null;
        }


    }

    @Bean
    public CredentialsProvider getCredentialsProvider(GoogleCloudProperties googleCloudProperties) {
        if (StringUtils.isNotBlank(googleCloudProperties.getPubsub().getPubSubEmulatorHost())) {
            return NoCredentialsProvider.create();
        } else {
            return null;
        }
    }

    /**
     * Gets or creates a Pub/Sub TopicAdminClient using the application-default credentials.
     * Services should not need to directly interact with this. Instead use methods in PubSubSetup.
     *
     * @return a TopicAdminClient
     * @see PubSubSetup#getOrCreateTopic()
     */
    @Bean
    public TopicAdminClient getTopicAdminClient(GoogleCloudProperties googleCloudProperties,
                                                Optional<TransportChannelProvider> transportChannelProvider,
                                                Optional<CredentialsProvider> credentialsProvider) {

        try {
            if (StringUtils.isNotBlank(googleCloudProperties.getPubsub().getPubSubEmulatorHost())) {
                //PubSub Emulator mode
                LOGGER.info("Using PubSub Emulator mode to create TopicAdminClient as the system env variable PUBSUB_EMULATOR_HOST was set");
                // Set the channel and credentials provider when creating a `TopicAdminClient`.
                return TopicAdminClient.create(
                        TopicAdminSettings.newBuilder()
                                .setTransportChannelProvider(transportChannelProvider.get())
                                .setCredentialsProvider(credentialsProvider.get())
                                .build());
            } else {
                return TopicAdminClient.create();
            }


        } catch (IOException e) {
            StackDriverErrorReportingHelper.logCustomErrorEvent(e);
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException("Unable to create Topic Admin Client", e);
        }
    }


    /**
     * Gets or creates a Pub/Sub SubscriptionAdminClient using the application-default credentials.
     * Services shouldnot need to directly interact with this. Instead, use methods in PubSubSetup.
     *
     * @return a SubscriptionAdminClient
     * @see PubSubSetup#getOrCreateSubscription(String)
     */
    @Bean
    public SubscriptionAdminClient getSubscriptionAdminClient(GoogleCloudProperties googleCloudProperties,
                                                              Optional<TransportChannelProvider> transportChannelProvider,
                                                              Optional<CredentialsProvider> credentialsProvider) {
        try {

            if (StringUtils.isNotBlank(googleCloudProperties.getPubsub().getPubSubEmulatorHost())) {
                return SubscriptionAdminClient.create(
                        SubscriptionAdminSettings.newBuilder()
                                .setTransportChannelProvider(transportChannelProvider.get())
                                .setCredentialsProvider(credentialsProvider.get())
                                .build());
            } else {
                return SubscriptionAdminClient.create();
            }


        } catch (IOException e) {
            StackDriverErrorReportingHelper.logCustomErrorEvent(e);
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException("Unable to create Subscription Admin Client", e);
        }
    }


    /**
     * Returns the default project ID for the current Google Cloud client context
     *
     * @return the default project ID
     */
    @Bean(name = "projectId")
    public String getProjectId() {
        return ServiceOptions.getDefaultProjectId();
    }

    /**
     * This method build the map of topic versions with key as "entity-action" and value as version
     *
     * @param googleCloudProperties
     * @return
     */
    @Bean(name = "topicVersionMap")
    public Map<String, List<Integer>> getTopicVersionMap(GoogleCloudProperties googleCloudProperties) {
        Map<String, List<Integer>> returnMap = new HashMap<>();
        if (googleCloudProperties.getPubsub().getTopic() != null
                && googleCloudProperties.getPubsub().getTopic().getEntities() != null) {

            String topicName = googleCloudProperties.getPubsub().getTopic().getName();
            googleCloudProperties.getPubsub().getTopic().getEntities().stream().forEach(entity -> {

                if (entity.getActions() != null) {
                    entity.getActions().stream().forEach(action -> {
                        //TODO: DEPRECATED. Delete after flat messages fully implemented in all applications
                        returnMap.put(
                                MessageFormat.format("{0}_{1}_{2}", topicName, entity.getName(), action.getName()),
                                action.getVersions());
                        // This is the needed key:value pairs for new flat messages
                        returnMap.put(
                                MessageFormat.format("{0}_{1}_{2}", topicName, "HdwPubSubMessage", action.getName()),
                                action.getVersions());
                    });
                }

            });
        }

        if (returnMap.isEmpty()) { // Set to null so that bean can be autowired with Optional
            return null;
        } else {
            return returnMap;
        }

    }

    /**
     * This method build the map of subscription versions with key as "topic_entity_action" and value as version
     *
     * @param googleCloudProperties
     * @return Map with key topic-entity-action and value as version
     */
    @Bean(name = "subscriptionVersionMap")
    public Map<String, Integer> getSubscriptionVersionMap(GoogleCloudProperties googleCloudProperties) {
        Map<String, Integer> returnMap = new HashMap<>();
        if (googleCloudProperties.getPubsub().getSubscriptions() != null) {

            googleCloudProperties.getPubsub().getSubscriptions().stream().forEach(s -> {
                if (s.getEntities() != null) {
                    s.getEntities().stream().forEach(entity -> {
                        if (entity.getActions() != null) {
                            entity.getActions().stream().forEach(action -> {
                                returnMap.put(
                                        MessageFormat.format("{0}_{1}_{2}", s.getTopic(), entity.getName(), action.getName()),
                                        action.getVersion());
                            });
                        }

                    });
                }

            });
        }

        if (returnMap.isEmpty()) { // Set to null so that bean can be autowired with Optional
            return null;
        } else {
            return returnMap;
        }
    }

}
