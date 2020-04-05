package com.homedepot.di.dc.osc.commons.pubsub;

import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.info.Info;
import org.springframework.boot.actuate.info.InfoContributor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
@ConditionalOnProperty(prefix="google.cloud.pubsub", name={"topic.name", "subscriptions[0].topic"})

public class PubSubInfoContributor implements InfoContributor {

    private PubSubSetup pubSubProvider;
    private final static String INFO_KEY = "pubsub";
    @Value("${spring.application.name}")
    private String applicationName;

    @Autowired
    public PubSubInfoContributor(PubSubSetup pubSubProvider) {
        this.pubSubProvider = pubSubProvider;
    }

    @Override
    public void contribute(Info.Builder builder) {
        List<String> topics = pubSubProvider.getTopics().stream()
                .map(ProjectTopicName::getTopic)
                .collect(Collectors.toList());
        List<String> subscriptions = pubSubProvider.getSubscriptions().stream()
                .map(ProjectSubscriptionName::getSubscription)
                .collect(Collectors.toList());

        PubSubInfo info = new PubSubInfo();
        info.setName(pubSubProvider.getAppMetaRepoData().getClientId());
        info.setTopics(topics);
        info.setSubscriptions(subscriptions);
        info.setEditor(pubSubProvider.getPubSubClient().isPubSubEditor());
        info.setLocal(pubSubProvider.getAppMetaRepoData().isLocal());

        builder.withDetail(INFO_KEY, info);
    }

    public static class PubSubInfo {
        private String name;
        private List<String> topics;
        private List<String> subscriptions;
        private boolean local;
        private boolean editor;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<String> getTopics() {
            return topics;
        }

        public void setTopics(List<String> topics) {
            this.topics = topics;
        }

        public List<String> getSubscriptions() {
            return subscriptions;
        }

        public void setSubscriptions(List<String> subscriptions) {
            this.subscriptions = subscriptions;
        }

        public boolean isLocal() {
            return local;
        }

        public void setLocal(boolean local) {
            this.local = local;
        }

        public boolean isEditor() {
            return editor;
        }

        public void setEditor(boolean editor) {
            this.editor = editor;
        }
    }
}
