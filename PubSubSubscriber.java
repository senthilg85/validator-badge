package com.homedepot.di.dc.osc.commons.pubsub;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.homedepot.di.dc.osc.commons.spanner.GoogleCloudProperties;
import com.homedepot.di.dc.osc.commons.stackdriver.StackDriverErrorReportingHelper;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(value = "google.cloud.pubsub.subscriptions[0].topic")
public class PubSubSubscriber {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubSubscriber.class);

    private GoogleCloudProperties googleCloudProperties;
    private PubSubSetup pubSubSetup;
    private MessageReceiver messageReceiver;
    private ArrayList<Subscriber> subscriberArrayList;
    private TransportChannelProvider emulatorTransportChannelProvider;
    private CredentialsProvider emulatorCredentialsProvider;

    @Autowired
    public PubSubSubscriber(GoogleCloudProperties googleCloudProperties,
                            PubSubSetup pubSubSetup, MessageReceiver messageReceiver,
                            Optional<TransportChannelProvider> transportChannelProvider,
                            Optional<CredentialsProvider> credentialsProvider) {
        this.googleCloudProperties = googleCloudProperties;
        this.pubSubSetup = pubSubSetup;
        this.messageReceiver = messageReceiver;
        this.subscriberArrayList = new ArrayList<>();
        //it can have nulls
        this.emulatorTransportChannelProvider = transportChannelProvider.orElse(null);
        this.emulatorCredentialsProvider = credentialsProvider.orElse(null);
    }

    //This is where the subscripts are bound to GCP (The subscriptions are already made(in GCP) by this time).
    @EventListener(ApplicationReadyEvent.class)
    public void registerAllSubscriptions(){
        /*
         * In most case one handlers would be mapped to just one subscription this is just an
         * example of how to register with subscriptions.
         */

        ArrayList<GoogleCloudProperties.Subscription> subscriptionGoogleCloudProperties =
                (ArrayList<GoogleCloudProperties.Subscription>) googleCloudProperties.getPubsub().getSubscriptions();
        for(GoogleCloudProperties.Subscription subscription: subscriptionGoogleCloudProperties) {
            startReceiver(messageReceiver, googleCloudProperties, pubSubSetup, subscription.getTopic()
                    , subscription.isForce(), googleCloudProperties.getPubsub().getNumberOfRetries());
        }
    }

    //This method registers a subscription to GCP PubSub
    public void startReceiver(MessageReceiver messageReceiver, GoogleCloudProperties googleCloudProperties,
                              PubSubSetup pubSubSetup, String topic, boolean force, int retries) {
        boolean didConnect = false;
        ProjectSubscriptionName subscriptionName = null;
        for (int iteration = 0; iteration < retries; iteration++) {
            try {
                subscriptionName = pubSubSetup.getOrCreateSubscription(topic);
                LOG.info("Starting subscription ({}) ...",subscriptionName);
                Subscriber subscriber;
                if(StringUtils.isBlank(googleCloudProperties.getPubsub().getPubSubEmulatorHost())){
                    subscriber = Subscriber.newBuilder(subscriptionName, messageReceiver).build();
                } else {
                    subscriber = Subscriber.newBuilder(subscriptionName, messageReceiver)
                            .setChannelProvider(this.emulatorTransportChannelProvider)
                            .setCredentialsProvider(this.emulatorCredentialsProvider)
                            .build();
                }

                subscriberArrayList.add(subscriber);
                subscriber.addListener(
                        new Subscriber.Listener() {
                            @Override
                            public void failed(Subscriber.State from, Throwable failure) {
                                // Handle failure. This is called when the Subscriber encountered a fatal error and is shutting down.
                                LOG.error(failure.getMessage(), failure);
                            }
                        },
                        MoreExecutors.directExecutor());
                startSubscriber(subscriber);
                didConnect = true;
                break; // Breaks retry loop
            } catch(Exception e) {
                StackDriverErrorReportingHelper.logCustomErrorEvent(e);
                LOG.error(e.getMessage(), e);
                LOG.error("Tried to make Subscription ({})", subscriptionName);
                try {
                    //Wait time before retry
                    TimeUnit.MILLISECONDS.sleep(googleCloudProperties.getPubsub().getDelayBeforeRetryInMills());
                } catch (InterruptedException e1) {
                    StackDriverErrorReportingHelper.logCustomErrorEvent(e1);
                    LOG.warn(e1.getMessage(), e1);
                }
            }
        }

        //Check if a valid connection was made
        if(!didConnect){
            String errorMessage =  "Was not able to connect to subscription ("+subscriptionName+"): number of trys: ("+retries+")"
                    + " DelayBeforeRetryInMills ("+googleCloudProperties.getPubsub().getDelayBeforeRetryInMills()+")";
            LOG.error(errorMessage);
            RuntimeException runtimeException = new RuntimeException(errorMessage);
            StackDriverErrorReportingHelper.logCustomErrorEvent(runtimeException);
            //Application should net start if connects to PubSub fail.
            throw runtimeException;
        }

    }

    @VisibleForTesting
    protected void startSubscriber(Subscriber subscriber) {
        subscriber.startAsync().awaitRunning();
    }

    @PreDestroy
    public void destroy() {
        subscriberArrayList.forEach( subscriber -> {
            if (subscriber != null) {
                LOG.info("Destroying subscriber({})...",subscriber);
                subscriber.stopAsync();
            }
        });
    }
}
