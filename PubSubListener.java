package com.homedepot.di.dc.osc.commons.pubsub;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.spanner.SpannerException;
import com.google.pubsub.v1.PubsubMessage;
import com.homedepot.di.dc.osc.commons.config.MetricsHelper;
import com.homedepot.di.dc.osc.commons.exception.BusinessException;
import com.homedepot.di.dc.osc.commons.exception.InvalidHdwMessageException;
import com.homedepot.di.dc.osc.commons.exception.MessageNotConfiguredException;
import com.homedepot.di.dc.osc.commons.exception.PersistenceException;
import com.homedepot.di.dc.osc.commons.pubsub.hdwmessage.HDWPubSubConverter;
import com.homedepot.di.dc.osc.commons.pubsub.hdwmessage.HdwMessage;
import com.homedepot.di.dc.osc.commons.pubsub.hdwmessage.HdwPubSubMessage;
import com.homedepot.di.dc.osc.commons.pubsub.hdwmessage.PubSubConverter;
import com.homedepot.di.dc.osc.commons.pubsub.hdwmessage.ReceiverUtils;
import com.homedepot.di.dc.osc.commons.stackdriver.StackDriverErrorReportingHelper;
import io.micrometer.core.instrument.Metrics;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;


@Service
@ConditionalOnProperty(name = "google.cloud.pubsub.subscriptions[0].topic")
public class PubSubListener implements MessageReceiver {
    private final static Logger LOGGER = LoggerFactory.getLogger(PubSubListener.class);

    @Value("${spring.application.name}")
    private String applicationName;

    private PubSubMessageHandler pubSubMessageHandler;
    private ReceiverUtils receiverUtils;
    private PubSubConverter hdwPubSubData;
    //TODO: remove once we can retire the old message version
    static final String COMMON_FORMAT = "commonFormat";

    @Autowired
    public PubSubListener(PubSubMessageHandler pubSubMessageHandler,
                          PubSubConverter hdwPubSubData,
                          String applicationName) {
        this.pubSubMessageHandler = pubSubMessageHandler;
        this.receiverUtils = new ReceiverUtils();
        this.hdwPubSubData = hdwPubSubData;
        this.applicationName = applicationName;
    }

    //This is where message from a subscription are received. Normally subscription and receivers would be one to one
    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
        try {
            // log the original raw PubSub message
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Got message from Pub/Sub. Raw value: {}", message.getData().toStringUtf8());
            }
            LOGGER.debug("Got message from Pub/Sub. Raw value: %s", message.getData());
            Metrics.counter("pubsub_receive_counter").increment();

            try {
                //TODO: All message should be subclass of HdwMessage.
                if (!message.containsAttributes(COMMON_FORMAT)) {
                    LOGGER.info("Processing original HdwMessage");
                    HdwMessage hdwMessage = hdwPubSubData.decode(message);
                    if (hdwMessage != null) {
                        // let consumers handle it or throw exception as needed
                        pubSubMessageHandler.routeMessage(hdwMessage);
                    } // else we <commons> will ack it cause it is unsupported version
                } else {

                    HdwPubSubMessage hdwMessage = hdwPubSubData.decodeNew(message);
                    LOGGER.info("Processing Common HdwPubSubMessage");
                    if (hdwMessage != null) {
                        // let consumers handle it or throw exception as needed
                        pubSubMessageHandler.routeMessage(hdwMessage);
                    } // else we <commons> will ack it cause it is unsupported version

                }
            } catch (MessageNotConfiguredException e) {
                LOGGER.trace(e.getMessage(), e);
                // this message will be acked because it is unsupported
            }

            // above will throw and bypass this depending on context/exception
            consumer.ack();
            Metrics.counter("pubsub_ack_counter", MetricsHelper.getPubsubTagsByHdwMessage(message)).increment();
        } catch (InvalidHdwMessageException invalidMsgEx) {

            try {
                if (invalidMsgEx.getHdwMessage() != null) {
                    if (invalidMsgEx.getCause() != null) {
                        LOGGER.error(receiverUtils.logRawMessageJSON(invalidMsgEx.getHdwMessage(),
                                invalidMsgEx.getMessage(),
                                "",
                                "Subscription Message Error: message acked"
                        ).toString(), invalidMsgEx.getCause());
                    } else {

                        // If the particular app is not configured to read this message, it is not an error
                        if (invalidMsgEx.getMessage().contains("Application.yml not configured for this message")) {
                            LOGGER.info(receiverUtils.logRawMessageJSON(invalidMsgEx.getHdwMessage(),
                                    invalidMsgEx.getMessage(),
                                    "",
                                    "Subscription Message Error: message acked"
                            ).toString(), invalidMsgEx);
                        } else {
                            LOGGER.error(receiverUtils.logRawMessageJSON(invalidMsgEx.getHdwMessage(),
                                    invalidMsgEx.getMessage(),
                                    "",
                                    "Subscription Message Error: message acked"
                            ).toString(), invalidMsgEx);
                        }

                    }

                } else if (invalidMsgEx.getCause() != null) {
                    LOGGER.error(receiverUtils.logRawMessageJSON(message,
                            invalidMsgEx.getMessage(),
                            "",
                            "Subscription Message Error: message acked"
                    ).toString(), invalidMsgEx.getCause());
                } else {
                    LOGGER.error(receiverUtils.logRawMessageJSON(message,
                            invalidMsgEx.getMessage(),
                            "",
                            "Subscription Message Error: message acked"
                    ).toString(), invalidMsgEx);
                }


            } catch (Exception e1) {
                LOGGER.warn(e1.getMessage());
            }
            //this is due to either invalid message or configuration is wrong so ack default.
            consumer.ack();
            Metrics.counter("pubsub_autoack_counter", MetricsHelper.getPubsubTagsByHdwMessage(message)).increment();

        } catch (SpannerException e) {
            StackDriverErrorReportingHelper.logCustomErrorEvent(e, applicationName);
            // log it first
            try {
                LOGGER.warn(receiverUtils.logRawMessageJSON(message, e.getMessage(), ExceptionUtils.getStackTrace(e),
                        "SpannerException").toString());
            } catch (JSONException e1) {
                LOGGER.warn(e1.getMessage());
            }
            // if the spanner exception is caused because the data has already been inserted into the db,
            // acknowledge the message so you don't continue to receive it indefinitely
            if ("ALREADY_EXISTS".equals(e.getErrorCode().name())) {
                consumer.ack();
                Metrics.counter("pubsub_ack_counter", MetricsHelper.getPubsubTagsByHdwMessage(message)).increment();
            } else {
                consumer.nack();
                Metrics.counter("pubsub_nack_counter", MetricsHelper.getPubsubTagsByHdwMessage(message)).increment();
            }

        } catch (BusinessException e) {

            try {
                LOGGER.warn(receiverUtils.logRawMessageJSON(message, e.getMessage(), ExceptionUtils.getStackTrace(e),
                        "BusinessException").toString());
            } catch (JSONException e1) {
                LOGGER.warn(e1.getMessage());
            }
            // ACK! remove from queue
            consumer.ack();
            Metrics.counter("pubsub_ack_counter", MetricsHelper.getPubsubTagsByHdwMessage(message)).increment();

        } catch (PersistenceException e) {
            StackDriverErrorReportingHelper.logCustomErrorEvent(e, applicationName);
            // log it
            try {
                LOGGER.warn(receiverUtils.logRawMessageJSON(message, e.getMessage(), ExceptionUtils.getStackTrace(e),
                        "PersistenceException").toString());
            } catch (JSONException e1) {
                LOGGER.warn(e1.getMessage());
            }

            // if the persistence exception is caused because the data has already been inserted into the db,
            // acknowledge the message so you don't continue to receive it indefinitely
            if (e.getCause().getCause().toString().contains("ALREADY_EXISTS")) {
                consumer.ack();
                Metrics.counter("pubsub_ack_counter", MetricsHelper.getPubsubTagsByHdwMessage(message)).increment();
            } else {
                consumer.nack();
                Metrics.counter("pubsub_nack_counter", MetricsHelper.getPubsubTagsByHdwMessage(message)).increment();
            }

        } catch (Exception e) {
            StackDriverErrorReportingHelper.logCustomErrorEvent(e);
            try {
                LOGGER.error(receiverUtils.logRawMessageJSON(message, e.getMessage(), ExceptionUtils.getStackTrace(e),
                        "Exception").toString());
            } catch (JSONException e1) {
                LOGGER.warn(e1.getMessage());
            }
            // not Spanner, Business, Persistence - something's wrong with current runtime - retry!
            consumer.nack();
            Metrics.counter("pubsub_nack_counter", MetricsHelper.getPubsubTagsByHdwMessage(message)).increment();
        }
    }

}
