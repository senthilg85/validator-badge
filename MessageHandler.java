package com.homedepot.di.dc.osc.inventory.pubsub;

import com.google.cloud.spanner.SpannerException;
import com.homedepot.di.dc.osc.commons.config.HdwContext;
import com.homedepot.di.dc.osc.commons.exception.BusinessException;
import com.homedepot.di.dc.osc.commons.pubsub.PubSubMessageHandler;
import com.homedepot.di.dc.osc.commons.pubsub.hdwmessage.HdwMessage;
import com.homedepot.di.dc.osc.commons.pubsub.hdwmessage.HdwPubSubMessage;
import com.homedepot.di.dc.osc.inventory.pubsub.allocation.AllocationMessageHandler;
import com.homedepot.di.dc.osc.inventory.pubsub.fulfillment.FulfillmentMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Objects;

@Component
public class MessageHandler implements PubSubMessageHandler {

    private final static Logger LOGGER = LoggerFactory.getLogger(MessageHandler.class);

    private FulfillmentMessageHandler fulfillmentMessageHandler;
    private AllocationMessageHandler allocationMessageHandler;


    public MessageHandler(FulfillmentMessageHandler fulfillmentMessageHandler, AllocationMessageHandler allocationMessageHandler) {
        this.fulfillmentMessageHandler = fulfillmentMessageHandler;
        this.allocationMessageHandler = allocationMessageHandler;
    }

    @Override
    public void routeMessage(HdwPubSubMessage hdwPubSubMessage) {
        HdwContext hdwContext = new HdwContext(hdwPubSubMessage.getDcNumber(), hdwPubSubMessage.getOscAuthToken());
        LOGGER.info("Processing PubSub message: {}", hdwPubSubMessage);
        try {
            if (Objects.equals(hdwPubSubMessage.getAction(), "allocation_request")) {
                allocationMessageHandler.proccessAllocationPubSubMessage(hdwPubSubMessage, hdwContext);
            } else if (Arrays.asList("order-create", "order-info", "order-modify").contains(hdwPubSubMessage.getAction())) {
                fulfillmentMessageHandler.allocateOrder(hdwPubSubMessage, hdwContext);
            } else if (Objects.equals(hdwPubSubMessage.getAction(), "order-cancel")) {
                fulfillmentMessageHandler.cancelOrder(hdwPubSubMessage, hdwContext);
            }
        }catch (SpannerException e){
            if(e.getCause() instanceof BusinessException){
                LOGGER.error(e.getCause().getMessage());
                throw (BusinessException) e.getCause();
            }
        }
    }

    @Deprecated
    @Override
    public void routeMessage(HdwMessage message) {
        //implemented in frontend so do not delete method
    }
}

/**
 *
 *    allocation_request example:
 *
 *
        // How the message is formed in allocation engine, for reference
        // LPNs
        hdwMessageBody.setLpnNumber(lpnInventory.getLpnNumber());
        hdwMessageBody.setLastUpdatedTime(lpnInventory.getLastUpdatedTime());
        // Pick locations
        hdwMessageBody.setSku(inventory.getSku());
        hdwMessageBody.setQuantity(inventory.getQuantity().toString());
        hdwMessageBody.setPickLocationId(inventory.getPickLocationId());
        // both
        hdwMessageBody.setDemandType(AllocationConstants.STORE_ORDER);
        hdwMessageBody.setOrderId(allocationQueue.getOrderId());
        hdwMessageBody.setOrderLineId(allocationQueue.getOrderLineId());
        hdwMessageBody.setOrderReferenceOrderNumber(allocationQueue.getReferenceOrderNumber());
        hdwMessageBody.setOrderShipmentStopSequence(allocationQueue.getShipmentStopSequence());
        hdwMessageBody.setOutboundShi
 *
 * [
 *   {
 *     "queueId": "aaaaaa",
 *     "demandType": "store order",
 *     "orderId": "order-id-zac2",
 *     "orderLineId": "order-line-id-zac2",
 *     "orderReferenceOrderNumber": "",
 *     "orderLineNumber":"2",
 *     "orderShip
 *     "lpnNumber": "Lsim03798876493619",
 *     "lastUpdatedTime": "2019-03-16T16:26:04.463Z",
 *     "pickLocationId": null,
 *     "sku": null,
 *     "quantity": null
 *   }
 * ]
 *
 *
 */
