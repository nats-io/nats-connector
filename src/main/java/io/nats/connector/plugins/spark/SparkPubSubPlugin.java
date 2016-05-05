/**
 * 
 */
package io.nats.connector.plugins.spark;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;

import io.nats.client.ConnectionFactory;
import io.nats.client.Message;
import io.nats.connector.Connector;
import io.nats.connector.plugin.NATSConnector;
import io.nats.connector.plugin.NATSConnectorPlugin;
import io.nats.connector.plugin.NATSEvent;

/**
 * @author laugimethods
 *
 */
public class SparkPubSubPlugin implements NATSConnectorPlugin, Serializable {

    NATSConnector connector = null;
    Logger logger = null;
    
    String subject = "TEST";

    boolean trace = false;

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnectorPlugin#onStartup(org.slf4j.Logger, io.nats.client.ConnectionFactory)
	 */
	@Override
	public boolean onStartup(Logger logger, ConnectionFactory factory) {
        this.logger = logger;

        try {
 //           loadProperties();
 //           loadConfig();
        }
        catch (Exception e) {
            logger.error("Unable to initialize.", e);
            return false;
        }

        return true;
	}

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnectorPlugin#onNatsInitialized(io.nats.connector.plugin.NATSConnector)
	 */
	@Override
	public boolean onNatsInitialized(NATSConnector connector) {
		logger.info("" + this + " is initialized with NATSConnector: " + connector);
		
        this.connector = connector;
        try {
//            connector.subscribe(subject);
        }
        catch (Exception e)
        {
            logger.error("NATS Subscription error", e);
            return false;
        }

        return true;
	}

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnectorPlugin#onNATSMessage(io.nats.client.Message)
	 */
	@Override
	public void onNATSMessage(Message msg) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnectorPlugin#onNATSEvent(io.nats.connector.plugin.NATSEvent, java.lang.String)
	 */
	@Override
	public void onNATSEvent(NATSEvent event, String message) {
        // When a connection has been disconnected unexpectedly, NATS will
        // try to reconnect.  Messages published during the reconnect will
        // be buffered and resent, so there may be no need to do anything.
        // Connection disconnected - close JEDIS, buffer messages?
        // Reconnected - reconnect to JEDIS.
        // Closed:  should handle elsewhere.
        // Async error.  Notify, let admins handle these.
        switch (event)
        {
            case ASYNC_ERROR:
                logger.error("NATS Asynchronous error: " + message);
                break;
            case RECONNECTED:
                logger.info("Reconnected to the NATS cluster: " + message);
                // At this point, we may not have to do much.  Buffered NATS messages
                // may be flushed. and we'll buffer and flush the Redis messages.
                // Revisit this later if we need more buffering.
                break;
            case DISCONNECTED:
                logger.info("Disconnected from the NATS cluster: " + message);
                break;
            case CLOSED:
                logger.debug("NATS Event Connection Closed: " + message);
                // shudown - if this is a result of shutdown elsewhere,
                // there will be no effect.
 //               connector.shutdown();
                break;
            default:
                logger.warn("Unknown NATS Event: " + message);
        }
	}

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnectorPlugin#onShutdown()
	 */
	@Override
	public void onShutdown() {
	}
	
	VoidFunction<String> onSparkInput = new VoidFunction<String>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void call(String str) throws Exception {
			System.out.println(this + " ::::: " + connector + " :::::::: " + str);
			
			if (connector == null) {
				connector = new SparkToNATSConnector();
			}
//			List<String> l = getSubjectsFromChannel(channelOrPattern);
	        
			Message natsMessage = new Message();
			
			byte[] payload = str.getBytes();
	        natsMessage.setData(payload, 0, payload.length);
	        
//	        for (String s : l) {
	            natsMessage.setSubject(subject);
	            // new Connector().publish(natsMessage);
	            connector.publish(natsMessage);

//	            logger.trace("Send Redis ({}) -> NATS ({})", channelOrPattern, s);
//	        }
		}    		
	};

}
