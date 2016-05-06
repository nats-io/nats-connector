/**
 * 
 */
package io.nats.connector.plugins.spark;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Message;
import io.nats.connector.plugin.NATSConnector;
import io.nats.connector.plugin.NATSConnectorPlugin;
import io.nats.connector.plugin.NATSEvent;

/**
 * @author laugimethods
 *
 */
public class SparkPubConnector implements NATSConnectorPlugin, Serializable {

/**
	 * 
	 */
	public SparkPubConnector() {
		super();
		System.out.println("CREATE SparkPubConnector " + this);
	}

	//    NATSConnector connector = null;
    protected ConnectionFactory connectionFactory = null;
    protected Connection        connection        = null;
    protected String          configFile = null;
    protected String 			configuration;

    Logger logger = null;
    
    public static String subject = "TEST";

    boolean trace = false;

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnectorPlugin#onStartup(org.slf4j.Logger, io.nats.client.ConnectionFactory)
	 */
	@Override
	public boolean onStartup(Logger logger, ConnectionFactory factory) {
        this.logger = logger;
        
        this.connectionFactory = factory;

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
        return true;
	}

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnectorPlugin#onNATSMessage(io.nats.client.Message)
	 */
	@Override
	public void onNATSMessage(Message msg) {
		throw new UnsupportedOperationException("The " + this.getClass() + " plugin CANNOT be used to connect NATS into Spark");
	}

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnectorPlugin#onNATSEvent(io.nats.connector.plugin.NATSEvent, java.lang.String)
	 */
	@Override
	public void onNATSEvent(NATSEvent event, String message) {
		throw new UnsupportedOperationException("The " + this.getClass() + " plugin CANNOT be used to connect NATS into Spark");
	}

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnectorPlugin#onShutdown()
	 */
	@Override
	public void onShutdown() {
	}
	
	protected ConnectionFactory getConnectionFactory() throws Exception {
		if (connectionFactory == null) {
			connectionFactory = new ConnectionFactory(getProperties());
		}
		
		return connectionFactory;
	}

    private Properties getProperties() throws Exception{

        // add those from the VM.
        Properties p = new Properties(System.getProperties());

        if (configFile == null)
            return p;

        logger.debug("Loading properties from '" + configFile + '"');
        FileInputStream in = new FileInputStream(configFile);
        try {
            p.load(in);
        }
        catch (Exception e) {
            logger.error("Unable to load properties.", e);
            throw e;
        }
        finally {
            in.close();
        }

        return p;
    }

	protected Connection getConnection() throws Exception {
		if (connection == null) {
			connection = getConnectionFactory().createConnection();
			System.out.println("CREATE CONNECTION: " + connection + " for " + this);
		}
		return connection;
	}
	
	VoidFunction<String> onSparkInput = new VoidFunction<String>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void call(String str) throws Exception {
			System.out.println(" :::::::: " + str);
//			List<String> l = getSubjectsFromChannel(channelOrPattern);
	        
			Message natsMessage = new Message();
			
			byte[] payload = str.getBytes();
	        natsMessage.setData(payload, 0, payload.length);
	        
//	        for (String s : l) {
	            natsMessage.setSubject(subject);
	            // new Connector().publish(natsMessage);
	            getConnection().publish(natsMessage);

//	            logger.trace("Send Redis ({}) -> NATS ({})", channelOrPattern, s);
//	        }
		}    		
	};

}
