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
public class SparkPubConnector implements Serializable {

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
    protected Properties		properties;

    Logger logger = null;
    
    public static String subject = "TEST";

    boolean trace = false;
	
	protected ConnectionFactory getConnectionFactory() throws Exception {
		if (connectionFactory == null) {
			connectionFactory = new ConnectionFactory(getProperties());
		}
		
		return connectionFactory;
	}

    public void setProperties(Properties properties) {
		this.properties = properties;
	}

	private Properties getProperties(){
    	if (properties == null) {
    		properties = new Properties(System.getProperties());
    	}

    	return properties;
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
			System.out.println(properties + " :::::::: " + str);
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
