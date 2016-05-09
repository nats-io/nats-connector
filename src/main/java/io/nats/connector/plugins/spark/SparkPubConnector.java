/**
 * 
 */
package io.nats.connector.plugins.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Message;

/**
 * @author laugimethods
 *
 */
public class SparkPubConnector implements Serializable {

	public static final String NATS_SUBJECTS = "nats.io.connector.spark.subjects";

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

    protected ConnectionFactory 	connectionFactory = null;
    protected Connection        	connection        = null;
    protected Properties			properties		  = null;
    protected Collection<String>	subjects;

    static final Logger logger = LoggerFactory.getLogger(SparkPubConnector.class);
    
    /**
	 * @param properties
	 * @param subjects
	 */
    protected SparkPubConnector() {
		super();
		logger.debug("CREATE SparkPubConnector: " + this);
	}

    protected SparkPubConnector(Properties properties, String... subjects) {
		super();
		this.properties = properties;
		this.subjects = transformIntoAList(subjects);
		logger.debug("CREATE SparkPubConnector {} with Properties '{}' and NATS Subjects '{}'.", this, properties, subjects);
	}

	/**
	 * @param properties
	 */
    protected SparkPubConnector(Properties properties) {
		super();
		this.properties = properties;
		logger.debug("CREATE SparkPubConnector {} with Properties '{}'.", this, properties);
	}

	/**
	 * @param subjects
	 */
    protected SparkPubConnector(String... subjects) {
		super();
		this.subjects = transformIntoAList(subjects);
		logger.debug("CREATE SparkPubConnector {} with NATS Subjects '{}'.", this, subjects);
	}

    protected Properties getProperties(){
    	if (properties == null) {
    		properties = new Properties(System.getProperties());
    	}
    	return properties;
    }

    protected Collection<String> getSubjects() throws Exception {
		if ((subjects ==  null) || (subjects.size() == 0)) {
			final String subjectsStr = getProperties().getProperty(NATS_SUBJECTS);
			if (subjectsStr == null) {
				throw new Exception("SparkPubConnector needs at least one NATS Subject.");
			}
			final String[] subjectsArray = subjectsStr.split(",");
			subjects = transformIntoAList(subjectsArray);
			logger.debug("Subject provided by the Properties: '{}'", subjects);
		}
		return subjects;
	}    		

	/**
	 * @param subjects
	 * @return
	 */
	protected List<String> transformIntoAList(String... subjects) {
		ArrayList<String> list = new ArrayList<String>(subjects.length);
		for (String subject: subjects){
			list.add(subject.trim());
		}
		return list;
	}
	
	protected ConnectionFactory getConnectionFactory() throws Exception {
		if (connectionFactory == null) {
			connectionFactory = new ConnectionFactory(getProperties());
		}		
		return connectionFactory;
	}

	// A dedicated lock object would not be serializable... So, KISS.
	protected synchronized Connection getConnection() throws Exception {
		if (connection == null) {
			connection = getConnectionFactory().createConnection();
			logger.debug("A NATS Connection {} has been created for {}", connection, this);
		}
		return connection;
	}
	
	VoidFunction<String> sendToNats = new VoidFunction<String>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void call(String str) throws Exception {
			Message natsMessage = new Message();
			
			byte[] payload = str.getBytes();
	        natsMessage.setData(payload, 0, payload.length);
	        
            final Connection localConnection = getConnection();
	        for (String subject : getSubjects()) {
	            natsMessage.setSubject(subject);
				localConnection.publish(natsMessage);

	            logger.trace("Send '{}' from Spark to NATS ({})", str, subject);
	        }
		}
	};
	
	public static VoidFunction<String> sendToNats(Properties properties, String... subjects) {
		return new SparkPubConnector(properties, subjects).sendToNats;
	}
	
	public static VoidFunction<String> sendToNats(Properties properties) {
		return new SparkPubConnector(properties).sendToNats;
	}
	
	public static VoidFunction<String> sendToNats(String... subjects) {
		return new SparkPubConnector(subjects).sendToNats;
	}

}
