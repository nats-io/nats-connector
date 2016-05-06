/**
 * 
 */
package io.nats.connector.plugins.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
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

	public static final String DEFAULT_SUBJECT = "spark";

    protected ConnectionFactory 	connectionFactory = null;
    protected Connection        	connection        = null;
    protected Properties			properties		  = null;
    protected Collection<String>	subjects;

    static final Logger logger = LoggerFactory.getLogger(SparkPubConnector.class);
    
    /**
	 * @param properties
	 * @param subjects
	 */
	public SparkPubConnector() {
		super();
		System.out.println("CREATE SparkPubConnector " + this);
	}

    public SparkPubConnector(Properties properties, String... subjects) {
		super();
		this.properties = properties;
		this.subjects = Arrays.asList(subjects);
	}

	/**
	 * @param properties
	 */
	public SparkPubConnector(Properties properties) {
		super();
		this.properties = properties;
	}

	/**
	 * @param subjects
	 */
	public SparkPubConnector(String... subjects) {
		super();
		this.subjects = Arrays.asList(subjects);
	}

	private Properties getProperties(){
    	if (properties == null) {
    		properties = new Properties(System.getProperties());
    	}

    	return properties;
    }

	private Collection<String> getSubjects() {
		if (subjects ==  null) {
			final String subjectsStr = getProperties().getProperty("subjects", DEFAULT_SUBJECT);
			final String[] subjectsArray = subjectsStr.split(",");
			subjects = Arrays.asList(subjectsArray);
		}
		return subjects;
	}    		
	
	protected ConnectionFactory getConnectionFactory() throws Exception {
		if (connectionFactory == null) {
			connectionFactory = new ConnectionFactory(getProperties());
		}
		
		return connectionFactory;
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
			Message natsMessage = new Message();
			
			byte[] payload = str.getBytes();
	        natsMessage.setData(payload, 0, payload.length);
	        
	        for (String subject : getSubjects()) {
	            natsMessage.setSubject(subject);
	            getConnection().publish(natsMessage);

	            logger.info("Send Spark ({}) -> NATS ({})", str, subject);
	        }
		}
	};

}
