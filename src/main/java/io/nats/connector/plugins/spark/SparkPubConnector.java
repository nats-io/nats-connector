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

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Message;

/**
 * @author laugimethods
 *
 */
public class SparkPubConnector implements Serializable {

	public static final String DEFAULT_SUBJECT = "spark";

    protected ConnectionFactory connectionFactory = null;
    protected Connection        connection        = null;
    protected Properties		properties		  = null;
    protected Collection<String>		subjects;

    Logger logger = null;
    
    /**
	 * 
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

	boolean trace = false;

    public void setProperties(Properties properties) {
		this.properties = properties;
	}

	private Properties getProperties(){
    	if (properties == null) {
    		properties = new Properties(System.getProperties());
    	}

    	return properties;
    }

	private Collection<String> getSubjects() {
		if (subjects ==  null) {
			String subjectsStr = getProperties().getProperty("subjects", DEFAULT_SUBJECT);
			String[] subjectsArray = subjectsStr.split(",");
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
			System.out.println(properties + " :::::::: " + str);
//			List<String> l = getSubjectsFromChannel(channelOrPattern);
	        
			Message natsMessage = new Message();
			
			byte[] payload = str.getBytes();
	        natsMessage.setData(payload, 0, payload.length);
	        
	        for (String s : getSubjects()) {
	            natsMessage.setSubject(s);
	            // new Connector().publish(natsMessage);
	            getConnection().publish(natsMessage);

//	            logger.trace("Send Redis ({}) -> NATS ({})", channelOrPattern, s);
	        }
		}
	};

}
