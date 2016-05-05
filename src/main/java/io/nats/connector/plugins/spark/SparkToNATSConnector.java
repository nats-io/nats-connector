/**
 * 
 */
package io.nats.connector.plugins.spark;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.connector.plugin.NATSConnector;

/**
 * @author laugimethods
 *
 */
public class SparkToNATSConnector implements NATSConnector {
	
    private Properties          properties = null;
    private Logger              logger     = LoggerFactory.getLogger(SparkToNATSConnector.class);
    String              		configFile = null;

    // TODO eval - this for performance.  Is it necessary?
    private AtomicBoolean     isRunning   = new AtomicBoolean();

    private ConnectionFactory connectionFactory = null;
    private Connection        connection        = null;

    public SparkToNATSConnector() throws Exception
    {
//        this.plugin = plugin;
        this.properties = getProperties();
        setup();
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

    private void setup() throws Exception
    {
        connectionFactory = new ConnectionFactory(properties);
/*        EventHandlers eh = new EventHandlers();

        connectionFactory.setClosedCallback(eh);
        connectionFactory.setDisconnectedCallback(eh);
        connectionFactory.setExceptionHandler(eh);
        connectionFactory.setReconnectedCallback(eh);*/

        // invoke on startup here, so the user can override or set their
        // own callbacks in the plugin if need be.
/*        if (invokeOnStartup(connectionFactory) == false) {
            shutdown();
            throw new Exception("Startup failure initiated From plug-in");
        }*/

        connection = connectionFactory.createConnection();
        logger.debug("Connected to NATS cluster.");
    }
    
	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnector#shutdown()
	 */
	@Override
	public void shutdown() {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnector#publish(io.nats.client.Message)
	 */
	@Override
	public void publish(Message message) {
        if (isRunning.get() == false)
            return;

        try {
            connection.publish(message);
        }
        catch (Exception ex) {
//            logger.error("Exception publishing: " + ex.getMessage());
//            logger.debug("Exception: " + ex);
        }
	}

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnector#flush()
	 */
	@Override
	public void flush() throws Exception {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnector#subscribe(java.lang.String)
	 */
	@Override
	public void subscribe(String subject) throws Exception {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnector#subscribe(java.lang.String, io.nats.client.MessageHandler)
	 */
	@Override
	public void subscribe(String subject, MessageHandler handler) throws Exception {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnector#subscribe(java.lang.String, java.lang.String)
	 */
	@Override
	public void subscribe(String subject, String queue) throws Exception {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnector#subscribe(java.lang.String, java.lang.String, io.nats.client.MessageHandler)
	 */
	@Override
	public void subscribe(String subject, String queue, MessageHandler handler) throws Exception {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnector#unsubscribe(java.lang.String)
	 */
	@Override
	public void unsubscribe(String subject) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnector#getConnection()
	 */
	@Override
	public Connection getConnection() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see io.nats.connector.plugin.NATSConnector#getConnectionFactory()
	 */
	@Override
	public ConnectionFactory getConnectionFactory() {
		// TODO Auto-generated method stub
		return null;
	}

}
