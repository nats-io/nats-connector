/**
 * 
 */
package io.nats.connector.plugins.spark;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.AsyncSubscription;
import io.nats.client.ConnectionFactory;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.connector.Connector;

/**
 * @author laugimethods
 *
 */
public class SparkPubSubPluginTest {

	protected static JavaSparkContext sc;
	protected static SparkPubSubPlugin sparkPlugin;

    Logger logger = null;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("My Spark Job").setMaster("local[2]");
		sc = new JavaSparkContext(sparkConf);

        UnitTestUtilities.startDefaultServer();
        Thread.sleep(500);

		sparkPlugin = new SparkPubSubPlugin();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
        UnitTestUtilities.stopDefaultServer();
        Thread.sleep(500);

        sc.stop();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
        System.setProperty(Connector.PLUGIN_CLASS, SparkPubSubPlugin.class.getName());

        // Enable tracing for debugging as necessary.
        //System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.plugins.redis.RedisPubSubPlugin", "trace");
        //System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.plugins.redis.RedisPubSubPluginTest", "trace");
        //System.setProperty("org.slf4j.simpleLogger.log.io.nats.client", "trace");

        logger = LoggerFactory.getLogger(SparkPubSubPlugin.class);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}
    abstract class TestClient
    {
        Object readyLock = new Object();
        boolean isReady = false;

        String id = "";

        Object completeLock = new Object();
        boolean isComplete = false;

        protected int testCount = 0;

        int msgCount = 0;

        int tallyMessage()
        {
            return (++msgCount);
        }

        int getMessageCount()
        {
            return msgCount;
        }

        TestClient(String id, int testCount)
        {
            this.id = id;
            this.testCount = testCount;
        }

        void setReady()
        {
            logger.debug("Client ({}) is ready.", id);
            synchronized (readyLock)
            {
                if (isReady)
                    return;

                isReady = true;
                readyLock.notifyAll();
            }
        }

        void waitUntilReady()
        {
            synchronized (readyLock)
            {
                while (!isReady) {
                    try {
                        readyLock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            logger.debug("Done waiting for Client ({}) to be ready.", id);
        }

        void setComplete()
        {
            logger.debug("Client ({}) has completed.", id);

            synchronized(completeLock)
            {
                if (isComplete)
                    return;

                isComplete = true;
                completeLock.notifyAll();
            }
        }

        void waitForCompletion()
        {
            synchronized (completeLock)
            {
                while (!isComplete)
                {
                    try {
                        completeLock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            logger.debug("Done waiting for Client ({}) to complete.", id);
        }
    }

    /**
     * Simulates a simple NATS subscriber.
     */
    class NatsSubscriber extends TestClient implements Runnable, MessageHandler
    {
        String subject = null;
        boolean checkPayload = true;

        NatsSubscriber(String id, String subject, int count)
        {
            super(id, count);
            this.subject = subject;

            logger.info("Creating NATS Subscriber ({})", id);
        }

        @Override
        public void run() {

            try {
                logger.info("NATS Subscriber ({}):  Subscribing to subject: {}", id, subject); //trace

                io.nats.client.Connection c = new ConnectionFactory().createConnection();

                AsyncSubscription s = c.subscribeAsync(subject, this);
                s.start();

                setReady();

                logger.info("NATS Subscriber ({}):  Subscribing to subject: {}", id, subject); // debug

                waitForCompletion();

                s.unsubscribe();

                logger.info("NATS Subscriber ({}):  Exiting.", id); // debug
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
            }
        }

        @Override
        public void onMessage(Message message) {

            String value = new String (message.getData());

            logger.info("NATS Subscriber ({}):  Received message: {}", id, value);

/*            if (checkPayload) {
                org.junit.Assert.assertTrue(REDIS_PAYLOAD.equals(value));
            }*/

            if (tallyMessage() == testCount)
            {
                logger.info("NATS Subscriber ({}) Received {} messages.  Completed.", id, testCount);
                setComplete();
            }
        }
    }

    @Test
    public void testSparkToNats() throws Exception {   
    	
//    	Connector c = new Connector();
        ExecutorService executor = Executors.newFixedThreadPool(1);
        
        NatsSubscriber ns1 = new NatsSubscriber("ns1", SparkPubSubPlugin.subject, 3);
        
        // start the subsciber apps
        executor.execute(ns1);
        
//        executor.execute(c);
       
        // wait for subscribers to be ready.
        ns1.waitUntilReady();

        // let the connector start
        Thread.sleep(1000);
        
    	JavaRDD<String> rdd = sc.parallelize(Arrays.asList(new String[] {
                "data_1",
                "data_2",
                "data_3",
            }));
    	
		rdd.foreach(sparkPlugin.onSparkInput);		
		rdd.foreachAsync(sparkPlugin.onSparkInput);
		
        // wait for the subscribers to complete.
        ns1.waitForCompletion();
		
        Thread.sleep(1000);
//        c.shutdown();
    }

	/**
	 * Test method for {@link io.nats.connector.plugins.spark.SparkPubSubPlugin#onStartup(org.slf4j.Logger, io.nats.client.ConnectionFactory)}.
	 */
	@Test
	public void testOnStartup() {
//		fail("Not yet implemented");
	}

    @Test
    public void testNatsToSpark() throws Exception {
    }
	
	/**
	 * Test method for {@link io.nats.connector.plugins.spark.SparkPubSubPlugin#onNatsInitialized(io.nats.connector.plugin.NATSConnector)}.
	 */
	@Test
	public void testOnNatsInitialized() {
//		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link io.nats.connector.plugins.spark.SparkPubSubPlugin#onNATSMessage(io.nats.client.Message)}.
	 */
	@Test
	public void testOnNATSMessage() {
//		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link io.nats.connector.plugins.spark.SparkPubSubPlugin#onNATSEvent(io.nats.connector.plugin.NATSEvent, java.lang.String)}.
	 */
	@Test
	public void testOnNATSEvent() {
//		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link io.nats.connector.plugins.spark.SparkPubSubPlugin#onShutdown()}.
	 */
	@Test
	public void testOnShutdown() {
//		fail("Not yet implemented");
	}

}
