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

    @Test
    public void testSparkToNats() throws Exception {   
    	
    	Connector c = new Connector();
        ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.execute(c);
        
        Thread.sleep(10000);
    	
    	JavaRDD<String> rdd = sc.parallelize(Arrays.asList(new String[] {
                "data_1",
                "data_2",
                "data_3",
            }));
    	
		rdd.foreach(sparkPlugin.onSparkInput);		
		rdd.foreachAsync(sparkPlugin.onSparkInput);
		
        Thread.sleep(1000);
        c.shutdown();
    }

	/**
	 * Test method for {@link io.nats.connector.plugins.spark.SparkPubSubPlugin#onStartup(org.slf4j.Logger, io.nats.client.ConnectionFactory)}.
	 */
	@Test
	public void testOnStartup() {
		fail("Not yet implemented");
	}

    @Test
    public void testNatsToSpark() throws Exception {
    }
	
	/**
	 * Test method for {@link io.nats.connector.plugins.spark.SparkPubSubPlugin#onNatsInitialized(io.nats.connector.plugin.NATSConnector)}.
	 */
	@Test
	public void testOnNatsInitialized() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link io.nats.connector.plugins.spark.SparkPubSubPlugin#onNATSMessage(io.nats.client.Message)}.
	 */
	@Test
	public void testOnNATSMessage() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link io.nats.connector.plugins.spark.SparkPubSubPlugin#onNATSEvent(io.nats.connector.plugin.NATSEvent, java.lang.String)}.
	 */
	@Test
	public void testOnNATSEvent() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link io.nats.connector.plugins.spark.SparkPubSubPlugin#onShutdown()}.
	 */
	@Test
	public void testOnShutdown() {
		fail("Not yet implemented");
	}

}
