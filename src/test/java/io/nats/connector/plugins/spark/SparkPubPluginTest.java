/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.connector.plugins.spark;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
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

public class SparkPubPluginTest {

	private static final String DEFAULT_SUBJECT = "spark";

	protected static JavaSparkContext sc;

    static Logger logger = null;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
        // Enable tracing for debugging as necessary.
        System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.plugins.spark.SparkPubConnector", "trace");
        System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.plugins.spark.SparkPubPluginTest", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.plugins.spark.TestClient", "debug");

        logger = LoggerFactory.getLogger(SparkPubConnector.class);       

        SparkConf sparkConf = new SparkConf().setAppName("My Spark Job").setMaster("local[2]");
		sc = new JavaSparkContext(sparkConf);

        UnitTestUtilities.startDefaultServer();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
        UnitTestUtilities.stopDefaultServer();
        sc.stop();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		assertTrue(logger.isDebugEnabled());
		assertTrue(LoggerFactory.getLogger(SparkPubConnector.class).isTraceEnabled());
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
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

            logger.debug("NATS Subscriber ({}):  Received message: {}", id, value);

            if (tallyMessage() == testCount)
            {
                logger.info("NATS Subscriber ({}) Received {} messages.  Completed.", id, testCount);
                setComplete();
            }
        }
    }

	/**
	 * @return
	 */
	protected List<String> getData() {
		final List<String> data = Arrays.asList(new String[] {
                "data_1",
                "data_2",
                "data_3",
                "data_4",
                "data_5",
                "data_6"
            });
		return data;
	}

	/**
	 * @param data
	 * @return
	 */
	protected NatsSubscriber getNatsSubscriber(final List<String> data, String subject) {
		ExecutorService executor = Executors.newFixedThreadPool(1);
        
        NatsSubscriber ns1 = new NatsSubscriber(subject + "_id", subject, data.size());
        
        // start the subscribers apps
        executor.execute(ns1);
       
        // wait for subscribers to be ready.
        ns1.waitUntilReady();
		return ns1;
	}

    @Test
    public void testStaticSparkToNatsNoSubjects() throws Exception {   
    	final List<String> data = getData();
                
		JavaRDD<String> rdd = sc.parallelize(data);
    	
    	try {
			rdd.foreach(SparkPubConnector.sendToNats());
		} catch (Exception e) {
			if (e.getMessage().contains("SparkPubConnector needs at least one NATS Subject"))
				return;
			else
				throw e;
		}	
    	
    	fail("An Exception(\"SparkPubConnector needs at least one Subject\") should have been raised.");
    }

    @Test
    public void testStaticSparkToNatsWithMultipleSubjects() throws Exception {   
    	final List<String> data = getData();
    	
    	String subject1 = "subject1";
        NatsSubscriber ns1 = getNatsSubscriber(data, subject1);
                
    	String subject2 = "subject2";
        NatsSubscriber ns2 = getNatsSubscriber(data, subject2);
                
		JavaRDD<String> rdd = sc.parallelize(data);
    	
    	rdd.foreach(SparkPubConnector.sendToNats(DEFAULT_SUBJECT, subject1, subject2));		
		
        // wait for the subscribers to complete.
        ns1.waitForCompletion();
        ns2.waitForCompletion();
    }

    @Test
    public void testStaticSparkToNatsWithProperties() throws Exception {   
    	final List<String> data = getData();
    	
        NatsSubscriber ns1 = getNatsSubscriber(data, DEFAULT_SUBJECT);
                
		JavaRDD<String> rdd = sc.parallelize(data);
    	
    	final Properties properties = new Properties();
    	properties.setProperty(SparkPubConnector.NATS_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");
		rdd.foreach(SparkPubConnector.sendToNats(properties));		
		
        // wait for the subscribers to complete.
        ns1.waitForCompletion();
    }

    @Test
    public void testStaticSparkToNatsWithSystemProperties() throws Exception {   
    	final List<String> data = getData();
    	
        NatsSubscriber ns1 = getNatsSubscriber(data, DEFAULT_SUBJECT);
                
		JavaRDD<String> rdd = sc.parallelize(data);
    	
    	System.setProperty(SparkPubConnector.NATS_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");
    	
    	try {
			rdd.foreach(SparkPubConnector.sendToNats());
			// wait for the subscribers to complete.
			ns1.waitForCompletion();
		} finally {
			System.clearProperty(SparkPubConnector.NATS_SUBJECTS);
		}		
    }
}
