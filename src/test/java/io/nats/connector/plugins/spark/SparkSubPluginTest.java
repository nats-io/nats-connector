/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.connector.plugins.spark;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.connector.Connector;
import io.nats.connector.spark.NatsPublisher;
import io.nats.connector.spark.UnitTestUtilities;

@Deprecated
@Ignore
public class SparkSubPluginTest {

	protected static JavaSparkContext sc;
	static Logger logger = null;
	static Boolean rightNumber = true;
	static Boolean atLeastSomeData = false;
	static String payload = null;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {		
		System.setProperty(Connector.PLUGIN_CLASS, SparkSubPlugin.class.getName());

		// Enable tracing for debugging as necessary.
//		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
		System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.Connector", "debug");
		System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.plugins.spark.SparkConnector", "trace");
		System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.plugins.spark.SparkSubPlugin", "trace");
		System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.plugins.spark.SerializableConnectionFactory", "debug");
		System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.plugins.spark.SparkSubPluginTest", "debug");
		System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.spark.NatsPublisher", "debug");

		logger = LoggerFactory.getLogger(SparkSubPluginTest.class);       

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
		assertTrue(LoggerFactory.getLogger(SparkConnector.class).isTraceEnabled());
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link io.nats.connector.spark.NatsToSparkConnector#NatsToSparkConnector(java.lang.String, int, java.lang.String, java.lang.String)}.
	 * @throws Exception 
	 */
	@Test
	public void testNatsToSparkPlugin() throws Exception {
		final int nbOfMessages = 5;
		
		Connector c = new Connector();
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final JavaReceiverInputDStream<String> messages = ssc.receiverStream(
				SparkConnector.createSparkConnector(StorageLevel.MEMORY_ONLY(), "Subject"));

		ExecutorService executor = Executors.newFixedThreadPool(6);

		NatsPublisher np = new NatsPublisher("np", "Subject",  nbOfMessages);
		
		messages.print();
		
		messages.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<String> rdd) throws Exception {
				logger.debug("RDD received: {}", rdd.collect());
				
				final long count = rdd.count();
				if ((count != 0) && (count != nbOfMessages)) {
					rightNumber = false;
				}
				
				atLeastSomeData = atLeastSomeData || (count > 0);
				
				for (String str :rdd.collect()) {
					if (! NatsPublisher.NATS_PAYLOAD.equals(str)) {
							payload = str;
						}
				}
			}			
		});

        // start the connector
        executor.execute(c);

		ssc.start();
		
		Thread.sleep(500);
		
		// start the publisher
		executor.execute(np);

		Thread.sleep(500);

		ssc.close();
		
		assertTrue("Not a single RDD did received messages.", atLeastSomeData);
		
		assertNull("'" + payload + " should be '" + NatsPublisher.NATS_PAYLOAD + "'", payload);
	}

}
