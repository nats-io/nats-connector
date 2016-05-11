/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.connector.spark;

import static org.junit.Assert.*;

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
import io.nats.connector.spark.NatsToSparkConnector;

public class NatsToSparkConnectorTest {

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
		// Enable tracing for debugging as necessary.
		System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.spark.NatsToSparkConnector", "trace");
		System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.spark.NatsToSparkConnectorTest", "debug");
		System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.spark.TestClient", "debug");
		System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.spark.NatsPublisher", "debug");

		logger = LoggerFactory.getLogger(NatsToSparkConnectorTest.class);       

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
		assertTrue(LoggerFactory.getLogger(NatsToSparkConnector.class).isTraceEnabled());
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link io.nats.connector.spark.NatsToSparkConnector#NatsToSparkConnector(java.lang.String, int, java.lang.String, java.lang.String)}.
	 * @throws InterruptedException 
	 */
	@Test
	public void testNatsToSparkConnector() throws InterruptedException {
		final int nbOfMessages = 5;
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final JavaReceiverInputDStream<String> messages = ssc.receiverStream(
				new NatsToSparkConnector(null, 0, "Subject", "Group", StorageLevel.MEMORY_ONLY()));

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
