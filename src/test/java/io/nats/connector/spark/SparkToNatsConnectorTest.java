/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.connector.spark;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkToNatsConnectorTest {

	private static final String DEFAULT_SUBJECT = "spark";
	protected static JavaSparkContext sc;
	static Logger logger = null;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Enable tracing for debugging as necessary.
		System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.spark.SparkToNatsConnector", "trace");
		System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.spark.SparkToNatsConnectorTest", "debug");
		System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.spark.TestClient", "debug");

		logger = LoggerFactory.getLogger(SparkToNatsConnectorTest.class);       

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
		assertTrue(LoggerFactory.getLogger(SparkToNatsConnector.class).isTraceEnabled());
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
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
			rdd.foreach(SparkToNatsConnector.publishToNats());
		} catch (Exception e) {
			if (e.getMessage().contains("SparkToNatsConnector needs at least one NATS Subject"))
				return;
			else
				throw e;
		}	

		fail("An Exception(\"SparkToNatsConnector needs at least one Subject\") should have been raised.");
	}

	@Test
	public void testStaticSparkToNatsWithMultipleSubjects() throws Exception {   
		final List<String> data = getData();

		String subject1 = "subject1";
		NatsSubscriber ns1 = getNatsSubscriber(data, subject1);

		String subject2 = "subject2";
		NatsSubscriber ns2 = getNatsSubscriber(data, subject2);

		JavaRDD<String> rdd = sc.parallelize(data);

		rdd.foreach(SparkToNatsConnector.publishToNats(DEFAULT_SUBJECT, subject1, subject2));		

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
		properties.setProperty(SparkToNatsConnector.NATS_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");
		rdd.foreach(SparkToNatsConnector.publishToNats(properties));		

		// wait for the subscribers to complete.
		ns1.waitForCompletion();
	}

	@Test
	public void testStaticSparkToNatsWithSystemProperties() throws Exception {   
		final List<String> data = getData();

		NatsSubscriber ns1 = getNatsSubscriber(data, DEFAULT_SUBJECT);

		JavaRDD<String> rdd = sc.parallelize(data);

		System.setProperty(SparkToNatsConnector.NATS_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");

		try {
			rdd.foreach(SparkToNatsConnector.publishToNats());
			// wait for the subscribers to complete.
			ns1.waitForCompletion();
		} finally {
			System.clearProperty(SparkToNatsConnector.NATS_SUBJECTS);
		}		
	}
}
