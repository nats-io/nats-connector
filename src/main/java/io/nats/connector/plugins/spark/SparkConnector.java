/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.connector.plugins.spark;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Subscription;

public class SparkConnector extends Receiver<String> {
	
	private static Set<WeakReference<SparkConnector>> weakInstancesSet = Collections.newSetFromMap(new WeakHashMap<WeakReference<SparkConnector>, Boolean>());
	
	protected SerializableConnectionFactory factory;
	protected String subjectFrom;
	
	static final Logger logger = LoggerFactory.getLogger(SparkConnector.class);
	
	protected SparkConnector(StorageLevel storageLevel) {
		super(storageLevel);		
	}

	public static SparkConnector createSparkConnector(StorageLevel storageLevel, String subjectFrom) {
		SparkConnector instance = new SparkConnector(storageLevel, subjectFrom);
		weakInstancesSet.add(new WeakReference<SparkConnector>(instance));
		return instance;
	}
	
	protected SparkConnector(StorageLevel storageLevel, String subjectFrom) {
		super(storageLevel);
		this.subjectFrom = subjectFrom;
	}

	public static Set<WeakReference<SparkConnector>> getInstances() {
	    return weakInstancesSet;
	}
	
	@Override
	public void onStart() {
		//Start the thread that receives data over a connection
		new Thread()  {
			@Override public void run() {
				try {
					receive();
				} catch (Exception e) {
					logger.error("Cannot start the connector: ", e);
				}
			}
		}.start();
	}

	@Override
	public void onStop() {
		// There is nothing much to do as the thread calling receive()
		// is designed to stop by itself if CTRL-C is Caught.
	}
	
	/** Create a socket connection and receive data until receiver is stopped **/
	protected void receive() {

		try {
			// Make connection and initialize streams			  
			final Connection nc = factory.createConnection();
			final Subscription sub = nc.subscribe(subjectFrom, m -> {
				String s = new String(m.getData());
				if (logger.isTraceEnabled()) { 
					logger.trace("Received on {}: {}.", m.getSubject(), s);
				}
				store(s);
			});

			logger.info("Listening on {}.", subjectFrom);
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				logger.info("Caught CTRL-C, shutting down gracefully...");
				try {
					sub.unsubscribe();
				} catch (IOException e) {
					logger.error("Problem while unsubscribing " + e.toString());
				}
				nc.close();
			}));
		} catch (IOException | TimeoutException e) {
			logger.error(e.getLocalizedMessage());
		}
	}

	/**
	 * @return the factory
	 */
	public SerializableConnectionFactory getFactory() {
		return factory;
	}

	/**
	 * @param factory the factory to set
	 */
	public void setFactory(SerializableConnectionFactory factory) {
		this.factory = factory;
	}

	/**
	 * @param factory the factory to set
	 */
	public void setFactory(ConnectionFactory factory) {
		this.factory = new SerializableConnectionFactory(factory);
	}

	/**
	 * @return the subject
	 */
	public String getSubject() {
		return subjectFrom;
	}

	/**
	 * @param subject the subject to set
	 */
	public void setSubject(String subject) {
		this.subjectFrom = subject;
	}

}
