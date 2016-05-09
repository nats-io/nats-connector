/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.connector.spark;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Subscription;

public class NatsToSparkConnector extends Receiver<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String host = null;
	int port = -1;	 
	String subject;
	String qgroup;
	String url;

	static final Logger logger = LoggerFactory.getLogger(NatsToSparkConnector.class);
			
	public NatsToSparkConnector(String host_ , int port_, String _subject, String _qgroup, StorageLevel _storageLevel) {
		super(_storageLevel);
		host = host_;
		port = port_;
		subject = _subject;
		qgroup = _qgroup;
		url = ConnectionFactory.DEFAULT_URL;
		if (host != null){
			url = url.replace("localhost", host);
		}
		if (port > 0){
			String strPort = Integer.toString(port);
			url = url.replace("4222", strPort);
		}
	}


	public NatsToSparkConnector(StorageLevel _storageLevel) {
		super(_storageLevel);
	}

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

	public void onStop() {
		// There is nothing much to do as the thread calling receive()
		// is designed to stop by itself if isStopped() returns false
	}

	/** Create a socket connection and receive data until receiver is stopped */
	private void receive() throws Exception{

		// Make connection and initialize streams			  
		try {
			ConnectionFactory cf = new ConnectionFactory(url);
			final Connection nc = cf.createConnection();
			AtomicInteger count = new AtomicInteger();
			final Subscription sub = nc.subscribe(subject, qgroup, m -> {
				String s = new String(m.getData());
				logger.trace("{} Received on {}: {}.", count.incrementAndGet(), m.getSubject(), s);
				store(s);
			});

			logger.info("Listening on {}.", subject);
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				logger.error("Caught CTRL-C, shutting down gracefully...");
				try {
					sub.unsubscribe();
				} catch (IOException e) {
					logger.error("Problem while unsubscribing " + e.toString());
				}
				nc.close();
			}));

			while (true) {
				// loop forever
			}
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage());
		}			        				
	}
}

