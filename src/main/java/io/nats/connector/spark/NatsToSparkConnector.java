/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.connector.spark;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Subscription;

/**
 * A NATS to Spark Connector.
 * <p>
 * It will transfer messages received from NATS into Spark data.
 * <p>
 * That class extends {@link org.apache.spark.streaming.receiver.Receiver}&lt;String&gt;.
 * <p>
 * An usage of this class would look like this.
 * <pre>
 * JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));
 * final JavaReceiverInputDStream&lt;String&gt; messages = ssc.receiverStream(
 *    new NatsToSparkConnector(null, 0, "Subject", "Group", StorageLevel.MEMORY_ONLY()));
 * </pre>
 * @see <a href="http://spark.apache.org/docs/1.6.1/streaming-custom-receivers.html">Spark Streaming Custom Receivers</a>
 */
public class NatsToSparkConnector extends Receiver<String> {

	private static final long serialVersionUID = 6989127121049901119L;

	static final Logger logger = LoggerFactory.getLogger(NatsToSparkConnector.class);

	String host = null;
	int port = -1;	 
	String subject;
	String qgroup;
	String url;
			
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
			ConnectionFactory cf = new ConnectionFactory(url);
			final Connection nc = cf.createConnection();
			AtomicInteger count = new AtomicInteger();
			final Subscription sub = nc.subscribe(subject, qgroup, m -> {
				String s = new String(m.getData());
				if (logger.isTraceEnabled()) { 
					logger.trace("{} Received on {}: {}.", count.incrementAndGet(), m.getSubject(), s);
				}
				store(s);
			});

			logger.info("Listening on {}.", subject);
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
}

