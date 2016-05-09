/*******************************************************************************
 * Copyright (c) 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.connector.spark;

import io.nats.client.AsyncSubscription;
import io.nats.client.ConnectionFactory;
import io.nats.client.Message;
import io.nats.client.MessageHandler;

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
