/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.connector.spark;

import io.nats.client.ConnectionFactory;

/**
 * Simulates a simple NATS publisher.
 */
public class NatsPublisher extends TestClient implements Runnable
{
	public static final String NATS_PAYLOAD = "Hello from NATS!";

	String subject = null;

	public NatsPublisher(String id, String subject, int count)
	{
		super(id, count);
		this.subject = subject;

		logger.debug("Creating NATS Publisher ({})", id);
	}

	@Override
	public void run() {

		try {

			logger.debug("NATS Publisher ({}):  Starting", id);

			io.nats.client.Connection c = new ConnectionFactory().createConnection();

			for (int i = 0; i < testCount; i++) {
				c.publish(subject, NATS_PAYLOAD.getBytes());
				tallyMessage();
			}
			c.flush();

			logger.debug("NATS Publisher ({}):  Published {} messages.", id, testCount);

			setComplete();
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
}
