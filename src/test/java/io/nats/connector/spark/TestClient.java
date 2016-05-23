/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.connector.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TestClient {
	static Logger logger = LoggerFactory.getLogger(TestClient.class);

	Object readyLock = new Object();
	boolean isReady = false;

	String id = "";

	Object completeLock = new Object();
	boolean isComplete = false;

	protected int testCount = 0;

	int msgCount = 0;

	int tallyMessage()
	{
		return (++msgCount);
	}

	int getMessageCount()
	{
		return msgCount;
	}

	TestClient(String id, int testCount)
	{
		this.id = id;
		this.testCount = testCount;
	}

	void setReady()
	{
		logger.debug("Client ({}) is ready.", id);
		synchronized (readyLock)
		{
			if (isReady)
				return;

			isReady = true;
			readyLock.notifyAll();
		}
	}

	void waitUntilReady()
	{
		synchronized (readyLock)
		{
			while (!isReady) {
				try {
					readyLock.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		logger.debug("Done waiting for Client ({}) to be ready.", id);
	}

	void setComplete()
	{
		logger.debug("Client ({}) has completed.", id);

		synchronized(completeLock)
		{
			if (isComplete)
				return;

			isComplete = true;
			completeLock.notifyAll();
		}
	}

	void waitForCompletion()
	{
		synchronized (completeLock)
		{
			while (!isComplete)
			{
				try {
					completeLock.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		logger.debug("Done waiting for Client ({}) to complete.", id);
	}

}
