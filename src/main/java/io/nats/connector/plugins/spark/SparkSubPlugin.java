/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.connector.plugins.spark;

import java.lang.ref.WeakReference;

import org.slf4j.Logger;

import io.nats.client.ConnectionFactory;
import io.nats.client.Message;
import io.nats.connector.plugin.NATSConnector;
import io.nats.connector.plugin.NATSConnectorPlugin;
import io.nats.connector.plugin.NATSEvent;

@Deprecated
public class SparkSubPlugin implements NATSConnectorPlugin {

    Logger logger = null;

	@Override
	public boolean onStartup(Logger logger, ConnectionFactory factory) {
        this.logger = logger;

        try {
			for (WeakReference<SparkConnector> sparkConnector: SparkConnector.getInstances()) {
				sparkConnector.get().setFactory(factory);
			}
        }
        catch (Exception e) {
            logger.error("Unable to initialize.", e);
            return false;
        }

        return true;
	}

	@Override
	public boolean onNatsInitialized(NATSConnector connector) {
		// This plugin doesn't need to continue
		return false;
	}

	@Override
	public void onNATSMessage(Message msg) {
		logger.trace("Received message: {}.", msg);
	}

	@Override
	public void onNATSEvent(NATSEvent event, String message) {
		logger.trace("Received event {} with message: {}.", event, message);
	}

	@Override
	public void onShutdown() {
	}
}
