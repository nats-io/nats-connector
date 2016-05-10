/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.connector.plugins.spark;

import org.slf4j.Logger;

import io.nats.client.ConnectionFactory;
import io.nats.client.Message;
import io.nats.connector.plugin.NATSConnector;
import io.nats.connector.plugin.NATSConnectorPlugin;
import io.nats.connector.plugin.NATSEvent;

public class SparkSubPlugin implements NATSConnectorPlugin {

	@Override
	public boolean onStartup(Logger logger, ConnectionFactory factory) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean onNatsInitialized(NATSConnector connector) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void onNATSMessage(Message msg) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onNATSEvent(NATSEvent event, String message) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onShutdown() {
		// TODO Auto-generated method stub
		
	}

}
