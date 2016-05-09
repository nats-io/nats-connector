/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.connector.plugins.spark;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Subscription;

public class SparkEmitter extends Receiver<String> {

	private static final long serialVersionUID = 1L;
	/**
	 * 
	 */
	String host = null;
	int port = -1;	 
	String subject;
	String qgroup;
	String url;

	public SparkEmitter(String host_ , int port_, String _subject, String _qgroup) {
	    super(StorageLevel.MEMORY_AND_DISK_2());
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
 
	  
	public SparkEmitter() {
		 super(StorageLevel.MEMORY_AND_DISK_2());
	}

	public void onStart() {
	     //Start the thread that receives data over a connection
	   new Thread()  {
	     @Override public void run() {
	        try {
				receive();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
		        System.out.printf("[#%d] Received on [%s]: '%s'\n", count.incrementAndGet(),
		                         m.getSubject(), s);
		        
		        store(s);
	        });
	          
	        System.out.printf("Listening on [%s]\n", subject);
	        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
	        	System.err.println("\nCaught CTRL-C, shutting down gracefully...\n");
	        	try {
	        		sub.unsubscribe();
	        	} catch (IOException e) {
	        		System.out.println("Problem unsubscribing " + e.toString());
	        	}
	            nc.close();
	        }));
	        
	        while (true) {
	            // loop forever
	        }
	    } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			        				
	}
}
	
