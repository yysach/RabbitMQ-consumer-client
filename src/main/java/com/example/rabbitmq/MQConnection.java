package com.example.rabbitmq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class MQConnection {
	private static final Logger logger = LogManager.getLogger(MQConnection.class);

	private String hostName;
	private String messageQueue;
	private Integer port;

	public MQConnection(String hostName, Integer port, String messageQueue) {
		this.hostName = hostName;
		this.port = port;
		this.messageQueue = messageQueue;
	}

	public void connectToMQBroker() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(this.hostName);
		factory.setPort(this.port);
		/*
		 * TODO: rest of the connection parameter need to initialize here
		 * 
		 * setHost setPort setConnectionTimeout setVirtualhost SslProtocol
		 * setAutomcaticRecoverEnabled setNetworkRecoveryInterval username and passowrd
		 * 
		 * factory.newConnection() createChannel
		 * 
		 * openReadChannel(connection, exchangeName, consumer);
		 */
		Connection connection = factory.newConnection();
		logger.info(" Connected to MQBroker --------");

		// giving empty string for default exchange
		new MQReadChannel(this.messageQueue).openReadChannel(connection, "");
	}

}
