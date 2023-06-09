package com.example.rabbitmq;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.example.rabbitmq.consumer.EventMQConsumer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class MQReadChannel {
	private static final Logger logger = LogManager.getLogger(MQReadChannel.class);

	private static final int PREFETCH_COUNT = 1;
	private static final boolean DURABLE = true;
	private static final boolean EXCLUSIVE = false;
	private static final boolean AUTO_DELETE = false;
	private static final boolean AUTO_ACK = false;
	private static final Map<String, Integer> ARGS = Collections.singletonMap("x-message-ttl", Integer.valueOf(900000));
	
	private final String queueName;
	private final String routingKey;

	public MQReadChannel(String messageQueue) {
		// TODO: providing same routing key as queueName
		this.queueName = messageQueue;
		this.routingKey = messageQueue;
	}

	private void bindQueue(Channel channel, String exchangeName) throws IOException {
		/*
		 * TODO: declare queue here use channel.queueDeclare(parameters);
		 * channel.queueBind(this.queueName, exchangeName, this.routingKey);
		 */

		
		channel.basicQos(PREFETCH_COUNT);
	}

	public void openReadChannel(Connection connection, String exchangeName) throws IOException {
		final Channel channel = connection.createChannel();
		bindQueue(channel, exchangeName);

		// providing consumer here
		channel.basicConsume(this.queueName, true, new EventMQConsumer(channel, this.queueName));
		logger.info(" Ready to consumer messages -------");
	}

}
