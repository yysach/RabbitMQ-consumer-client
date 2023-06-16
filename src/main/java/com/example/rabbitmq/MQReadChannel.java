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
	private static final boolean AUTO_ACK = true;
	private static final Map<String, Object> ARGS = Collections.singletonMap("x-message-ttl", (Object)Integer.valueOf(900000));
	
	private final String queueName;
	private final String routingKey;
	private final boolean createQueue;

	public MQReadChannel(String messageQueue, boolean createQueue) {
		// TODO: providing same routing key as queueName
		this.queueName = messageQueue;
		this.routingKey = messageQueue;
		this.createQueue = createQueue;
	}

	private void bindQueue(Channel channel, String exchangeName) throws IOException {
		if (this.createQueue) {
			channel.queueDeclare(this.queueName, DURABLE, EXCLUSIVE, AUTO_DELETE, ARGS);
		}
		channel.queueBind(this.queueName, exchangeName, this.routingKey);
		channel.basicQos(PREFETCH_COUNT);
	}

	public void openReadChannel(Connection connection, String exchangeName) throws IOException {
		final Channel channel = connection.createChannel();
		bindQueue(channel, exchangeName);

		// providing consumer here
		channel.basicConsume(this.queueName, AUTO_ACK, new EventMQConsumer(channel, this.queueName));
		logger.info(" Ready to consumer messages -------");
	}

}
