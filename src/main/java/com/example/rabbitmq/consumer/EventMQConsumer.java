package com.example.rabbitmq.consumer;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.example.rabbitmq.Main;
import com.example.rabbitmq.util.FileIO;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class EventMQConsumer extends DefaultConsumer {
	private static final Logger logger = LogManager.getLogger(EventMQConsumer.class);
	private static int countEventConsumed = 0;
	private static final String FILE_EXTENSION = ".json";
	private String queueName;

	public EventMQConsumer(Channel channel, String queueName) {
		super(channel);
		this.queueName = queueName;
	}

	@Override
	public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
			throws IOException {
		String message = new String(body, "UTF-8");
		logger.debug("Message Consumed : " + message);
		String routingKey = envelope.getRoutingKey();
		long deliveryTag = envelope.getDeliveryTag();
		logger.debug("RoutingKey : " + routingKey);
		logger.debug("deliveryTag : " + deliveryTag);

		// processNotification(message);
		countEventConsumed++;
		logger.info("Number of event consumed : " + countEventConsumed);
		this.getChannel().basicAck(deliveryTag, false);
	}

	private void processNotification(String message) {
		// will dump notification event from here
		String filePath = Main.dumpDataDir + File.separator + this.queueName + "-" + LocalDate.now() + FILE_EXTENSION;
		logger.debug("FilePath : " + new File(filePath).getAbsolutePath());
		FileIO.writeAndAppendInFile(message, filePath);
	}

}
