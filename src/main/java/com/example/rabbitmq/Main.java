package com.example.rabbitmq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {
	private static final Logger logger = LogManager.getLogger(Main.class);

	private static String hostName = "localhost";
	private static Integer port = 5672;
	private static String messageQueue = "temp-queue";

	public static String dumpDataDir = ".";
	private static MQConnection mqConnection;

	public static void setMQConnection(String hostName, Integer port, String messageQueue)
			throws IOException, TimeoutException {
		mqConnection = new MQConnection(hostName, port, messageQueue);
		mqConnection.connectToMQBroker();
	}

	public static void main(String args[]) throws IOException, TimeoutException {

		Options options = new Options();
		options.addOption("h", "host", true, "The hostName for rabbitmq server. Default:localhost");
		options.addOption("p", "port", true, "The port for the rabbitmq server to listen on. Default:5672");
		options.addOption("d", "dumpdataDir", true, "The directory to dump data to. Default: currentDirectory");
		options.addOption("q", "messageQueue", true, "The messageQueue name to read data from. Default:temp-queue");
		options.addOption("help", "help", false, "shows the help guide for the jar");

		HelpFormatter formatter = new HelpFormatter();
		CommandLineParser parser = new DefaultParser();
		CommandLine commandLine = null;

		try {
			commandLine = parser.parse(options, args);
		} catch (Exception ex) {
			logger.error("got cmdline parser exception", ex);
			System.exit(0);
		}

		if (commandLine.hasOption("help")) {
			formatter.printHelp("java -jar rabbit-consumer-project-0.0.1-SNAPSHOT.jar", options);
			System.exit(0);
		}

		if (commandLine.hasOption("h")) {
			hostName = commandLine.getOptionValue("host");
		}

		if (commandLine.hasOption("p")) {
			port = Integer.parseInt(commandLine.getOptionValue("port"));
		}

		if (commandLine.hasOption("d")) {
			dumpDataDir = commandLine.getOptionValue("dumpdataDir");
		}

		if (commandLine.hasOption("q")) {
			messageQueue = commandLine.getOptionValue("messageQueue");
		}

		printArgumentsOption();
		setMQConnection(hostName, port, messageQueue);
	}

	private static void printArgumentsOption() {
		System.out.println("------------------------------------------------");
		System.out.println("HostName : " + hostName);
		System.out.println("Port : " + port);
		System.out.println("DumpDataDir : " + dumpDataDir);
		System.out.println("MessageQueue Name : " + messageQueue);
		System.out.println("------------------------------------------------");
	}
}