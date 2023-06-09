package com.example.rabbitmq.util;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class FileIO {

	private static final Logger logger = LogManager.getLogger(FileIO.class);

	private static final ObjectMapper mapper = new ObjectMapper();

	private static File getFileObject(String filePath) {
		File file = new File(filePath);
		if (!file.exists()) {
			try {
				file.createNewFile();
				return file;
			} catch (IOException ex) {
				logger.error("got exception while creating dump file", ex);
			}
		}
		return file;
	}

	private static JsonNode deserilizeMessage(String message) {
		JsonNode node = null;
		try {
			node = mapper.readTree(message);
			return node;
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return node;
	}

	private static ArrayNode deserilizeMessageFile(File file) {
		ArrayNode node = null;
		if (file.exists() && file.length() > 0) {
			try {
				node = (ArrayNode) mapper.readTree(file);
			} catch (IOException e) {
				logger.error("Failed to deserilize message file");
			}
		} else {
			node = mapper.createArrayNode();
		}

		return node;
	}

	public static void writeAndAppendInFile(String message, String filePath) {
		Objects.requireNonNull(message);
		Objects.requireNonNull(filePath);
		File file = getFileObject(filePath);
		Objects.requireNonNull(file, "file object not found");
		if (!file.exists()) {
			logger.error("Failed to create dump file");
			return;
		}

		JsonNode node = deserilizeMessage(message);
		ArrayNode arrayNode = deserilizeMessageFile(file);

		Objects.requireNonNull(node, "message deserialized as null");
		Objects.requireNonNull(arrayNode, "json file deserilized as null");

		arrayNode.add(node);

		try {
			mapper.writeValue(file, arrayNode);
			logger.debug(" Data Dumped --------");
		} catch (IOException e) {
			logger.error("Failed to write final message output on file");
		}
	}
}
