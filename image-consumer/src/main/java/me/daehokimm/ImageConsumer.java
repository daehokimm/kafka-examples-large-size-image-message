package me.daehokimm;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.*;

public class ImageConsumer {

	private final static String TOPIC_NAME = "chucked-image";

	public static void main(String[] args) throws IOException {

		// broker configure
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "me.daehokimm.ImageChunkDeserializer");		// custom deserializer
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "chucked-image");

		Consumer<Long, ImageChunk> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singleton(TOPIC_NAME));
		Map<String, List<ImageChunk>> dictionary = new HashMap<>();		// for hold image segments to merge
		while (true) {		// infinite loop
			// subscribe topic
			consumer
					.poll(Duration.ofMillis(2000))
					.records(TOPIC_NAME)
					.forEach(record -> {
						String imageName = record.value().getImageName();
						if (!dictionary.containsKey(imageName))
							dictionary.put(imageName, new ArrayList<>());

						dictionary.get(imageName).add(record.value());
					});

			// merge images
			MergeImageSegments(dictionary);
		}
	}

	private static void MergeImageSegments(Map<String, List<ImageChunk>> dictionary) throws IOException {
		for(String name : dictionary.keySet()) {
			List<ImageChunk> imageChunks = dictionary.get(name);
			int totalPart = imageChunks.get(0).getTotalPart();
			if (totalPart != imageChunks.size())
				continue;

			// sort by partNum
			imageChunks.sort(Comparator.comparingInt(ImageChunk::getPartNum));
			int totalByteSize = imageChunks.stream().mapToInt(imageChunk -> imageChunk.getBytes().length).sum();
			byte[] bytes = new byte[totalByteSize];

			// merge bytes
			int offset = 0;
			for (ImageChunk imageChunk : imageChunks) {
				byte[] imageBytes = imageChunk.getBytes();
				System.arraycopy(imageBytes, 0, bytes, offset, imageBytes.length);
				offset += imageBytes.length;
			}

			// write file
			String imageName = imageChunks.get(0).getImageName();
			OutputStream out = new FileOutputStream(new File("images/" + imageName));
			out.write(bytes);
			out.close();
			System.out.println("== image [" + imageName + "] is wrote");
			System.out.println("* size : " + totalByteSize);

			dictionary.remove(name);
		}
	}
}
