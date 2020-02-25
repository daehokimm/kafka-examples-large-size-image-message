package me.daehokimm;

import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class ChoppedImageDeserializer implements Deserializer<ChoppedImage> {
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public ChoppedImage deserialize(String topic, byte[] data) {
		if (data == null)
			return null;

		ByteBuffer buffer = ByteBuffer.wrap(data);
		int imageNameSize = buffer.getInt();
		byte[] imageNameByte = new byte[imageNameSize];
		buffer.get(imageNameByte);
		String imageName = new String(imageNameByte);

		long ts = buffer.getLong();
		int totalParts = buffer.getInt();
		int partNum = buffer.getInt();

		int byteSize = buffer.getInt();
		byte[] bytes = new byte[byteSize];
		buffer.get(bytes);

		return new ChoppedImage(imageName, ts, totalParts, partNum, bytes);
	}

	@Override
	public void close() {

	}
}
