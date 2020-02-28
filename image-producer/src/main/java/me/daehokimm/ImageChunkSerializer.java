package me.daehokimm;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class ImageChunkSerializer implements Serializer<ImageChunk> {
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public byte[] serialize(String topic, ImageChunk data) {
		if (data == null)
			return null;

		byte[] imageNameBytes = data.getImageName().getBytes();
		ByteBuffer buffer = ByteBuffer.allocate(4 + imageNameBytes.length + 8 + 4 + 4 + 4 + data.getBytes().length);
		buffer.putInt(imageNameBytes.length);
		buffer.put(imageNameBytes);
		buffer.putLong(data.getTimestamp());
		buffer.putInt(data.getTotalPart());
		buffer.putInt(data.getPartNum());
		buffer.putInt(data.getBytes().length);
		buffer.put(data.getBytes());

		return buffer.array();
	}

	@Override
	public void close() {
	}
}
