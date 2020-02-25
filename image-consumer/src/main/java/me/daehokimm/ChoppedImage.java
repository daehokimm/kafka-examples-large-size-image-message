package me.daehokimm;

import java.util.Arrays;
import java.util.Objects;

public class ChoppedImage {
	private String imageName;
	private long timestamp;
	private int totalPart;
	private int partNum;
	private byte[] bytes;

	public ChoppedImage(String imageName, long timestamp, int totalPart, int partNum, byte[] bytes) {
		this.imageName = imageName;
		this.timestamp = timestamp;
		this.totalPart = totalPart;
		this.partNum = partNum;
		this.bytes = bytes;
	}

	public String getImageName() {
		return imageName;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public int getTotalPart() {
		return totalPart;
	}

	public int getPartNum() {
		return partNum;
	}

	public byte[] getBytes() {
		return bytes;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ChoppedImage that = (ChoppedImage) o;
		return timestamp == that.timestamp &&
				totalPart == that.totalPart &&
				partNum == that.partNum &&
				Objects.equals(imageName, that.imageName) &&
				Arrays.equals(bytes, that.bytes);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(imageName, timestamp, totalPart, partNum);
		result = 31 * result + Arrays.hashCode(bytes);
		return result;
	}

	@Override
	public String toString() {
		return "ChoppedImage{" +
				"imageName='" + imageName + '\'' +
				", timestamp=" + timestamp +
				", totalPart=" + totalPart +
				", partNum=" + partNum +
				", bytes=" + Arrays.toString(bytes) +
				'}';
	}
}
