package pyspark_cassandra.types;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

public class GatheredByteBuffers implements Iterable<ByteBuffer> {
	private List<ByteBuffer> buffers;

	public GatheredByteBuffers(List<ByteBuffer> buffers) {
		this.buffers = buffers;
	}

	public List<ByteBuffer> getBuffers() {
		return buffers;
	}

	@Override
	public Iterator<ByteBuffer> iterator() {
		return buffers.iterator();
	}
}
