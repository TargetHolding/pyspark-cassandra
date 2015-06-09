/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package pyspark_cassandra.types;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import net.razorvine.pickle.Opcodes;
import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.PickleUtils;
import net.razorvine.pickle.custom.IObjectPickler;
import net.razorvine.pickle.custom.Pickler;

public class GatheringByteBufferPickler implements IObjectPickler {
	@Override
	public void pickle(Object o, OutputStream out, Pickler pickler) throws PickleException, IOException {
		GatheredByteBuffers buffers = ((GatheredByteBuffers) o);

		out.write(Opcodes.GLOBAL);
		out.write("__builtin__\nbytearray\n".getBytes());

		out.write(Opcodes.BINSTRING);

		int length = 0;
		for (ByteBuffer b : buffers) {
			length += b.remaining();
		}

		out.write(PickleUtils.integer_to_bytes(length));

		WritableByteChannel c = Channels.newChannel(out);
		for (ByteBuffer b : buffers) {
			c.write(b);
		}

		out.write(Opcodes.TUPLE1);
		out.write(Opcodes.REDUCE);
	}
}