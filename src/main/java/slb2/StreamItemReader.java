package slb2;

import slb2.reader.ItemReader;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * Reads a stream of StreamItems from a file, e.g., wikipedia dataset.
 */
public class StreamItemReader implements ItemReader {
	private BufferedReader in;

	public StreamItemReader(BufferedReader input) {
		this.in = input;
	}

	@Override
	public String[] nextItem() {
		String line = null;
		try {
			line = in.readLine();
		} catch (IOException e) {
			System.err.println("Unable to read from file");
			e.printStackTrace();
		}

		if (line == null || line.length() == 0) {
			return null;
		}

        String[] fields = line.split(" ");
        return fields;
	}

	public void close() throws IOException{
		try {
			in.close();
		}catch (IOException e) {
			throw e;
		}
	}
}
