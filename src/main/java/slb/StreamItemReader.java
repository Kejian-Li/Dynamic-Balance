package slb;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Reads a stream of StreamItems from a file.
 */
public class StreamItemReader {
	private BufferedReader in;

	public StreamItemReader(BufferedReader input) {
		this.in = input;
	}

	public StreamItem nextItem() throws IOException {
		String line = null;
		try {
			line = in.readLine();
		} catch (IOException e) {
			System.err.println("Unable to read from file");
			throw e;
		}

		if (line == null || line.length() == 0)
			return null;

//		String[] tokens = line.split("\t");
//		if (tokens.length < 2)
//			return null;
//
//		long timestamp = Long.parseLong(tokens[0]);
//		String[] words = tokens[1].split(" ");
//		return new StreamItem(timestamp, Arrays.asList(words));
        String[] fields = line.split(" ");
        long timestamp = (long) Float.parseFloat(fields[1]);
        List<String> words = new ArrayList<>();
        for(int i = 2; i < fields.length; i++) {
            words.add(fields[i]);
        }
        return new StreamItem(timestamp, words);
	}

	public void close() throws IOException{
		try {
			in.close();
		}catch (IOException e) {
			throw e;
		}
	}
}
