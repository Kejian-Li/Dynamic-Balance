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


        long timestamp = Long.parseLong(line.split(" ")[0]);
        String[] words = line.split(" ");

        List<String> lists = new ArrayList<>();
        for (int i = 1; i < words.length; i++) {
            lists.add(words[i]);
        }

        return new StreamItem(timestamp, lists);
    }
}
