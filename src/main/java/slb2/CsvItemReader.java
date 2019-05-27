package slb2;

import com.csvreader.CsvReader;

import java.io.IOException;

/**
 * For csv format, eg., twitter dataset
 */
public class CsvItemReader {

    private CsvReader reader;

    public CsvItemReader(CsvReader reader) throws IOException {
        reader.readHeaders();
        this.reader = reader;
    }

    public String[] nextItem() throws IOException {
        String text;
        try {
            if (reader.readRecord()) {
                text = reader.get(4); // tweet id = 0,...., text = 4,...
            } else {
                return null;
            }
        } catch (IOException e) {
            throw e;
        }

        String[] words = text.split(" ");  // split text into words as keys
        return words;
    }

    public void close() {
        reader.close();
    }
}
