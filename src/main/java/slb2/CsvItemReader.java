package slb2;

import com.csvreader.CsvReader;

import java.io.IOException;

/**
 * For csv format file, eg., twitter dataset
 */
public class CsvItemReader implements ItemReader {

    private CsvReader reader;

    public CsvItemReader(CsvReader reader) throws IOException {
        reader.readHeaders();
        this.reader = reader;
    }

    @Override
    public String[] nextItem() {
        String text = null;
        try {
            if (reader.readRecord()) {
                text = reader.get(4); // tweet id = 0,...., text = 4,...
            } else {
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        String[] words = text.split(" ");  // split text into words as keys
        return words;
    }

    public void close() {
        reader.close();
    }
}
