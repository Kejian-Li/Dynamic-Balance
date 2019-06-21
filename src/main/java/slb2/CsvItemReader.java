package slb2;

import com.csvreader.CsvReader;

import java.io.IOException;

/**
 * For csv format file, e.g., twitter data set
 */
public class CsvItemReader implements ItemReader {

    private CsvReader reader;
    private DataType dataType;

    public CsvItemReader(CsvReader reader, DataType dataType) throws IOException {
        this.reader = reader;
        this.dataType = dataType;
    }


    @Override
    public String[] nextItem() {
        String[] item = null;
        try {
            if (reader.readRecord()) {
                if (dataType == DataType.TWITTER) {
                    String text = reader.get(4);            // tweet id = 0,...., text = 4,...
                    item = text.split(" ");   // split text into words as keys
                } else if (dataType == DataType.ZIPF) {
                    item = reader.getValues();
                }
            } else {
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return item;
    }

    public void close() {
        reader.close();
    }
}
