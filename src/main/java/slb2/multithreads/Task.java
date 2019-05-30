package slb2.multithreads;

import com.csvreader.CsvReader;
import slb2.CsvItemReader;
import slb2.DataType;
import slb2.Operator;
import slb2.StreamItemReader;

import java.io.*;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPInputStream;

/**
 * For upstream source operators
 */
public class Task implements Runnable {

    private int index;
    private int numSources;
    private Operator operator;
    private String inFileName;
    private CountDownLatch latch;

    public Task(int index, int numSources, Operator operator, String inFileName, CountDownLatch latch) {
        this.index = index;
        this.numSources = numSources;
        this.operator = operator;
        this.inFileName = inFileName;
        this.latch = latch;
    }

    @Override
    public void run() {
        if (inFileName.endsWith(".gz")) {            // for wikipedia dataset
            startEmulate(getInput(inFileName));
        } else if (inFileName.endsWith(".csv")) {     // for twitter dataset
            try {
                if (inFileName.startsWith("twcs")) {
                    startEmulate(new CsvItemReader(new CsvReader(inFileName), DataType.TWITTER));
                }else if(inFileName.startsWith("zipf_data")) {
                    startEmulate(new CsvItemReader(new CsvReader(inFileName), DataType.ZIPF));
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        latch.countDown();
    }


    private BufferedReader getInput(String inFileName) {
        BufferedReader in = null;
        try {
            InputStream rawin = new FileInputStream(inFileName);
            rawin = new GZIPInputStream(rawin);
            in = new BufferedReader(new InputStreamReader(rawin));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return in;
    }

    private void startEmulate(BufferedReader in) {
        StreamItemReader reader = new StreamItemReader(in);
        String[] item = reader.nextItem();
        // core loop
        while (item != null) {
            String key = item[1]; // for wikipedia data
            operator.processElement(key);
            item = reader.nextItem();
        }
        try {
            reader.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void startEmulate(CsvItemReader reader) {  // concurrent read
        skipLines(reader, index);
        String[] item = reader.nextItem();
        // core loop
        while (item != null) {
            for (int i = 0; i < item.length; i++) {
                operator.processElement(item[i]);
            }
            skipLines(reader, numSources);
            item = reader.nextItem();
        }
        reader.close();
    }

    private void skipLines(CsvItemReader reader, int x) {
        for (int i = 0; i < x; i++) {
            reader.skipLine();
        }
    }

}
