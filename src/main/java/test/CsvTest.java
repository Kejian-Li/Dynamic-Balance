package test;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class CsvTest {

    public static void main(String[] args) throws IOException {
        String filePathName = "C:\\Users\\lizi\\Desktop\\分布式流处理系统的数据分区算法研究\\paper_writing\\hello.csv";

        File csvFile = new File(filePathName);
        if (csvFile.exists()) {
            csvFile.delete();
        }
        csvFile.createNewFile();
        try {
            CsvReader reader = new CsvReader(filePathName);
            String[] columnName = new String[4];
            columnName[0] = "tuples/M";
            columnName[1] = "load imbalance";
            columnName[2] = "cardinality imbalance";
            columnName[3] = "simulation time/ms";
            reader.setHeaders(columnName);
            String[] headers = reader.getHeaders();
            for (int i = 0; i < headers.length; i++) {
                System.out.print(headers[i] + ", ");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        CsvWriter writer = new CsvWriter(filePathName);
        String[] record = new String[4];
        record[0] = "1";
        record[1] = "0.1";
        record[2] = "0.1";
        record[3] = "4000";
        writer.writeRecord(record);
        writer.close();

    }


}
