package slb2;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class Simulator {

    private final double PRINT_INTERVAL = 1e6;
    private int numServers;
    private int numSources;
    private String inFileName;
    private StreamPartitioner partitioner;

    private Operator[] upstreamOperators;
    private Operator[] downstreamOperators;

//    private DataOutputStream dos;

    private CsvWriter writer;

    public Simulator(int numSources, int numServers, String inFileName,
                     String outFilePathName, StreamPartitioner partitioner) throws Exception {
        this.numSources = numSources;
        this.numServers = numServers;
        this.inFileName = inFileName;
        this.partitioner = partitioner;

        downstreamOperators = new Operator[numServers]; // Operator entity for downstream
        for (int i = 0; i < numServers; i++) {
            downstreamOperators[i] = new Operator();
        }

        upstreamOperators = new Operator[numSources]; // Operator entity for upstream
        for (int i = 0; i < numSources; i++) {
            upstreamOperators[i] = new Operator(partitioner, downstreamOperators);
        }

        try {
            File csvFile = new File(outFilePathName);
            if (csvFile.exists()) {
                csvFile.delete();
            }
            csvFile.createNewFile();

            String[] columnName = new String[4];
            columnName[0] = "x";  // tuple/M
            columnName[1] = "y";  // load imbalance
            columnName[2] = "z";  // cardinality imbalance
            columnName[3] = "s";  // simulation time/ms

            writer = new CsvWriter(outFilePathName);
            writer.writeRecord(columnName);
        } catch (FileNotFoundException e) {
            throw e;
        }

    }

    public void start() throws Exception {
        System.out.println("Starting to read the item stream...");
        System.out.println(partitioner.getName() + " Partitioner output:");

        long totalCount = 0;
        if (inFileName.endsWith(".gz")) {            // for wikipedia dataset
            totalCount = startEmulate(getInput(inFileName));
        } else if (inFileName.endsWith(".csv")) {     // for twitter dataset
            totalCount = startEmulate(new CsvItemReader(new CsvReader(inFileName)));
        }

        outputResult(downstreamOperators, numServers, totalCount);
    }

    private BufferedReader getInput(String inFileName) throws IOException {
        BufferedReader in = null;
        try {
            InputStream rawin = new FileInputStream(inFileName);
            rawin = new GZIPInputStream(rawin);
            in = new BufferedReader(new InputStreamReader(rawin));
        } catch (FileNotFoundException e) {
            System.err.println("File not found");
            e.printStackTrace();
            System.exit(1);
        }
        return in;
    }


    private long startEmulate(BufferedReader in) throws Exception {
        System.out.println();

        long simulationStartTime = System.currentTimeMillis();
        StreamItemReader reader = new StreamItemReader(in);
        String[] item = reader.nextItem();

        int sourceIndex = 0;
        Operator operator;

        int itemCount = 0;
        // core loop
        while (item != null) {
            if (++itemCount % PRINT_INTERVAL == 0) {
                int x = itemCount / 1000000;
                long simulationDuration = System.currentTimeMillis() - simulationStartTime;
                System.out.println("Read " + x + "M tweets.\tSimulation time: " + simulationDuration + " ms");
                outputPartialResult(downstreamOperators, numServers, itemCount, x, simulationDuration);
            }
            String key = item[1]; // for wikipedia data

            operator = upstreamOperators[sourceIndex];   // round-robin emulation for upstream operators
            operator.processElement(key);

            sourceIndex++;
            if (sourceIndex == numSources) {
                sourceIndex = 0;
            }
            item = reader.nextItem();
        }

        reader.close();
        writer.close();
        System.out.println();
        System.out.println("Finished reading items\nTotal items: " + itemCount);

        return itemCount;
    }

    private long startEmulate(CsvItemReader reader) throws Exception {
        System.out.println();
        writer.writeComment("M tuples, load imbalance, cardinality imbalance, simulation time");

        long simulationStartTime = System.currentTimeMillis();
        String[] item = reader.nextItem();
        int sourceIndex = 0;
        Operator operator;

        long wordCount = 0;
        // core loop
        while (item != null) {
            for (int i = 0; i < item.length; i++) {
                if (++wordCount % PRINT_INTERVAL == 0) {
                    int x = (int) (wordCount / 1000000);
                    long simulationDuration = System.currentTimeMillis() - simulationStartTime;
                    System.out.println("Read " + x + "M words.\tSimulation time: " + simulationDuration + " ms");
                    outputPartialResult(downstreamOperators, numServers, wordCount, x, simulationDuration);
                }

                operator = upstreamOperators[sourceIndex];   // round-robin emulation for upstream operators
                operator.processElement(item[i]);

                sourceIndex++;
                if (sourceIndex == numSources) {
                    sourceIndex = 0;
                }
            }
            item = reader.nextItem();
        }

        reader.close();
        writer.close();

        System.out.println();
        System.out.println("Finished reading items\nTotal words: " + wordCount);

        return wordCount;
    }

    private void outputPartialResult(Operator[] downstreamOperators, int numServers,
                                     long totalCount, int x, long simulationDuration) {

        // output for load imbalance
        long maxLoad = downstreamOperators[0].getLoad();
        long temp;
        for (int i = 1; i < numServers; i++) {
            temp = downstreamOperators[i].getLoad();
            if (maxLoad < temp) {
                maxLoad = temp;
            }
        }

        double averageLoad = totalCount / (double) numServers;
        double loadImbalance = (maxLoad - averageLoad) / averageLoad;
        System.out.println("Load Imbalance: " + loadImbalance);

        long maxCardinality = downstreamOperators[0].getCardinality();
        long totalCardinality = 0;
        for (int i = 1; i < numServers; i++) {
            temp = downstreamOperators[i].getCardinality();
            totalCardinality += temp;
            if (maxCardinality < temp) {
                maxCardinality = temp;
            }
        }

        double averageCardinality = totalCardinality / (double) numServers;
        double cardinalityImbalance = (maxCardinality - averageCardinality) / averageCardinality;
        System.out.println("Cardinality Imbalance: " + cardinalityImbalance);
        System.out.println();

        String[] record = new String[4];
        record[0] = String.valueOf(x);
        record[1] = String.valueOf(loadImbalance);
        record[2] = String.valueOf(cardinalityImbalance);
        record[3] = String.valueOf(simulationDuration);

        try {
            writer.writeRecord(record);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void outputResult(Operator[] downstreamOperators, int numServers, long totalCount) {
        System.out.println();
        System.out.println();

        // output for load imbalance
        System.out.println("<1> Load: ");
        long maxLoad = downstreamOperators[0].getLoad();
        System.out.print(downstreamOperators[0].getLoad() + ",  ");
        long temp;
        for (int i = 1; i < numServers - 1; i++) {
            temp = downstreamOperators[i].getLoad();
            if (maxLoad < temp) {
                maxLoad = temp;
            }
            System.out.print(temp + ",  ");
        }
        temp = downstreamOperators[numServers - 1].getCardinality();
        if (maxLoad < temp) {
            maxLoad = temp;
        }
        System.out.println(temp);

        double averageLoad = totalCount / (double) numServers;
        double loadImbalance = (maxLoad - averageLoad) / averageLoad;
        System.out.println("Load Imbalance: " + loadImbalance);
        System.out.println();

        System.out.println("<2> Cardinality: ");
        long maxCardinality = downstreamOperators[0].getCardinality();
        System.out.print(downstreamOperators[0].getCardinality() + ",  ");
        long totalCardinality = 0;
        for (int i = 1; i < numServers - 1; i++) {
            temp = downstreamOperators[i].getCardinality();
            totalCardinality += temp;
            if (maxCardinality < temp) {
                maxCardinality = temp;
            }
            System.out.print(temp + ",  ");
        }
        temp = downstreamOperators[numServers - 1].getCardinality();
        totalCardinality += temp;
        if (maxCardinality < temp) {
            maxCardinality = temp;
        }
        System.out.println(temp);

        double averageCardinality = totalCardinality / (double) numServers;
        double cardinalityImbalance = (maxCardinality - averageCardinality) / averageCardinality;
        System.out.println("Cardinality Imbalance: " + cardinalityImbalance);
        System.out.println();
    }

}
