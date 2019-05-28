package slb2;

import com.csvreader.CsvReader;
import slb.StreamItem;
import slb.StreamItemReader;

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

    public Simulator(int numSources, int numServers, String inFileName, StreamPartitioner partitioner) {
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

    }

    public void start() throws Exception {
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
        System.out.println("Starting to read the item stream");
        long simulationStartTime = System.currentTimeMillis();
        StreamItemReader reader = new StreamItemReader(in);
        StreamItem item = reader.nextItem();
        long currentTimestamp;

        int sourceIndex = 0;
        Operator operator;

        int itemCount = 0;
        // core loop
        while (item != null) {
            if (++itemCount % PRINT_INTERVAL == 0) {
                System.out.println("Read " + itemCount / 1000000
                        + "M tweets.\tSimulation time: "
                        + (System.currentTimeMillis() - simulationStartTime) + " ms");
            }
            currentTimestamp = item.getTimestamp();
            String key = item.getWord(0);

            operator = upstreamOperators[sourceIndex];   // round-robin emulation for upstream operators
            operator.processElement(currentTimestamp, key);

            sourceIndex++;
            if (sourceIndex == numSources) {
                sourceIndex = 0;
            }
            item = reader.nextItem();
        }

        reader.close();
        System.out.println();
        System.out.println("Finished reading items\nTotal items: " + itemCount);

        return itemCount;
    }

    private long startEmulate(CsvItemReader reader) throws Exception {
        System.out.println("Starting to read the item stream");

        long simulationStartTime = System.currentTimeMillis();
        String[] item;
        try {
            item = reader.nextItem();
        } catch (IOException e) {
            throw e;
        }
        int sourceIndex = 0;
        Operator operator;

        long wordCount = 0;
        // core loop
        while (item != null) {
            for (int i = 0; i < item.length; i++) {
                if (++wordCount % PRINT_INTERVAL == 0) {
                    System.out.println("Read " + wordCount / 1000000
                            + "M words.\tSimulation time: "
                            + (System.currentTimeMillis() - simulationStartTime) + " ms");
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
        System.out.println();
        System.out.println("Finished reading items\nTotal words: " + wordCount);

        return wordCount;
    }

    private void outputResult(Operator[] downstreamOperators, int numServers, long totalCount) {
        System.out.println();
        System.out.println(partitioner.getName() + " Partitioner output:");
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
