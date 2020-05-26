package slb2.Wikipedia;

import com.csvreader.CsvWriter;
import slb2.StreamItemReader;
import slb2.partitioners.AbstractPartitioner;
import slb2.partitioners.Operator;


import java.io.*;
import java.util.zip.GZIPInputStream;

public class WikipediaSimulator {

    private final double PRINT_INTERVAL = 1e6;
    private int numServers;
    private int numSources;
    private String wikipediaFilePath;
    private AbstractPartitioner partitioner;

    private Operator[] upstreamOperators;
    private Operator[] downstreamOperators;
    private String outFilePathName;


    private CsvWriter writer;

    public WikipediaSimulator(int numSources, int numServers, String wikipediaFilePath,
                     String outFilePathName, AbstractPartitioner partitioner) {
        this.numSources = numSources;
        this.numServers = numServers;
        this.wikipediaFilePath = wikipediaFilePath;
        this.partitioner = partitioner;


        downstreamOperators = new Operator[numServers]; // Operators for downstream
        for (int i = 0; i < numServers; i++) {
            downstreamOperators[i] = new Operator();
        }

        upstreamOperators = new Operator[numSources]; // Operators for upstream
        for (int i = 0; i < numSources; i++) {
            upstreamOperators[i] = new Operator(partitioner, downstreamOperators);
        }
        this.outFilePathName = outFilePathName;
    }



    public void startEmulate(String wikipediaFilePath) throws Exception {  // for wikipedia
        System.out.println();

        BufferedReader in = null;
        try {
            InputStream rawin = new FileInputStream(wikipediaFilePath);
            rawin = new GZIPInputStream(rawin);
            in = new BufferedReader(new InputStreamReader(rawin));
        } catch (FileNotFoundException e) {
            System.err.println("File not found");
            e.printStackTrace();
            System.exit(1);
        }

        long simulationStartTime = System.currentTimeMillis();

        StreamItemReader reader = new StreamItemReader(in);
        String[] item = reader.nextItem();

        int sourceIndex = 0;
        Operator operator;

        int itemCount = 0;
        // core loop
        while (item != null) {
            String key = item[2]; // url of wikipedia data as key

            operator = upstreamOperators[sourceIndex];   // round-robin emulation for upstream operators
            operator.processElement(key);

            if (++itemCount % PRINT_INTERVAL == 0) {
                int x = (int) (itemCount / PRINT_INTERVAL);
                long simulationTime = System.currentTimeMillis() - simulationStartTime;
                System.out.println("Read " + x + "M words.\tSimulation time: " + simulationTime + " ms");
            }

            sourceIndex++;
            if (sourceIndex == numSources) {
                sourceIndex = 0;
            }
            item = reader.nextItem();
        }

        long finalEndTime = System.currentTimeMillis();
        reader.close();

        outputFinalResult(downstreamOperators, numServers);

        System.out.println();
        System.out.println("Finished reading items\nTotal items: " + itemCount);

        long simulationTotalTime = finalEndTime - simulationStartTime;
        System.out.println("Finished reading items\nTotal time: " + simulationTotalTime + " ms");
        System.out.println();
        System.out.println("Processing Time per Tuple: " + simulationTotalTime / (long) itemCount + " ms");
    }

    private void outputFinalResult(Operator[] downstreamOperators, int numServers) {
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
        temp = downstreamOperators[numServers - 1].getLoad();
        if (maxLoad < temp) {
            maxLoad = temp;
        }
        System.out.println(temp);

        long totalCount = 0;
        for (int i = 0; i < numServers; i++) {
            totalCount += downstreamOperators[i].getLoad();
        }

        double averageLoad = totalCount / numServers;
        double loadImbalance = (maxLoad - averageLoad) / averageLoad;
        System.out.println("Load Imbalance: " + loadImbalance);
        System.out.println();

        System.out.println("<2> Cardinality: ");

        long allCardinality = 0;
        long maxCardinality = 0;
        for (int i = 0; i < numServers - 1; i++) {
            temp = downstreamOperators[i].getCardinality();
            allCardinality += temp;
            if (maxCardinality < temp) {
                maxCardinality = temp;
            }
            System.out.print(temp + ",  ");
        }
        temp = downstreamOperators[numServers - 1].getCardinality();
        allCardinality += temp;
        if (maxCardinality < temp) {
            maxCardinality = temp;
        }
        System.out.println(temp);

        long totalCardinality = partitioner.getTotalCardinality();
        double averageCardinality = allCardinality / numServers;
        double cardinalityImbalance = (maxCardinality - averageCardinality) / averageCardinality;
        System.out.println("Cardinality imbalance: " + cardinalityImbalance);

        double replicationFactor = allCardinality / (double) totalCardinality;
        System.out.println("Replication factor: " + allCardinality + " / " + totalCardinality + " = " + replicationFactor);

        System.out.println();

    }



}
