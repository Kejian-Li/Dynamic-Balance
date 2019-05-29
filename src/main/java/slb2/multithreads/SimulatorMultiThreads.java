package slb2.multithreads;

import com.csvreader.CsvWriter;
import slb2.Operator;
import slb2.StreamPartitioner;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Single thread simulator
 */
public class SimulatorMultiThreads {
    private int numServers;
    private int numSources;
    private String inFileName;
    private StreamPartitioner partitioner;

    private Operator[] downstreamOperators;

//    private DataOutputStream dos;

    private CsvWriter writer;

    public SimulatorMultiThreads(int numSources, int numServers, String inFileName,
                                 String outFilePathName, StreamPartitioner partitioner) throws Exception {
        this.numSources = numSources;
        this.numServers = numServers;
        this.inFileName = inFileName;
        this.partitioner = partitioner;

        downstreamOperators = new Operator[numServers]; // Operator entity for downstream
        for (int i = 0; i < numServers; i++) {
            downstreamOperators[i] = new Operator();
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

    public void start() {
        CountDownLatch latch = new CountDownLatch(numSources);
        ExecutorService executor = Executors.newFixedThreadPool(numSources);
        for (int i = 0; i < numSources; i++) {
            Task task = new Task(i, numSources, new Operator(partitioner, downstreamOperators), inFileName, latch);
            executor.submit(task);
        }

        try {
            latch.await();
        }catch (InterruptedException e) {
            e.printStackTrace();
        }
        outputResult(downstreamOperators, numServers);
    }


    private void outputResult(Operator[] downstreamOperators, int numServers) {
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

        long totalCount = 0;
        for (int i = 0; i < numServers; i++) {
            totalCount += downstreamOperators[i].getLoad();
        }

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
