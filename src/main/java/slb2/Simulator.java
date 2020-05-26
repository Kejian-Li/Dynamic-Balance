package slb2;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import slb2.partitioners.AbstractPartitioner;
import slb2.partitioners.HolisticPartitionerForString;
import slb2.partitioners.Operator;
import slb2.reader.CsvItemReader;
import slb2.reader.DataType;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Single thread simulator
 */
public class Simulator {

    private final double PRINT_INTERVAL = 1e6;
    private int numServers;
    private int numSources;
    private String inFilePathName;
    private AbstractPartitioner partitioner;

    private Operator[] upstreamOperators;
    private Operator[] downstreamOperators;
    private String outFilePathName;
    private DataType dataType;

    private CsvWriter writer;

    public Simulator(int numSources, int numServers, String inFilePathName,
                     String outFilePathName, AbstractPartitioner partitioner, DataType dataType) throws Exception {
        this.numSources = numSources;
        this.numServers = numServers;
        this.inFilePathName = inFilePathName;
        this.partitioner = partitioner;
        this.dataType = dataType;

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

    private void initializeCsvWriterByTuple(String outFilePathName) {


        File csvFile = new File(outFilePathName);
        if (csvFile.exists()) {
            csvFile.delete();
        }
        try {
            csvFile.createNewFile();

            this.writer = new CsvWriter(new FileWriter(outFilePathName), 'c');
            writer.writeComment("M tuples, load imbalance, replication factor, cardinality imbalance, simulation time");
            String[] columnName = new String[5];
            columnName[0] = "x";  // tuples/M
            columnName[1] = "y";  // load imbalance
            columnName[2] = "z";  // replication factor
            columnName[3] = "c";  // cardinality imbalance
            columnName[4] = "t";  // simulation time

            writer.writeRecord(columnName);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void initializeCsvWriterForZipfDifferentSkewness(String outFilePathName) {

        File csvFile = new File(outFilePathName);
        if (!csvFile.exists()) {
            try {
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(csvFile, true));  // append
                writer = new CsvWriter(bufferedWriter, ',');
                writer.writeComment("zipf z, load imbalance, replication factor, cardinality imbalance, simulation time");
                String[] columnName = new String[5];
                columnName[0] = "x";  // zipf z
                columnName[1] = "y";  // load imbalance
                columnName[2] = "z";  // replication factor
                columnName[3] = "c";  // cardinality imbalance
                columnName[4] = "t";  // simulation time

                writer.writeRecord(columnName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            try {
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(csvFile, true));  // append
                writer = new CsvWriter(bufferedWriter, ',');
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void initializeCsvWriterForZipfSameSkewnessDifferentDelta(String outFilePathName) {
        File csvFile = new File(outFilePathName);
        if (!csvFile.exists()) {
            try {
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(csvFile, true));  // append
                writer = new CsvWriter(bufferedWriter, ',');
                writer.writeComment("delta a, load imbalance, replication factor, cardinality imbalance, simulation time");
                String[] columnName = new String[5];
                columnName[0] = "x";  // zipf z
                columnName[1] = "y";  // load imbalance
                columnName[2] = "z";  // replication factor
                columnName[3] = "c";  // cardinality imbalance
                columnName[4] = "t";  // simulation time

                writer.writeRecord(columnName);   // for convenient plot charts
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            try {
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(csvFile, true));  // append
                writer = new CsvWriter(bufferedWriter, ',');
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public void start() throws Exception {
        System.out.println("Starting to read the item stream...");
        System.out.println(partitioner.getName() + " Partitioner output:");

        initializeCsvWriterByTuple(outFilePathName);
//        initializeCsvWriterForZipfDifferentSkewness(outFilePathName);
//        initializeCsvWriterForZipfSameSkewnessDifferentDelta(outFilePathName);

        long simulationTime = 0;

        if (inFilePathName.endsWith(".gz")) {            // for wikipedia dataset
            startEmulate(getInput(inFilePathName));
        } else if (inFilePathName.endsWith(".csv")) {     // for twitter dataset
            if (dataType == DataType.TWITTER) {
                startEmulate(new CsvItemReader(new CsvReader(inFilePathName), DataType.TWITTER));
            } else if (dataType == DataType.ZIPF) {   // for zipf dataset
                simulationTime = startEmulate(new CsvItemReader(new CsvReader(inFilePathName), DataType.ZIPF));
            }
        }

//        outputFinalResultByTuple(downstreamOperators, numServers);  // by tuples

//        outputFinalResultForZipf(downstreamOperators, numServers, simulationTime);  // by different z for zipf
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


    private void startEmulate(BufferedReader in) throws Exception {  // for wikipedia
        System.out.println();

        long simulationStartTime = System.currentTimeMillis();
        StreamItemReader reader = new StreamItemReader(in);
        String[] item = reader.nextItem();

        int sourceIndex = 0;
        Operator operator;

        int itemCount = 0;
        // core loop
        while (item != null) {
            String key = item[1]; // url of wikipedia data as key

            operator = upstreamOperators[sourceIndex];   // round-robin emulation for upstream operators
            operator.processElement(key);

            if (++itemCount % PRINT_INTERVAL == 0) {
                int x = (int) (itemCount / PRINT_INTERVAL);
                long simulationDuration = System.currentTimeMillis() - simulationStartTime;
                System.out.println("Read " + x + "M tweets.\tSimulation time: " + simulationDuration + " ms");
                outputPartialResultByTuple(downstreamOperators, numServers, itemCount, x, simulationDuration);
            }

            sourceIndex++;
            if (sourceIndex == numSources) {
                sourceIndex = 0;
            }
            item = reader.nextItem();
        }

        reader.close();

        System.out.println();
        System.out.println("Finished reading items\nTotal items: " + itemCount);
    }

    private long wordCount = 0;

    private long startEmulate(CsvItemReader reader) throws Exception {  // for twitter
        System.out.println();

        long simulationStartTime = System.currentTimeMillis();
        String[] item = reader.nextItem();
        int sourceIndex = 0;
        Operator operator;

        // core loop
        while (item != null) {
            for (int i = 0; i < item.length; i++) {

                operator = upstreamOperators[sourceIndex];   // round-robin emulation for upstream operators
                operator.processElement(item[i]);

                if (++wordCount % PRINT_INTERVAL == 0) {
                    int x = (int) (wordCount / PRINT_INTERVAL);
                    long simulationTime = System.currentTimeMillis() - simulationStartTime;
                    System.out.println("Read " + x + "M words.\tSimulation time: " + simulationTime + " ms");

                    outputPartialResultByTuple(downstreamOperators, numServers, wordCount, x, simulationTime);
                }

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
        return System.currentTimeMillis() - simulationStartTime;
    }


    private void outputPartialResultByTuple(Operator[] downstreamOperators, int numServers,
                                            long temporaryCount, int x, long simulationTime) {

        // output for load imbalance
        long maxLoad = downstreamOperators[0].getLoad();
        long tempLoad;
        for (int i = 1; i < numServers; i++) {
            tempLoad = downstreamOperators[i].getLoad();
            if (maxLoad < tempLoad) {
                maxLoad = tempLoad;
            }
        }

        double averageLoad = temporaryCount / (double) numServers;
        double loadImbalance = (maxLoad - averageLoad) / averageLoad;
        System.out.println("Load Imbalance: " + loadImbalance);

        long allCardinality = 0;
        long tempCardinality = 0;
        long maxCardinality = 0;
        for (int i = 0; i < numServers - 1; i++) {
            tempCardinality = downstreamOperators[i].getCardinality();
            allCardinality += tempCardinality;
            if (maxCardinality < tempCardinality) {
                maxCardinality = tempCardinality;
            }
        }

        tempCardinality = downstreamOperators[numServers - 1].getCardinality();
        allCardinality += tempCardinality;
        if (maxCardinality < tempCardinality) {
            maxCardinality = tempCardinality;
        }


        double averageCardinality = allCardinality / (double) numServers;
        double cardinalityImbalance = (maxCardinality - averageCardinality) / averageCardinality;
        System.out.println("Cardinality imbalance: " + cardinalityImbalance);

        long totalCardinality = partitioner.getTotalCardinality();
        double replicationFactor = allCardinality / (double) totalCardinality;
        System.out.println("Replication factor: " + allCardinality + " / " + totalCardinality + " = " + replicationFactor);

        System.out.println();

        String[] record = new String[5];
        record[0] = String.valueOf(x);
        record[1] = String.valueOf(loadImbalance);
        record[2] = String.valueOf(replicationFactor);
        record[3] = String.valueOf(cardinalityImbalance);
        record[3] = String.valueOf(simulationTime);

        try {
            writer.writeRecord(record);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void outputFinalResultByTuple(Operator[] downstreamOperators, int numServers) {
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

    private void outputFinalResultForZipf(Operator[] downstreamOperators, int numServers, long simulationTime) {
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
        System.out.println(((HolisticPartitionerForString) partitioner).getVk().size());
        System.out.println();

        outputForZipfDifferentSkewness(writer, loadImbalance, replicationFactor, cardinalityImbalance, simulationTime);
//        outputForZipfSameSkewnessDifferentDelta(writer, loadImbalance, replicationFactor, cardinalityImbalance, simulationTime);

    }

    private void outputForZipfDifferentSkewness(CsvWriter writer, double loadImbalance,
                                                double replicationFactor,
                                                double cardinalityImbalance,
                                                long simulationTime) {
        float z = 0.8f;
        String[] record = new String[5];
        record[0] = String.valueOf(z);
        record[1] = String.valueOf(loadImbalance);
        record[2] = String.valueOf(replicationFactor);
        record[3] = String.valueOf(cardinalityImbalance);
        record[4] = String.valueOf(simulationTime);
        try {
            writer.writeRecord(record);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void outputForZipfSameSkewnessDifferentDelta(CsvWriter writer, double loadImbalance,
                                                         double replicationFactor,
                                                         double cardinalityImbalance,
                                                         long simulationTime) {
        float delta = 0.001f;
        String[] record = new String[5];
        record[0] = String.valueOf(delta);
        record[1] = String.valueOf(loadImbalance);
        record[2] = String.valueOf(replicationFactor);
        record[3] = String.valueOf(cardinalityImbalance);
        record[4] = String.valueOf(simulationTime);
        try {
            writer.writeRecord(record);
            writer.close();
        } catch (
                IOException e) {
            e.printStackTrace();
        }
    }

}
