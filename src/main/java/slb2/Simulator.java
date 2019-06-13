package slb2;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import com.google.common.collect.Multimap;

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
    private StreamPartitioner partitioner;

    private Operator[] upstreamOperators;
    private Operator[] downstreamOperators;
    private String outFilePathName;

    private float skewness;

//    private DataOutputStream dos;

    private DataType dataType;
    private CsvWriter writer;
    private CsvWriter testWriter;

    public Simulator(int numSources, int numServers, String inFilePathName,
                     String outFilePathName, StreamPartitioner partitioner, DataType dataType) throws Exception {
        this.numSources = numSources;
        this.numServers = numServers;
        this.inFilePathName = inFilePathName;
        this.partitioner = partitioner;
        this.dataType = dataType;

        downstreamOperators = new Operator[numServers]; // Operator entity for downstream
        for (int i = 0; i < numServers; i++) {
            downstreamOperators[i] = new Operator();
        }

        upstreamOperators = new Operator[numSources]; // Operator entity for upstream
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
        } catch (IOException e) {
            e.printStackTrace();
        }

        String[] columnName = new String[4];
        columnName[0] = "x";  // tuple/M
        columnName[1] = "y";  // load imbalance
        columnName[2] = "z";  // cardinality imbalance
        columnName[3] = "s";  // simulation time/ms

        writer = new CsvWriter(outFilePathName);
        try {
            writer.writeComment("M tuples, load imbalance, cardinality imbalance, simulation time");
            writer.writeRecord(columnName);   // for convenient plot charts
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
                writer.writeComment("zipf z, load imbalance, replication ratio");
                String[] columnName = new String[3];
                columnName[0] = "x";  // zipf z
                columnName[1] = "y";  // load imbalance
                columnName[2] = "z";  // replication ratio

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

    private void initializeCsvWriterForZipfSameSkewnessDifferentDelta(String outFilePathName) {
        File csvFile = new File(outFilePathName);
        if (!csvFile.exists()) {
            try {
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(csvFile, true));  // append
                writer = new CsvWriter(bufferedWriter, ',');
                writer.writeComment("delat a, load imbalance, key replication ratio");
                String[] columnName = new String[3];
                columnName[0] = "x";  // delta a
                columnName[1] = "y";  // load imbalance
                columnName[2] = "z";  // replication ratio

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

//            initializeCsvWriterByTuple(outFilePathName);
        initializeCsvWriterForZipfDifferentSkewness(outFilePathName);
//        initializeCsvWriterForZipfSameSkewnessDifferentDelta(outFilePathName);

//        initializeForTest();

        if (inFilePathName.endsWith(".gz")) {            // for wikipedia dataset
            startEmulate(getInput(inFilePathName));
        } else if (inFilePathName.endsWith(".csv")) {     // for twitter dataset
            if (dataType == DataType.TWITTER) {
                startEmulate(new CsvItemReader(new CsvReader(inFilePathName), DataType.TWITTER));
            } else if (dataType == DataType.ZIPF) {   // for zipf dataset
                    startEmulate(new CsvItemReader(new CsvReader(inFilePathName), DataType.ZIPF));
            }
        }

//        outputFinalResultByTuple(downstreamOperators, numServers);  // by tuples

        outputFinalResultForZipf(downstreamOperators, numServers);  // by different z for zipf
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
            if (++itemCount % PRINT_INTERVAL == 0) {
                int x = itemCount / 1000000;
                long simulationDuration = System.currentTimeMillis() - simulationStartTime;
                System.out.println("Read " + x + "M tweets.\tSimulation time: " + simulationDuration + " ms");
                outputPartialResultByTuple(downstreamOperators, numServers, itemCount, x, simulationDuration);
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
    }

    long wordCount = 0;

    private void startEmulate(CsvItemReader reader) throws Exception {  // for twitter
        System.out.println();

        long simulationStartTime = System.currentTimeMillis();
        String[] item = reader.nextItem();
        int sourceIndex = 0;
        Operator operator;

        // core loop
        while (item != null) {
            for (int i = 0; i < item.length; i++) {
                if (++wordCount % PRINT_INTERVAL == 0) {
                    int x = (int) (wordCount / 1000000);
                    long simulationDuration = System.currentTimeMillis() - simulationStartTime;
                    System.out.println("Read " + x + "M words.\tSimulation time: " + simulationDuration + " ms");

//                    outputPartialResultByTuple(downstreamOperators, numServers, wordCount, x, simulationDuration);
//                    outputPartialResultByTupleForZipf(downstreamOperators, numServers, wordCount);
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
//        writer.close();

//        testWriter.close();

        System.out.println();
        System.out.println("Finished reading items\nTotal words: " + wordCount);
    }

    private void initializeForTest() {
        String testCsvFilePath = "C:\\Users\\lizi\\Desktop\\分布式流处理系统的数据分区算法研究\\charts\\zipf_out" +
                "\\zipf_varing_out\\varing_z_volfram\\load_";

        skewness = 1.0f;
        testCsvFilePath = testCsvFilePath + skewness + "_" + numServers + "_statistics.csv";
        File csvFile = new File(testCsvFilePath);
        if (csvFile.exists()) {
            csvFile.delete();
        }
        try {
            csvFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            testWriter = new CsvWriter(testCsvFilePath);
            String[] columnName = new String[numServers];
            for (int i = 0; i < numServers; i++) {
                columnName[i] = String.valueOf(i);
            }

            testWriter.writeRecord(columnName);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void outputPartialResultByTupleForZipf(Operator[] downstreamOperators, int numServers, long wordCount) {
        String[] loads = new String[numServers];

        long maxLoad = downstreamOperators[0].getLoad();
        loads[0] = String.valueOf(maxLoad);
        long temp;
        for (int i = 1; i < numServers - 1; i++) {
            temp = downstreamOperators[i].getLoad();
            if (maxLoad < temp) {
                maxLoad = temp;
            }
            loads[i] = String.valueOf(temp);
        }

        temp = downstreamOperators[numServers - 1].getLoad();
        if (maxLoad < temp) {
            maxLoad = temp;
        }
        loads[numServers - 1] = String.valueOf(temp);

        try {
            testWriter.writeRecord(loads);
        } catch (IOException e) {
            e.printStackTrace();
        }

        double averageLoad = wordCount / (double) numServers;
        double loadImbalance = (maxLoad - averageLoad) / averageLoad;
        System.out.println("Load Imbalance: " + loadImbalance);

        long allCardinality = 0;
        for (int i = 1; i < numServers; i++) {
            temp = downstreamOperators[i].getCardinality();
            allCardinality += temp;
        }

        double keyReplicationRatio = (double) allCardinality / ((GetStatistics) partitioner).getTotalCardinality();
        System.out.println("Replication factor: " + keyReplicationRatio);
        System.out.println();
    }


    private void outputPartialResultByTuple(Operator[] downstreamOperators, int numServers,
                                            long temporaryCount, int x, long simulationDuration) {

        // output for load imbalance
        long maxLoad = downstreamOperators[0].getLoad();
        long temp;
        for (int i = 1; i < numServers; i++) {
            temp = downstreamOperators[i].getLoad();
            if (maxLoad < temp) {
                maxLoad = temp;
            }
        }

        double averageLoad = temporaryCount / (double) numServers;
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

    private void outputFinalResultForZipf(Operator[] downstreamOperators, int numServers) {
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

        long allCardinality = 0;
        for (int i = 0; i < numServers - 1; i++) {
            temp = downstreamOperators[i].getCardinality();
            allCardinality += temp;
            System.out.print(temp + ",  ");
        }
        temp = downstreamOperators[numServers - 1].getCardinality();
        allCardinality += temp;
        System.out.println(temp);

        System.out.println("All cardinality: " + allCardinality);
        long totalCardinality = ((GetStatistics) partitioner).getTotalCardinality();
        System.out.println("Total cardinality: " + totalCardinality);
        double keyReplicationFactor = allCardinality / (double) totalCardinality;
        System.out.println("Replication factor: " + keyReplicationFactor);

        outputForZipfDifferentSkewness(writer, loadImbalance, keyReplicationFactor);
//        outputForZipfSameSkewnessDifferentDelta(writer, loadImbalance, keyReplicationFactor);

        Multimap<Integer, Integer> Vk = ((GetStatistics) partitioner).getVk();
        System.out.println(Vk.keySet().size());
        for (int x : Vk.keySet()) {
            System.out.println(x + " " + Vk.get(x));
        }
    }

    private void outputForZipfDifferentSkewness(CsvWriter writer, double loadImbalance, double replicationRatio) {
        float z = 2.0f;
        String[] record = new String[3];
        record[0] = String.valueOf(z);
        record[1] = String.valueOf(loadImbalance);
        record[2] = String.valueOf(replicationRatio);
        try {
            writer.writeRecord(record);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void outputForZipfSameSkewnessDifferentDelta(CsvWriter writer, double loadImbalance, double keyReplicationRatio) {
        float delta = 0.001f;
        String[] record = new String[3];
        record[0] = String.valueOf(delta);
        record[1] = String.valueOf(loadImbalance);
        record[2] = String.valueOf(keyReplicationRatio);
        try {
            writer.writeRecord(record);
            writer.close();
        } catch (
                IOException e) {
            e.printStackTrace();
        }
    }

}
