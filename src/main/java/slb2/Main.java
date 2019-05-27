package slb2;

import com.csvreader.CsvReader;
import slb.StreamItem;
import slb.StreamItemReader;

import java.io.*;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;

public class Main {
    private static final double PRINT_INTERVAL = 1e6;
    private static Operator[] upstreamOperators;
    private static Operator[] downstreamOperators;
    private static int numSources;


    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            ErrorMessage();
        }

        final int simulatorType = Integer.parseInt(args[0]);
        final String inFileName = args[1];
        numSources = Integer.parseInt(args[2]);  // number of upstream operators
        final int numServers = Integer.parseInt(args[3]);  // number of downstream operators

        // default
        int threshold = 5;   // frequency threshold of Head
        float epsilon = 0.0001f;   // lossy count frequency threshold

        if (simulatorType == 3 || simulatorType == 4 || simulatorType == 5) {
            threshold = Integer.parseInt(args[4]);
        }

        if (simulatorType == 3) {
            epsilon = Float.parseFloat(args[5]);
        }

        float delta = 0.2f;
        float alpha = 0.4f;
        if (simulatorType == 7) {
            delta = Float.parseFloat(args[4]);
            alpha = Float.parseFloat(args[5]);
        }

        StreamPartitioner partitioner = null;

        if (simulatorType == 1) {
            partitioner = new HashPartitioner(numServers);
        } else if (simulatorType == 2) {
            partitioner = new PKG_Partitioner(numServers);
        } else if (simulatorType == 3) {
            partitioner = new DChoices_Partitioner(numServers, threshold, epsilon);
        } else if (simulatorType == 4) {
            partitioner = new WChoices_Partitioner(numServers, threshold);
        } else if (simulatorType == 5) {
            partitioner = new RR_Partitioner(numServers, threshold);
        } else if (simulatorType == 6) {
            partitioner = new SG_Partitioner(numServers);
        } else if (simulatorType == 7) {
            partitioner = new HolisticPartitioner(numServers, delta, alpha);  //epsilon -> alpha
        }

        downstreamOperators = new Operator[numServers]; // Operator entity for downstream
        for (int i = 0; i < numServers; i++) {
            downstreamOperators[i] = new Operator();
        }

        upstreamOperators = new Operator[numSources]; // Operator entity for upstream
        for (int i = 0; i < numSources; i++) {
            upstreamOperators[i] = new Operator(partitioner, downstreamOperators);
        }

        if (inFileName.endsWith(".gz")) {    // for wikipedia dataset
            startEmulate(getInput(inFileName));
        } else if (inFileName.endsWith(".csv")) {  // for twitter dataset
            startEmulate(new CsvItemReader(new CsvReader(inFileName)));
        }

        outputResult(downstreamOperators, numServers);
    }


    private static BufferedReader getInput(String inFileName) throws IOException {
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

    private static int itemCount = 0;

    private static void startEmulate(BufferedReader in) throws Exception {
        System.out.println("Starting to read the item stream");
        long simulationStartTime = System.currentTimeMillis();
        StreamItemReader reader = new StreamItemReader(in);
        StreamItem item = reader.nextItem();
        long currentTimestamp;

        int sourceIndex = 0;
        Operator operator;

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
    }

    private static void startEmulate(CsvItemReader reader) throws Exception {
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
                    System.out.println("Read " + itemCount / 1000000
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
        System.out.println("Finished reading items\nTotal items: " + wordCount);
    }

    private static void outputResult(Operator[] downstreamOperators, int numServers) {
        System.out.println();
        System.out.println();

        // output for load imbalance
        System.out.println("<1> Load: ");
        long maxLoad = downstreamOperators[0].getLoad();

        long temp;
        for (int i = 0; i < numServers - 1; i++) {
            temp = downstreamOperators[i].getLoad();
            if (maxLoad < temp) {
                maxLoad = temp;
            }
            System.out.print(temp + ",  ");
        }
        System.out.println(downstreamOperators[numServers - 1].getLoad());
        System.out.println();

        double averageLoad = itemCount / numServers;
        double loadImbalance = (maxLoad / averageLoad) - 1;
        System.out.println("Load Imbalance: " + loadImbalance);
        System.out.println();

        System.out.println("<2> Cardinality: ");
        long maxCardinality = downstreamOperators[0].getCardinality();
        long totalCardinality = 0;
        for (int i = 0; i < numServers - 1; i++) {
            temp = downstreamOperators[i].getCardinality();
            totalCardinality += temp;
            if (maxCardinality < temp) {
                maxCardinality = temp;
            }
            System.out.print(temp + ",  ");
        }
        temp = downstreamOperators[numServers - 1].getCardinality();
        totalCardinality += temp;
        System.out.println(temp);

        System.out.println();
        double averageCardinality = totalCardinality / numServers;
        double cardinalityImbalance = (maxCardinality / averageCardinality) - 1;
        System.out.println("Cardinality Imbalance: " + cardinalityImbalance);
        System.out.println();
    }

    private static void ErrorMessage() {
        System.err.println("Choose the type of simulator using:");

        System.err
                .println("1. Hash: <SimulatorType inFileName numServers >");
        System.err
                .println("2. PKG: <SimulatorType inFileName numSources numServers>");
        System.err
                .println("3. D-Choices : <SimulatorType inFileName numServers>"); // epsilon
        System.err
                .println("4. W-Choices : <SimulatorType inFileName numServers>");
        System.err
                .println("5. Round Robin: <SimulatorType inFileName numServers>");
        System.err
                .println("6. Shuffle: <SimulatorType inFileName numServer>");
        System.err
                .println("7. Holistic: <SimulatorType inFileName numServer>");

        System.exit(1);
    }

}
