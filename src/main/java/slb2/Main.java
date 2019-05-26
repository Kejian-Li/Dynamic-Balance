package slb2;

import slb.StreamItem;
import slb.StreamItemReader;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class Main {
    private static final double PRINT_INTERVAL = 1e6;
    private static Operator[] upstreamOperators;
    private static Operator[] downstreamOperators;
    private static int numSources;


    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            ErrorMessage();
        }

        final int simulatorType = Integer.parseInt(args[0]);
        final String inFileName = args[1];
        numSources = Integer.parseInt(args[2]);  // number of upstream operators
        final int numServers = Integer.parseInt(args[3]);  // number of downstream operators

        // default
        int threshold = 5;   // frequency threshold of Head
        float epsilon = 0.0001f;   // lossy count frequency threshold

        if (simulatorType == 5 || simulatorType == 6 || simulatorType == 3 || simulatorType == 4) {
            threshold = Integer.parseInt(args[4]);
        }

        if (simulatorType == 4) {
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
            partitioner = new PKGPartitioner(numServers);
//        } else if (simulatorType == 3) {
//            partitioner = new LBGreedyChoice(threshold));
        } else if (simulatorType == 4) {
            partitioner = new DChoicesPartitioner(numServers, epsilon);
//        } else if (simulatorType == 5) {
//            partitioner = new LBWChoice(threshold));
        } else if (simulatorType == 6) {
            partitioner = new SGPartitioner(numServers);
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


        startEmulate(inFileName);
        outputResult(downstreamOperators, numSources, numServers);
    }

    private static void startEmulate(String inFileName) throws Exception {
        // read items and route them to the correct server
        System.out.println("Starting to read the item stream");
        BufferedReader in = null;
        try {
            InputStream rawin = new FileInputStream(inFileName);
            if (inFileName.endsWith(".gz"))
                rawin = new GZIPInputStream(rawin);
            in = new BufferedReader(new InputStreamReader(rawin));
        } catch (FileNotFoundException e) {
            System.err.println("File not found");
            e.printStackTrace();
            System.exit(1);
        }

        // core loop
        long simulationStartTime = System.currentTimeMillis();
        StreamItemReader reader = new StreamItemReader(in);
        StreamItem item = reader.nextItem();
        long currentTimestamp = 0;
        int itemCount = 0;

        int sourceIndex = 0;
        Operator operator;
        while (item != null) {
            if (++itemCount % PRINT_INTERVAL == 0) {
                System.out.println("Read " + itemCount / 1000000
                        + "M tweets.\tSimulation time: "
                        + (System.currentTimeMillis() - simulationStartTime) + " ms");
            }
            currentTimestamp = item.getTimestamp();
            String key = item.getWord(0);

            operator = upstreamOperators[sourceIndex];
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

    private static void outputResult(Operator[] downstreamOperators, int numSources, int numServers) {
        System.out.println();
        System.out.println();

        // output for load imbalance
        System.out.println("<1> Load: ");
        System.out.println();


        System.out.println();
        System.out.println("Cardinality Imbalance: ");
        System.out.println();
    }

    private static long findMax(long[] array) {
        long maxOne = array[0];
        for (int i = 1; i < array.length; i++) {
            if (maxOne < array[i]) {
                maxOne = array[i];
            }
        }
        return maxOne;
    }

    private static void ErrorMessage() {
        System.err.println("Choose the type of simulator using:");
        System.err
                .println("1. PKG: <SimulatorType inFileName outFileName serversNo initialTime NumSources>");
        System.err
                .println("2. Hash: <SimulatorType inFileName outFileName serversNo initialTime>");
        System.err
                .println("3. G-Choices : <SimulatorType inFileName outFileName serversNo initialTime>");
        System.err
                .println("4. D-Choices : <SimulatorType inFileName outFileName serversNo initialTime>"); // epsilon
        System.err
                .println("5. W-Choices : <SimulatorType inFileName outFileName serversNo initialTime>");
        System.err
                .println("6. Round Robin: <SimulatorType inFileName outFileName serversNo initialTime>");
        System.err
                .println("7. Round Robin: <SimulatorType inFileName outFileName serversNo initialTime>");

        System.exit(1);
    }

}
