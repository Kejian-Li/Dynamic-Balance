package slb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

public class Main {
    private static final double PRINT_INTERVAL = 1e6;

    private static void ErrorMessage() {
        System.err.println("Choose the type of simulator using:");
        System.err
                .println("1. Partial Key Grouping with multiple sources with local estimation from data: <SimulatorType inFileName outFileName serversNo initialTime NumSources>");
        System.err
                .println("2. Simple Consistent Hashing hash mode num of Servers: <SimulatorType inFileName outFileName serversNo initialTime>");

        // threshold
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

    private static void flush(Iterable<Server> series, BufferedWriter out,
                              long timestamp) throws IOException {
        for (Server serie : series) { // sync all servers to the current
            // timestamp
            serie.synch(timestamp);
        }
        boolean hasMore = false;
        do {
            for (Server serie : series) {
                hasMore &= serie.flushNext(out);
            }
            out.newLine();
        } while (hasMore);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 5) {
            ErrorMessage();
        }

        final int simulatorType = Integer.parseInt(args[0]);
        final String inFileName = args[1];
        final String outFileName = args[2];
        final int numServers = Integer.parseInt(args[3]);
        final long initialTime = Long.parseLong(args[4]);
        int numSources = Integer.parseInt(args[5]);

        // default
        int threshold = 5;   // frequency threshold of Head

        float epsilon = 0.0001f;   // lossy count frequency threshold

        if (simulatorType == 5 || simulatorType == 6 || simulatorType == 3 || simulatorType == 4) {
            threshold = Integer.parseInt(args[6]);
        }

        if (simulatorType == 4) {
            epsilon = Float.parseFloat(args[7]);
        }

        float delta = 0.2f;
        float alpha = 0.4f;
        if (simulatorType == 7) {
            delta = Float.parseFloat(args[6]);
            alpha = Float.parseFloat(args[7]);
        }


        // initialize numServers Servers per TimeGranularity
        EnumMap<TimeGranularity, List<Server>> timeSeries = new EnumMap<>(TimeGranularity.class);
        for (TimeGranularity tg : TimeGranularity.values()) {
            List<Server> list = new ArrayList<>(numServers);
            for (int i = 0; i < numServers; i++)
                list.add(new Server(initialTime, tg, 0));
            timeSeries.put(tg, list);
        }

        // initialize one output file per TimeGranularity
        EnumMap<TimeGranularity, BufferedWriter> outputs = new EnumMap<>(TimeGranularity.class);
        for (TimeGranularity tg : TimeGranularity.values()) {
            outputs.put(tg, new BufferedWriter(new FileWriter(outFileName + "_"
                    + tg.toString() + ".txt")));
        }

        // initialize one LoadBalancer per TimeGranularity for simulatorTypes
        EnumMap<TimeGranularity, LoadBalancer> hashes = new EnumMap<>(TimeGranularity.class);
        for (TimeGranularity tg : TimeGranularity.values()) {
            if (simulatorType == 1) {
                hashes.put(tg, new LBPartialKeyGrouping(timeSeries.get(tg),
                        numSources));
            } else if (simulatorType == 2) {
                hashes.put(tg, new LBHashing(timeSeries.get(tg)));
            } else if (simulatorType == 3) {
                hashes.put(tg, new LBGreedyChoice(timeSeries.get(tg),
                        numSources, threshold));
            } else if (simulatorType == 4) {
                hashes.put(tg, new LBDChoice(timeSeries.get(tg),
                        numSources, threshold, epsilon));
            } else if (simulatorType == 5) {
                hashes.put(tg, new LBWChoice(timeSeries.get(tg),
                        numSources, threshold));
            } else if (simulatorType == 6) {
                hashes.put(tg, new LBRR(timeSeries.get(tg),
                        numSources, threshold));
            } else if (simulatorType == 7) {
                hashes.put(tg, new LBDB(timeSeries.get(tg),
                        numSources, delta, alpha));  //epsilon -> alpha
            }
        }

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

        while (item != null) {
            if (++itemCount % PRINT_INTERVAL == 0) {
                System.out.println("Read " + itemCount / 1000000
                        + "M tweets.\tSimulation time: "
                        + (System.currentTimeMillis() - simulationStartTime)
                        / 1000 + " seconds");
                for (BufferedWriter bw : outputs.values())
                    // flush output every PRINT_INTERVAL items
                    bw.flush();
            }

            currentTimestamp = item.getTimestamp();
            EnumSet<TimeGranularity> statsToConsume = EnumSet.noneOf(TimeGranularity.class); // empty set of time series

            String key = item.getWord(0);
            for (Entry<TimeGranularity, LoadBalancer> entry : hashes.entrySet()) {

                LoadBalancer loadBalancer = entry.getValue();
                Server server = loadBalancer.getSever(currentTimestamp, key);
                boolean hasStatsReady = server.updateStats(currentTimestamp, key);

                if (hasStatsReady)
                    statsToConsume.add(entry.getKey());
            }


            for (TimeGranularity granularity : statsToConsume) {
                printStatsToConsume(timeSeries.get(granularity), outputs.get(granularity), currentTimestamp);
            }

            item = reader.nextItem();
        }

        long[] totalLoad;
        long[][] localLoad;

        for (Entry<TimeGranularity, LoadBalancer> entry : hashes.entrySet()) {
            MyLoadBalancer loadBalancer = (MyLoadBalancer) entry.getValue();
            localLoad = loadBalancer.getLocalLoad();
            totalLoad = loadBalancer.getTotalLoad();
            System.out.println(entry.getKey().toString() + " =>");
            System.out.println("upstream sources load: ");
            for (int i = 0; i < numSources - 1; i++) {
                System.out.print(totalLoad[i] + ", ");
            }
            System.out.println(totalLoad[numSources - 1]);
            System.out.println("downstream servers load: ");
            for (int i = 0; i < numServers; i++) {
                long serverLoad = 0;
                for (int j = 0; j < numSources; j++) {
                    serverLoad += localLoad[j][i];
                }
                System.out.print(serverLoad + ", ");
            }
            System.out.println();
        }

        // print final stats
        for (TimeGranularity tg : TimeGranularity.values()) {
            flush(timeSeries.get(tg), outputs.get(tg), currentTimestamp);
        }

        // close all files
        in.close();
        for (BufferedWriter bw : outputs.values())
            bw.close();

        System.out.println("Finished reading items\nTotal items: " + itemCount);
    }

    /**
     * Prints stats from time serie to out.
     *
     * @param servers   The series to print.
     * @param out       The writer to print to.
     * @param timestamp Time up to which to print.
     * @throws IOException
     */
    private static void printStatsToConsume(Iterable<Server> servers,
                                            BufferedWriter out, long timestamp) throws IOException {
        for (Server sever : servers) { // sync all servers to the current timestamp
            sever.synch(timestamp);
        }
        boolean hasMore = false;
        do {
            for (Server server : servers) { // print up to the point in which
                // all the servers have stats ready
                // (AND barrier)
                hasMore &= server.printNextUnused(out);
            }
            out.newLine();
        } while (hasMore);
    }
}
