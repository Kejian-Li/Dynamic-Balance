package slb2;

public class Main {


    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            ErrorMessage();
        }

        final int simulatorType = Integer.parseInt(args[0]);
        final String inFilePathName = args[1];
        final int numSources = Integer.parseInt(args[2]);  // number of upstream operators
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
        String outputName = null;

        if (simulatorType == 1) {
            partitioner = new HashPartitioner(numServers);
            outputName = "hash";
        } else if (simulatorType == 2) {
            partitioner = new PKG_Partitioner(numServers);
            outputName = "pkg";
        } else if (simulatorType == 3) {
            partitioner = new DChoices_Partitioner(numServers, threshold, epsilon);
            outputName = "d-choices";
        } else if (simulatorType == 4) {
            partitioner = new WChoices_Partitioner(numServers, threshold);
            outputName = "w-choices";
        } else if (simulatorType == 5) {
            partitioner = new RR_Partitioner(numServers, threshold);
            outputName = "RR";
        } else if (simulatorType == 6) {
            partitioner = new SG_Partitioner(numServers);
            outputName = "shuffle";
        } else if (simulatorType == 7) {
            partitioner = new HolisticPartitioner(numServers, delta, alpha);  //epsilon -> alpha
            outputName = "holistic";
        }

        String outFilePathName = "C:\\Users\\lizi\\Desktop\\分布式流处理系统的数据分区算法研究\\paper_writing\\wiki_"
                + outputName + ".csv";

        Simulator simulator = new Simulator(numSources, numServers, inFilePathName, outFilePathName, partitioner);
        simulator.start();

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
