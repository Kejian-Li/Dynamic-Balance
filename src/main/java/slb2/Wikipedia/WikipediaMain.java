package slb2.Wikipedia;


import slb2.partitioners.*;

public class WikipediaMain {

    public static void main(String[] args) {

        final int simulatorType = Integer.parseInt(args[0]);

        final int numSources = Integer.parseInt(args[1]);  // number of upstream operators
        final int numServers = Integer.parseInt(args[2]);  // number of downstream operators

        // default
        int threshold = 5;   // frequency threshold of Head


        AbstractPartitioner partitioner = null;

        String outputFileName = null;

        if (simulatorType == 1) {
            partitioner = new HashPartitioner(numServers);
            outputFileName = "hash";
        } else if (simulatorType == 2) {
            partitioner = new PKG_Partitioner(numServers);
            outputFileName = "pkg";
        } else if (simulatorType == 3) {
            partitioner = new DChoices_Partitioner(numServers, threshold);
            outputFileName = "d-choices";
        } else if (simulatorType == 4) {
            partitioner = new WChoices_Partitioner(numServers, threshold);
            outputFileName = "w-choices";
        } else if (simulatorType == 5) {
            partitioner = new HolisticPartitionerForString(numServers);
            outputFileName = "holistic";
        }

        final String outFilePath = "C:\\Users\\lizi\\Desktop\\Holistic_Workspace\\wikipedia_workspace";  // just path

        String outFilePathName = outFilePath + "\\" + "twitter_" + numServers + "_" + outputFileName + ".csv";

        String wikipediaFilePath = "C:\\Users\\lizi\\Desktop\\Holistic_Workspace\\dataset\\wiki_dataset\\wiki.1191201596.gz";
        WikipediaSimulator simulator = new WikipediaSimulator(numSources, numServers, wikipediaFilePath, outFilePathName, partitioner);

        try {
            simulator.startEmulate(wikipediaFilePath);
        }catch (Exception e) {
            e.printStackTrace();
        }

    }
}
