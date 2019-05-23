package data;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.NavigableMap;

public class ZipfTest {

    private static String outputFileUri = "C:\\Users\\lizi\\Desktop\\" +
            "基于Akka的分布式流处理系统的设计与实现\\DAS_workspace\\zipf_data\\synthetical_data\\hello.txt";

    private static int size = 100;
    private static double skewness = 1.2; //range [0.8, 1.2]

    public static void main(String[] agrs) throws Exception {
        ZipfGenerator z1 = new ZipfGenerator(size, skewness);

        PrintWriter pw = new PrintWriter(new FileWriter(outputFileUri));
        for (NavigableMap.Entry<Double, Integer> entry : z1.getZipfDataMap().entrySet()) {
            // System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
             String str="Key = " + entry.getKey() + ", Value = " + entry.getValue();
//            String str = entry.getKey() + " ";
            pw.println(str);
        }
        pw.close();
    }
}

