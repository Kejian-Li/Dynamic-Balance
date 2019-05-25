import util.cardinality.HyperLogLog;

public class HyperLogLogTest {

    public static void main(String[] agrs) {
        HyperLogLog hyperLogLog = new HyperLogLog(6);
        hyperLogLog.cardinality();

    }
}
