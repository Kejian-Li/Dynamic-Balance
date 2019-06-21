package slb2.partitioners;

import util.cardinality.HyperLogLogPlus;


/**
 * Abstract class for partitioners except Holistic.
 */
public abstract class AbstractPartitioner implements StreamPartitioner, ICardinality {

    protected HyperLogLogPlus hyperLogLog;
    private final static int DEFAULT_LOG2M = 12; // 12 for 10^7 keys of 32 bits

    public AbstractPartitioner() {
        hyperLogLog = new HyperLogLogPlus(DEFAULT_LOG2M);
    }

    /**
     * for statistics of distinct keys
     * @param key
     */
    @Override
    public void add(Object key) {
//        hyperLogLog.offer(Integer.parseInt(key.toString()));  // for zipf whose data element is integer
        hyperLogLog.offer(key);
    }

    @Override
    public long getTotalCardinality() {
        return hyperLogLog.cardinality();
    }
}
