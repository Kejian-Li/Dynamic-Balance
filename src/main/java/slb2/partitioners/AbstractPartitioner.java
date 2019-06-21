package slb2.partitioners;

import util.cardinality.HyperLogLogPlus;


/**
 * Abstract class for all partitioners.
 */
public abstract class AbstractPartitioner implements StreamPartitioner, ICardinality {

    protected HyperLogLogPlus hyperLogLog;
    private final static int DEFAULT_LOG2M = 24;

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
