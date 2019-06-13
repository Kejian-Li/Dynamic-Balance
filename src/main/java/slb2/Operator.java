package slb2;

import util.cardinality.HyperLogLog;

public class Operator implements ILoad {

    private StreamPartitioner partitioner;   // core of upstream operators
    private Operator[] downstreamOperators;  // for upstream operators to have references of downstream operators

    private long elementCount;        // for downstream operators to get load statistics
    private HyperLogLog hyperLogLog;  // for other algorithm to get cardinality statistics
    private final int DEFAULT_LOG2M = 12;

    public Operator() {  // for downstream operators
        elementCount = 0;
        hyperLogLog = new HyperLogLog(DEFAULT_LOG2M);
    }

    public Operator(StreamPartitioner partitioner, Operator[] downstreamOperators) {  // for upstream operators
        this.partitioner = partitioner;
        this.downstreamOperators = downstreamOperators;
    }

    public void processElement(Object key)  {  // for upstream operators
        // process element, then partition it
        int selected = 0;
        try {
            selected = partitioner.partition(key);
        }catch (Exception e) {
            e.printStackTrace();
        }

        downstreamOperators[selected].receiveElement(key);
    }

    public void receiveElement(Object key) {  // for downstream operator
        elementCount++;
        hyperLogLog.offer(Integer.parseInt(key.toString()));
    }

    @Override
    public long getLoad() {
        return elementCount;
    }

    @Override
    public long getCardinality() {
        return hyperLogLog.cardinality();
    }
}
