package slb2.partitioners;

public interface ICardinality {

    /**
     * add the key to HyperLogLog or HyperLogLogPlus for counting distinct keys
     * @param key
     */
    void add(Object key);

    long getTotalCardinality();

}
