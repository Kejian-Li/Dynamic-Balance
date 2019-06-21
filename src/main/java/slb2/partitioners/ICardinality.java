package slb2.partitioners;

public interface ICardinality {

    /**
     * add given key to hyperloglog for counting distinct keys
     * @param key
     */
    void add(Object key);

    long getTotalCardinality();

}
