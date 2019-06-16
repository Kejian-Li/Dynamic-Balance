package slb2;

import com.google.common.collect.Multimap;

/**
 * Interface for getting statistics.
 */
public interface GetStatistics {

    long getTotalCardinality();

    Multimap<Integer, Integer> getVk();
}
