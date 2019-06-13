package slb2;

import com.google.common.collect.Multimap;

public interface GetStatistics {

    long getTotalCardinality();

    Multimap<Integer, Integer> getVk();
}
