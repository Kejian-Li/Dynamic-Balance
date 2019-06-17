package slb2;

import com.google.common.collect.Multimap;

/**
 * Interface for getting statistics.
 */
public interface GetStatistics {

    Multimap<Object, Integer> getVk();
}
