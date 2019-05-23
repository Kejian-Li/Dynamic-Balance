package util;

import java.util.List;
import java.util.Set;

public interface IFrequencyList<T> extends IBaseFrequency<T> {


    /**
     * @return The keys of the items stored in the data structure
     */
    Set<T> keySet();

    /**
     * Get the k most frequent elements.
     * @param k The maximum number of elements to be returned
     * @return A list of the most frequent items, ordered in descending order of frequency.
     */
    List<CountEntry<T>> peek(int k);

    /**
     * Get the most frequent items, sorted in descending order of frequency.
     * @param k The maximum number of items to be returned
     * @param minSupport
     * @return The list with the k most frequent items
     */
    List<CountEntry<T>> peek(int k, double minSupport);

    /**
     * @return The list of all the frequent items without any particular order,
     * with the default support.
     */
    List<CountEntry<T>> getFrequentItems();

    /**
     * @param minSupport
     * @return The list of all the frequent items without any particular order.
     */
    List<CountEntry<T>> getFrequentItems(double minSupport);
}
