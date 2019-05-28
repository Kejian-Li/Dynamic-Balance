package slb2;


import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import slb.Constants;

import java.util.HashMap;
import java.util.List;

/**
 * Unlike normal round-robin fashion of shuffle grouping, this partitioner treats the head and tail differently,
 * specializing on head. Compared with W-Choices, this algorithm assigns keys of the head in a load-oblivious manner.
 */

public class RR_Partitioner implements StreamPartitioner {



    private int numServers;

    private long[] localLoad;    // for both head and tail
    private long[] localLoadHH;  // for head

    private StreamSummary<String> streamSummary;
    private Seed seed;
    private HashFunction[] hashes;
    private long totalElement;
    private int nextIndex;  // round-robin for the Head
    private int DEFAULT_CHOICES = 2;  // for tail, same as WChoices_Partitioner

    private int threshold;

    public RR_Partitioner(int numServers, int threshold) {
        this.numServers = numServers;
        this.threshold = threshold;

        this.nextIndex = 0;

        this.localLoad = new long[numServers];
        this.localLoadHH = new long[numServers];

        streamSummary = new StreamSummary<>(Constants.STREAM_SUMMARY_CAPACITY);


        seed = new Seed(numServers);

        hashes = new HashFunction[numServers];
        for (int i = 0; i < numServers; i++) {
            hashes[i] = Hashing.murmur3_128(seed.getSeed(i));
        }

    }

    @Override
    public int partition(Object key) throws Exception {
        totalElement++;
        streamSummary.offer(key.toString());

        float probability = DEFAULT_CHOICES / (float) (numServers * threshold);
        HashMap<String, Long> topK = getTopK(streamSummary, probability, totalElement);

        if (topK.containsKey(key.toString())) {   // for head
            int selected = nextIndex;
            localLoadHH[selected]++;
            nextIndex++;                // load-oblivious, W-Choices is load-aware
            if (nextIndex == numServers) {
                nextIndex = 0;
            }
            return selected;
        }

        //Hash the Tail accordingly
        int i = 0;                                 // for tail
        int[] selected = new int[DEFAULT_CHOICES];

        while (i < DEFAULT_CHOICES) {
            selected[i] = Math.abs(hashes[i].hashBytes(key.toString().getBytes()).asInt() % numServers);
            i++;
        }

        int chosen = chooseMinLoad(merge(localLoad, localLoadHH), selected);

        localLoad[chosen]++;

        return chosen;
    }

    public HashMap<String, Long> getTopK(StreamSummary<String> streamSummary, float probability, long totalItems) {
        HashMap<String, Long> topK = new HashMap<>();
        List<Counter<String>> counters = streamSummary.topK(streamSummary.getCapacity());

        for (Counter<String> counter : counters) {
            float freq = counter.getCount();
            float error = counter.getError();
            float itemProb = (freq + error) / totalItems;
            if (itemProb > probability) {
                topK.put(counter.getItem(), counter.getCount());
            }
        }
        return topK;

    }

    private long[] merge(long[] arr1, long[] arr2) {
        long[] result = new long[arr1.length];
        for (int i = 0; i < arr1.length; i++)
            result[i] = arr1[i] + arr2[i];
        return result;
    }

    private int chooseMinLoad(long[] localLoad, int[] selected) {
        int min = selected[0];
        long minOne = localLoad[selected[0]];
        for (int i = 1; i < selected.length; i++) {
            if (localLoad[selected[i]] < minOne)
                minOne = localLoad[selected[i]];
                min = selected[i];
        }
        return min;
    }

    @Override
    public String getName() {
        return "RR";
    }
}
