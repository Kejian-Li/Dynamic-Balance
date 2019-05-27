package slb2;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import slb.Constants;

import java.util.HashMap;
import java.util.List;

public class WChoices_Partitioner implements StreamPartitioner {


    private final int numServers;

    private long[] localLoad;    // for both head and tail
    private long[] localLoadHH;  // for head

    private StreamSummary<String> streamSummary;
    private Seed seed;
    private HashFunction[] hashes;
    private long totalElement;

    private int threshold;
    private int DEFAULT_CHOICES = 2;  // for tail, same as RR_Partitioner

    public WChoices_Partitioner(int numServers, int threshold) {
        this.numServers = numServers;
        this.threshold = threshold;

        this.localLoad = new long[numServers];
        this.localLoadHH = new long[numServers];


        this.streamSummary = new StreamSummary<>(Constants.STREAM_SUMMARY_CAPACITY);

        this.seed = new Seed(numServers);

        hashes = new HashFunction[numServers];
        for (int i = 0; i < hashes.length; i++) {
            hashes[i] = Hashing.murmur3_128(seed.getSeed(i));
        }
    }


    @Override
    public int partition(Object key) throws Exception {
        totalElement++;
        streamSummary.offer(key.toString());

        float probability = 2 / (float) (numServers * threshold);
        HashMap<String, Long> topK = getTopK(streamSummary, probability, totalElement);

        if (topK.containsKey(key.toString())) {         // for head
            int[] wChoices = new int[numServers];
            int i = 0;
            while (i < numServers) {
                wChoices[i] = i;
                i++;
            }

            int chosen = chooseMinLoad(merge(localLoad, localLoadHH), wChoices);  // load-aware, RR is load-oblivious
            localLoadHH[chosen]++;
            return chosen;
        }

        //Hash the Tail accordingly
        int i = 0;                                      // for tail
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
            float count = counter.getCount();
            float error = counter.getError();
            float itemProb = (count + error) / totalItems;
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
        for (int i = 0; i < selected.length; i++) {
            if (localLoad[selected[i]] < minOne) {
                minOne = localLoad[selected[i]];
                min = selected[i];
            }
        }
        return min;
    }
}
