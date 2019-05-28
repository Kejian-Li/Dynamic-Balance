package slb2;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import slb.Constants;

import java.util.HashMap;
import java.util.List;

public class DChoices_Partitioner implements StreamPartitioner {

    private int numServers;
    private long localLoad[];
    private StreamSummary<String> streamSummary;

    private Seed seed;
    private HashFunction[] hashes;
    private long totalElement;
    private int threshold;
    private float epsilon;

    public DChoices_Partitioner(int numServers, int threshold, float epsilon) {
        this.numServers = numServers;
        this.localLoad = new long[numServers];
        this.streamSummary = new StreamSummary<>(Constants.STREAM_SUMMARY_CAPACITY);
        this.seed = new Seed(numServers);

        hashes = new HashFunction[numServers];

        for (int i = 0; i < hashes.length; i++) {
            hashes[i] = Hashing.murmur3_128(seed.getSeed(i));
        }
        this.threshold = threshold;
        this.epsilon = epsilon;
    }

    @Override
    public int partition(Object key) throws Exception {
        totalElement++;
        streamSummary.offer(key.toString());

        int choices = 2;
        float probability = choices / (float) (numServers * threshold);

        HashMap<String, Long> topK = getTopK(streamSummary, probability, totalElement);

        if (topK.containsKey(key.toString())) {
            double topFrequency = getTopFrequency(streamSummary, totalElement);
            PHeadCount pHead = getPHead(streamSummary, probability, totalElement);
            double pTail = 1 - pHead.probability;
            double n = (double) numServers;
            double val1 = (n - 1) / n;
            int d = (int) Math.round(topFrequency * n);
            double val2, val3, val4, sum1;
            double sum2, value1, value2, value3, value4;
            do {
                //finding sum Head
                val2 = Math.pow(val1, pHead.numberOfElements * d);
                val3 = 1 - val2;
                val4 = Math.pow(val3, 2);
                sum1 = pHead.probability + pTail * val4;

                //finding sum1
                value1 = Math.pow(val1, d);
                value2 = 1 - value1;
                value3 = Math.pow(value2, d);
                value4 = Math.pow(value2, 2);
                sum2 = topFrequency + ((pHead.probability - topFrequency) * value3) + (pTail * value4);
                d++;
            } while ((d <= numServers) && ((sum1 > (0 + epsilon)) || (sum2 > (value2 + epsilon))));
            choices = d - 1;
        }

        //Hash the Tail accordingly
        int i = 0;
        int[] selected = new int[choices];

        if (choices < numServers) {
            while (i < choices) {
                selected[i] = Math.abs(hashes[i].hashBytes(key.toString().getBytes()).asInt() % numServers);
                i++;
            }
        } else {
            while (i < choices) {
                selected[i] = i;
                i++;
            }
        }

        int chosen = chooseMinLoad(localLoad, selected);
        localLoad[chosen]++;

        return chosen;
    }

    @Override
    public String getName() {
        return "D-Choices";
    }

    private int chooseMinLoad(long[] localLoad, int[] selected) {
        int min = selected[0];
        long minOne = localLoad[selected[0]];
        for (int i = 1; i < selected.length; i++) {
            if (localLoad[selected[i]] < minOne) {
                minOne = localLoad[selected[i]];
                min = selected[i];
            }
        }
        return min;
    }

    private HashMap<String, Long> getTopK(StreamSummary<String> streamSummary, float probability, long totalItems) {
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

    private PHeadCount getPHead(StreamSummary<String> streamSummary, double probability, long totalItems) {
        PHeadCount pHeadCount = new PHeadCount();
        pHeadCount.probability = 0;

        List<Counter<String>> counters = streamSummary.topK(streamSummary.getCapacity());

        for (Counter<String> counter : counters) {
            float count = counter.getCount();
            float error = counter.getError();
            float itemProb = (count + error) / totalItems;
            if (itemProb > probability) {
                pHeadCount.probability += itemProb;
                pHeadCount.numberOfElements++;
            }
        }
        return pHeadCount;
    }

    private float getTopFrequency(StreamSummary<String> streamSummary, long totalItems) {
        List<Counter<String>> counters = streamSummary.topK(1);
        for (Counter<String> counter : counters) {
            float count = counter.getCount();
            float error = counter.getError();
            float frequency = (count + error) / totalItems;
            return frequency;
        }
        return 0f;
    }

}
