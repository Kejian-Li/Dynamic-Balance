package slb;

import com.google.common.hash.Hashing;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * Local load of upstream sources for downstream servers
 */
public class LocalLoad {

    private int serverNum;
    private long elementCount[];
    private Map<Object, HashSet<Integer>> map;

    public LocalLoad(int serverNum) {
        this.serverNum = serverNum;
        elementCount = new long[serverNum];
    }

    public void increaseCount(int index) { // add key to downstream server of given index
        elementCount[index]++;
    }

    public long getMaxLoad() {
        long maxCount = elementCount[0];
        for (int i = 1; i < serverNum; i++) {
            if (elementCount[i] > maxCount) {
                maxCount = elementCount[i];
            }
        }
        return maxCount;
    }

    public int getIndexOfMinLoad() { // across all of downstream server
        int min = 0;
        long minCount = elementCount[0];
        for (int i = 1; i < serverNum; i++) {  // serverNum -> V
            if (elementCount[i] < minCount) {
                minCount = elementCount[i];
                min = i;
            }
        }
        return min;
    }


    public void addOriginalIndexIntoVk(Object key) {
        HashSet<Integer> Vk = new HashSet<>();
        Vk.add(hashIndex(key));
        map.put(key, Vk);
    }
    // add selected index into Vk
    public void addNewIndexIntoVk(Object key, int selected) {
        HashSet<Integer> Vk = map.get(key);
        Vk.add(selected);
    }

    private int hashIndex(Object key) {
        return Math.abs(Hashing.murmur3_128().hashBytes(key.toString().getBytes()).asInt() % serverNum);
    }

    public int findIndexOfMinLoadInVk(Object key) {
        HashSet<Integer> Vk = map.get(key);
        if (Vk == null || Vk.size() < 1) {
            System.out.println("Vk is not good");
        }
        Iterator<Integer> it = Vk.iterator();
        int min = it.next();
        long minOne = elementCount[min];
        int temp;
        while (it.hasNext()) {
            temp = it.next();
            if (elementCount[temp] < minOne) {
                minOne = elementCount[temp];
                min = temp;
            }
        }
        return min;
    }

}
