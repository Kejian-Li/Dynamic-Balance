package data;

import java.util.NavigableMap;
import java.util.TreeMap;

public class ZipfGenerator {

    private NavigableMap<Double, Integer> map;

    private static final double Constant = 1.0;

    public ZipfGenerator(int size, double skewness) {  // skewness range: [0.8, 2.0]
        // create the TreeMap
        map = generateZipfData(size, skewness);
    }

    //size为rank个数，skew为数据倾斜程度, 取值为0表示数据无倾斜，取值越大倾斜程度越高
    private static NavigableMap<Double, Integer> generateZipfData(int size, double skew) {
        NavigableMap<Double, Integer> map = new TreeMap<>();
        //总频率
        double div = 0;
        //对每个rank，计算对应的词频，计算总词频
        for (int i = 1; i <= size; i++) {
            //the frequency in position i
            div += (Constant / Math.pow(i, skew));
        }
        //计算每个rank对应的y值，所以靠前rank的y值区间远比后面rank的y值区间大
        double sum = 0;
        for (int i = 1; i <= size; i++) {
            double p = (Constant / Math.pow(i, skew)) / div;
            sum += p;
            map.put(sum, i - 1);
        }
        return map;
    }

    public NavigableMap<Double, Integer> getZipfDataMap() {
        return map;
    }

    //    public int next() {         // [1,n]
//        double value = random.nextDouble();
//        //找最近y值对应的rank
//        return map.ceilingEntry(value).getValue() + 1;
//    }

}
