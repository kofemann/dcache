package org.dcache.commons.stats;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 *
 */
public final class Histograms {

    public static abstract class Histogram<T> {

        private final String title;

        public Histogram(String title) {
            this.title = title;
        }

        public String toString(int barSize) {

            Map<?, Integer> bins = this.getBins();

            StringBuilder sb = new StringBuilder("Distribution of ")
                    .append(title)
                    .append('\n');

            if (!bins.isEmpty()) {
                // find the top value
                int max = bins.values().stream().max(Integer::compare).get();

                bins.entrySet()
                        .stream()
                        .map(e -> {
                            return String.format("%24s | %6d | %s\n",
                                    String.valueOf(e.getKey()),
                                    e.getValue(),
                                    Strings.repeat("#", (int) (Math.floorDiv(barSize * e.getValue(), max))));
                        })
                        .forEach(sb::append);
            }
            return sb.toString();
        }

        public String getTitle() {
            return title;
        }

        @Override
        public String toString() {
            return this.toString(50);
        }

        abstract Map<?, Integer> getBins();

        public abstract void add(T v);
    }

    public static <T> Histogram<T> synchronizedHistogram(Histogram<T> h) {
        return new Histogram<T>(h.getTitle()) {
            @Override
            Map<?, Integer> getBins() {
                synchronized(h) {
                 return h.getBins();
                }
            }

            @Override
            public void add(T v) {
                synchronized(h) {
                    h.add(v);
                }
            }
        };
    }
    private static class ObjectHistogram<T> extends Histogram<T> {

        private final Map<T, Integer> bins = new TreeMap<>();

        public ObjectHistogram(String title) {
            super(title);
        }

        @Override
        public void add(T v) {
            bins.put(v, bins.getOrDefault(v, 0) + 1);
        }

        @Override
        Map<?, Integer> getBins() {
            return bins;
        }
    }

    public static class IntHistogram extends Histogram<Integer> {

        private int min = 0;
        private int max = 0;

        /**
         * offset two write in the ring buffer
         */
        private int offset = 0;

        /**
         * array to store values. used as a ring buffer
         */
        private final int values[];
        public IntHistogram(String title, int size) {
            super(title);
            values = new int[size];
            Arrays.fill(values, -1);
        }

        @Override
        Map<?, Integer> getBins() {

            int width = max - min;

            if (width == 0) {
                return Collections.emptyMap();
            }

            // log2(n) + 1
            int numberOfBins = (int) (Math.log(width)/Math.log(2)) + 1;

            int binWidth = Math.floorDiv((width), numberOfBins) -1;

            int[] bins = new int[numberOfBins +1];

            Arrays.stream(values)
                    .filter(v -> v != -1)
                    .boxed()
                    .collect(Collectors.groupingBy(v -> v))
                    .entrySet().forEach((e) -> {
                        int bin = (e.getKey() * numberOfBins) / (width);
                        bins[bin] += e.getValue().size();
                    });

            ImmutableRangeMap.Builder b = ImmutableRangeMap.builder();
            for(int i = 0; i < bins.length; i++) {
                b.put(Range.closedOpen(i*binWidth, (i+1)*binWidth), bins[i]);
            }
            return b.build().asMapOfRanges();
        }

        @Override
        public void add(Integer v) {
            min = Math.min(min, v);
            max = Math.max(max, v);

            values[offset] = v;
            offset = (++offset ) % values.length;
        }

    }

    public static <T> Histogram<T> newObjectHistogram(String title) {
        return new ObjectHistogram<>(title);
    }

    public static Histogram<Integer> newIntHistogram(String title, int size) {
        return new IntHistogram(title, size);
    }

    public static void main(String[] args) {

        Histogram<String> h = newObjectHistogram("Words");

        h.add("Hello");
        h.add("Hello");
        h.add("Hello");
        h.add("Hello");
        h.add("Hello");
        h.add("Hello");

        h.add("kuku");
        h.add("kuku");
        h.add("kuku");
        h.add("kuku");
        h.add("kuku");
        h.add("kuku");
        h.add("kuku");
        h.add("kuku");
        h.add("kuku");
        h.add("kuku");

        h.add("bla");

        h.add("bla");
        h.add("bla");

        System.out.println(h);


        Histogram A = synchronizedHistogram( newIntHistogram("NUmbers", 10000) );
        Random r = new Random();
        for(int i = 0; i < 100000; i++) {
            A.add(r.nextInt(100)+r.nextInt(100));
        }

        System.out.println(A);
    }
}
