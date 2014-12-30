package com.tdunning.math.stats;

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;


public class BatchTDigest2 {

    private final double compression;
    protected Random gen = new Random();

    private Histogram buffer = new Histogram(false);
    private Histogram main = new Histogram(false);
    private Histogram spare = new Histogram(false);
    
    final static class Centroid  {
        private double centroid;
        private int count;

        public Centroid(double x, int w) {
            centroid = x;
            count = w;
        }

        public double mean() {
            return centroid;
        }

        public int count() {
            return count;
        }

        @Override
        public String toString() {
            return "Centroid{" +
                    "centroid=" + centroid +
                    ", count=" + count +
                    '}';
        }
    }
    
    final static class Histogram implements Iterable<Centroid> {

        /**
         * Merge sort src1 and src2 into dest.
         */
        public static void merge(Histogram src1, Histogram src2, Histogram dest) {
            assert dest.length() == 0;
            final int len1 = src1.length();
            final int len2 = src2.length();
            int i = 0;
            int j = 0;
            while (i < len1 && j < len2) {
                final double mean1 = src1.mean(i);
                final double mean2 = src2.mean(j);
                if (src1.mean(i) < src2.mean(j)) {
                    dest.append(mean1, src1.count(i));
                    i += 1;
                } else {
                    dest.append(mean2, src2.count(j));
                    j += 1;
                }
            }
            while (i < len1) {
                dest.append(src1.mean(i), src1.count(i));
                i += 1;
            }
            while (j < len2) {
                dest.append(src2.mean(j), src2.count(j));
                j += 1;
            }
        }

        private static double weightedAverage(double d1, int w1, double d2, int w2) {
            assert d1 <= d2;
            final double average = (d1 * w1 + d2 * w2) / (w1 + w2);
            // the below line exists because of floating-point rounding errors, for
            // the compact method we need the order to be maintained!
            return Math.max(d1, Math.min(average, d2));
        }

        /** Compress <code>src</code> into <code>dest</code>. */
        public static void compact(Histogram src, Histogram dest, double compression) {
            // The key here is that this method is guaranteed to maintain order since
            // we only merge adjacent keys and the histogram is sorted
            assert dest.length() == 0;
            assert src.sorted();
            final long totalCount = src.totalCount();
            double currentMean = Double.NaN;
            int currentCount = 0;
            for (int srcIdx = 0, length = src.length(); srcIdx < length; ++srcIdx) {
                final double mean = src.mean(srcIdx);
                final int count = src.count(srcIdx);

                if (Double.isNaN(currentMean)) {
                    currentMean = mean;
                    currentCount = count;
                } else if (srcIdx == length - 1 || mean - currentMean <= src.mean(srcIdx + 1) - mean) {
                    // the current mean is the closest mean, try to merge
                    final double q = totalCount == 1 ? 0.5 : (dest.totalCount() + (currentCount + count - 1) / 2.0) / (totalCount - 1);
                    final double k = 4 * totalCount * q * (1 - q) / compression;
                    if (count + currentCount <= k) {
                        // count is ok => merge
                        currentMean = weightedAverage(currentMean, currentCount, mean, count);
                        currentCount += count;
                    } else {
                        // count is too high => don't merge
                        dest.append(currentMean, currentCount);
                        dest.append(mean, count);
                        currentMean = Double.NaN;
                    }
                } else {
                    // the next mean is closer, flush
                    dest.append(currentMean, currentCount);
                    currentMean = mean;
                    currentCount = count;
                }
            }
            if (Double.isNaN(currentMean) == false) {
                dest.append(currentMean, currentCount);
            }
        }

        private double[] means;
        private int[] counts;
        private long totalCount;
        private int length;

        Histogram(boolean record) {
            means = new double[8];
            counts = new int[8];
        }

        void append(double centroid, int count) {
            if (length == means.length) {
                // grow by 1/4
                final int newCapacity = length + (length >>> 2);
                means = Arrays.copyOf(means, newCapacity);
                counts = Arrays.copyOf(counts, newCapacity);
            }
            means[length] = centroid;
            counts[length] = count;
            length += 1;
            totalCount += count;
        }

        long totalCount() {
            return totalCount;
        }

        int length() {
            return length;
        }

        double mean(int i) {
            return means[i];
        }

        int count(int i) {
            return counts[i];
        }

        void reset() {
            length = 0;
            totalCount = 0;
        }

        boolean sorted() {
            for (int i = 0; i < length - 1; ++i) {
                if (means[i] > means[i + 1]) {
                    return false;
                }
            }
            return true;
        }

        void sort() {
            quickSort(0, length, ThreadLocalRandom.current());
        }

        private void swap(int i, int j) {
            final double tmpMean = means[i];
            means[i] = means[j];
            means[j] = tmpMean;
            final int tmpCount = counts[i];
            counts[i] = counts[j];
            counts[j] = tmpCount;
        }

        private void quickSort(int from, int to, Random random) {
            if (to - from <= 1) {
                // sorted by definition
                return;
            }
            final int p = partition(from, to, random);
            quickSort(from, p, random);
            quickSort(p + 1, to, random);
        }

        private int partition(int from, int to, Random random) {
            final int pivotIndex = from + random.nextInt(to - from);
            final double pivotValue = means[pivotIndex];
            swap(pivotIndex, to - 1);
            int p = from;
            for (int i = from; i < to - 1; ++i) {
                if (means[i] < pivotValue) {
                    swap(i, p++);
                }
            }
            swap(p, to - 1);
            return p;
        }

        @Override
        public String toString() {
            StringBuilder s = new StringBuilder().append("{ ");
            for (int i = 0; i < length(); ++i) {
                s.append(mean(i)).append(":").append(count(i)).append(", ");
            }
            if (length() > 1) {
                s.setLength(s.length() - 2);
            }
            return s.append(" }").toString();
        }

        @Override
        public Iterator<Centroid> iterator() {
            return new Iterator<Centroid>() {

                int i = 0;
                
                @Override
                public boolean hasNext() {
                    return i < length;
                }

                @Override
                public Centroid next() {
                    final Centroid next = new Centroid(mean(i), count(i));
                    i += 1;
                    return next;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    
    public static double interpolate(double x, double x0, double x1) {
        return (x - x0) / (x1 - x0);
    }

    public static void encode(ByteBuffer buf, int n) {
        int k = 0;
        while (n < 0 || n > 0x7f) {
            byte b = (byte) (0x80 | (0x7f & n));
            buf.put(b);
            n = n >>> 7;
            k++;
            if (k >= 6) {
                throw new IllegalStateException("Size is implausibly large");
            }
        }
        buf.put((byte) n);
    }

    public static int decode(ByteBuffer buf) {
        int v = buf.get();
        int z = 0x7f & v;
        int shift = 7;
        while ((v & 0x80) != 0) {
            if (shift > 28) {
                throw new IllegalStateException("Shift too large in decode");
            }
            v = buf.get();
            z += (v & 0x7f) << shift;
            shift += 7;
        }
        return z;
    }

    protected static BatchTDigest2 merge(Iterable<BatchTDigest2> subData, Random gen, BatchTDigest2 r) {
        List<Centroid> centroids = new ArrayList<Centroid>();
        for (BatchTDigest2 digest : subData) {
            for (Centroid centroid : digest.centroids()) {
                centroids.add(centroid);
            }
        }
        Collections.shuffle(centroids, gen);

        for (Centroid c : centroids) {
            r.add(c.mean(), c.count(), c);
        }
        return r;
    }

    static double quantile(double previousIndex, double index, double nextIndex, double previousMean, double nextMean) {
        final double delta = nextIndex - previousIndex;
        final double previousWeight = (nextIndex - index) / delta;
        final double nextWeight = (index - previousIndex) / delta;
        return previousMean * previousWeight + nextMean * nextWeight;
    }

    /**
     * Adds a sample to a histogram.
     *
     * @param x The value to add.
     */
    public void add(double x) {
        add(x, 1);
    }

    public void add(BatchTDigest2 other) {
        List<Centroid> tmp = new ArrayList<Centroid>();
        for (Centroid centroid : other.centroids()) {
            tmp.add(centroid);
        }

        Collections.shuffle(tmp, gen);
        for (Centroid centroid : tmp) {
            add(centroid.mean(), centroid.count(), centroid);
        }
    }

    protected Centroid createCentroid(double mean, int id) {
        return new Centroid(mean, id);
    }

    public double quantile(double q) {
        if (q < 0 || q > 1) {
            throw new IllegalArgumentException("q should be in [0,1], got " + q);
        }

        final int centroidCount = centroids().size();
        if (centroidCount == 0) {
            return Double.NaN;
        } else if (centroidCount == 1) {
            return centroids().iterator().next().mean();
        }

        // if values were stored in a sorted array, index would be the offset we are interested in
        final double index = q * (size() - 1);

        double previousMean = Double.NaN, previousIndex = 0;
        long total = 0;
        // Jump over pages until we reach the page containing the quantile we are interested in
        Iterator<? extends Centroid> it = centroids().iterator();
        Centroid next;
        while (true) {
            next = it.next();
            final double nextIndex = total + (next.count() - 1.0) / 2;
            if (nextIndex >= index) {
                if (Double.isNaN(previousMean)) {
                    assert total == 0;
                    // special case 1: the index we are interested in is before the 1st centroid
                    if (nextIndex == previousIndex) {
                        return next.mean();
                    }
                    // assume values grow linearly between index previousIndex=0 and nextIndex2
                    Centroid next2 = it.next();
                    final double nextIndex2 = total + next.count() + (next2.count() - 1.0) / 2;
                    previousMean = (nextIndex2 * next.mean() - nextIndex * next2.mean()) / (nextIndex2 - nextIndex);
                }
                // common case: we found two centroids previous and next so that the desired quantile is
                // after 'previous' but before 'next'
                return quantile(previousIndex, index, nextIndex, previousMean, next.mean());
            } else if (!it.hasNext()) {
                // special case 2: the index we are interested in is beyond the last centroid
                // again, assume values grow linearly between index previousIndex and (count - 1)
                // which is the highest possible index
                final double nextIndex2 = size() - 1;
                final double nextMean2 = (next.mean() * (nextIndex2 - previousIndex) - previousMean * (nextIndex2 - nextIndex)) / (nextIndex - previousIndex);
                return quantile(nextIndex, index, nextIndex2, next.mean(), nextMean2);
            }
            total += next.count();
            previousMean = next.mean();
            previousIndex = nextIndex;
        }
    }

    public double cdf(double x) {
        if (size() == 0) {
            return Double.NaN;
        } else if (size() == 1) {
            return x < centroids().iterator().next().mean() ? 0 : 1;
        } else {
            double r = 0;

            // we scan a across the centroids
            Iterator<? extends Centroid> it = centroids().iterator();
            Centroid a = it.next();

            // b is the look-ahead to the next centroid
            Centroid b = it.next();

            // initially, we set left width equal to right width
            double left = (b.mean() - a.mean()) / 2;
            double right = left;

            // scan to next to last element
            while (it.hasNext()) {
                if (x < a.mean() + right) {
                    return (r + a.count() * AbstractTDigest.interpolate(x, a.mean() - left, a.mean() + right)) / size();
                }
                r += a.count();

                a = b;
                b = it.next();

                left = right;
                right = (b.mean() - a.mean()) / 2;
            }

            // for the last element, assume right width is same as left
            left = right;
            a = b;
            if (x < a.mean() + right) {
                return (r + a.count() * AbstractTDigest.interpolate(x, a.mean() - left, a.mean() + right)) / size();
            } else {
                return 1;
            }
        }
    }

    public BatchTDigest2(double compression) {
        this.compression = compression;
    }
    
    void add(double x, int w, Centroid base) {
        if (x != base.mean() || w != base.count()) {
            throw new IllegalArgumentException();
        }
        add(x, w);
    }
    
    private void merge() {
        if (buffer.length() > 0) {
            buffer.sort();
            Histogram.merge(buffer, main, spare);
            buffer.reset();

            main.reset();
            Histogram.compact(spare, main, compression);
            spare.reset();
        }
    }
    
    protected final void checkValue(double x) {
        if (Double.isNaN(x)) {
            throw new IllegalArgumentException("Cannot add NaN");
        }
    }
    
    public void add(double x, int w) {
        checkValue(x);
        buffer.append(x, w);
        if (buffer.length() >= main.length()) {
            merge();

            if (main.length() > 10 * compression) {
                compress();
            }
        }
    }

    public void compress() {
        Histogram.compact(main, spare, compression);
        main.reset();
        Histogram.compact(spare, main, compression);
        spare.reset();
    }

    public long size() {
        return main.totalCount() + buffer.totalCount();
    }

    public Collection<Centroid> centroids() {
        merge();
        return new AbstractList<Centroid>() {
            @Override
            public Centroid get(int index) {
                return new Centroid(main.mean(index), main.count(index));
            }

            @Override
            public int size() {
                return main.length();
            }
        };
    }

    public double compression() {
        return compression;
    }

    public int byteSize() {
        merge();
        return 4 + 8 + 4 + main.length() * 12;
    }

    public int smallByteSize() {
        int bound = byteSize();
        ByteBuffer buf = ByteBuffer.allocate(bound);
        asSmallBytes(buf);
        return buf.position();
    }

    public final static int VERBOSE_ENCODING = 1;
    public final static int SMALL_ENCODING = 2;

    /**
     * Outputs a histogram as bytes using a particularly cheesy encoding.
     */
    public void asBytes(ByteBuffer buf) {
        merge();
        buf.putInt(VERBOSE_ENCODING);
        buf.putDouble(compression());
        buf.putInt(centroids().size());
        for (Centroid centroid : centroids()) {
            buf.putDouble(centroid.mean());
        }

        for (Centroid centroid : centroids()) {
            buf.putInt(centroid.count());
        }
    }

    public void asSmallBytes(ByteBuffer buf) {
        buf.putInt(SMALL_ENCODING);
        buf.putDouble(compression());
        buf.putInt(centroids().size());

        double x = 0;
        for (Centroid centroid : centroids()) {
            double delta = centroid.mean() - x;
            x = centroid.mean();
            buf.putFloat((float) delta);
        }

        for (Centroid centroid : centroids()) {
            int n = centroid.count();
            encode(buf, n);
        }
    }

    /**
     * Reads a histogram from a byte buffer
     *
     * @return The new histogram structure
     */
    public static BatchTDigest2 fromBytes(ByteBuffer buf) {
        int encoding = buf.getInt();
        if (encoding == VERBOSE_ENCODING) {
            double compression = buf.getDouble();
            BatchTDigest2 r = new BatchTDigest2(compression);
            int n = buf.getInt();
            double[] means = new double[n];
            for (int i = 0; i < n; i++) {
                means[i] = buf.getDouble();
            }
            for (int i = 0; i < n; i++) {
                r.main.append(means[i], buf.getInt());
            }
            return r;
        } else if (encoding == SMALL_ENCODING) {
            double compression = buf.getDouble();
            BatchTDigest2 r = new BatchTDigest2(compression);
            int n = buf.getInt();
            double[] means = new double[n];
            double x = 0;
            for (int i = 0; i < n; i++) {
                double delta = buf.getFloat();
                x += delta;
                means[i] = x;
            }

            for (int i = 0; i < n; i++) {
                int z = decode(buf);
                r.main.append(means[i], z);
            }
            return r;
        } else {
            throw new IllegalStateException("Invalid format for serialized histogram");
        }
    }
   
}
