package org.broadinstitute.hellbender.tools.spark.utils;

import org.broadinstitute.hellbender.tools.spark.sv.SVUtils;

import java.util.*;

/**
 * Created by tsharpe on 4/1/16.
 */
public final class HopscotchHashTimingTest {

    @FunctionalInterface
    public interface Action {
        void execute();
    }
    private static double time( final Action action ) {
        long nanosecs = System.nanoTime();
        action.execute();
        return (System.nanoTime() - nanosecs)/1.E9;
    }

    private static final int N_TRIALS = 5;
    private static final int N_VALUES = 20000000;

    public static void main( String[] args ) {

        final Random rng = new Random(0xdeadbeef);
        final List<Integer[]> trials = new ArrayList<>(N_TRIALS);
        for ( int trialId = 0; trialId != N_TRIALS; ++trialId ) {
            Integer[] values = new Integer[N_VALUES];
            for ( int valueId = 0; valueId != N_VALUES; ++valueId ) {
                values[valueId] = rng.nextInt();
            }
            trials.add(values);
        }

        final List<Set<Integer>> hashSets = new ArrayList<>(N_TRIALS);
        System.out.println("HashSet construction: "+time( () -> {
            for ( Integer[] values : trials ) {
                Set<Integer> hashSet = new HashSet<>(SVUtils.hashMapCapacity(N_VALUES));
                for (Integer value : values) {
                    hashSet.add(value);
                }
                hashSets.add(hashSet);
            }
        }));

        final List<Set<Integer>> hopscotchHashSets = new ArrayList<>(N_TRIALS);
        System.out.println("HopscotchHashSet construction: "+time( () -> {
            for (Integer[] values : trials) {
                Set<Integer> hashSet = new HopscotchHashSet<>(N_VALUES);
                for (Integer value : values) {
                    hashSet.add(value);
                }
                hopscotchHashSets.add(hashSet);
            }
        }));

        System.out.println("HashSet +retrieval: "+time( () -> {
            for (int trialId = 0; trialId != N_TRIALS; ++trialId) {
                Set<Integer> hashSet = hashSets.get(trialId);
                for (Integer value : trials.get(trialId))
                    hashSet.contains(value);
            }
        }));

        System.out.println("HopscotchHashSet +retrieval: "+time( () -> {
            for (int trialId = 0; trialId != N_TRIALS; ++trialId) {
                Set<Integer> hashSet = hopscotchHashSets.get(trialId);
                for (Integer value : trials.get(trialId))
                    hashSet.contains(value);
            }
        }));

        Integer[] missingValues = new Integer[N_VALUES];
        for ( int valueId = 0; valueId != N_VALUES; ++valueId ) {
            missingValues[valueId] = rng.nextInt();
        }

        System.out.println("HashSet -retrieval: "+time( () -> {
            for (int trialId = 0; trialId != N_TRIALS; ++trialId) {
                Set<Integer> hashSet = hashSets.get(trialId);
                for (Integer value : missingValues)
                    hashSet.contains(value);
            }
        }));

        System.out.println("HopscotchHashSet -retrieval: "+time( () -> {
            for (int trialId = 0; trialId != N_TRIALS; ++trialId) {
                Set<Integer> hashSet = hopscotchHashSets.get(trialId);
                for (Integer value : missingValues)
                    hashSet.contains(value);
            }
        }));
    }
}
