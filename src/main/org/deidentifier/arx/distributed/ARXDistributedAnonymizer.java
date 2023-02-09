/*
 * ARX Data Anonymization Tool
 * Copyright 2012 - 2022 Fabian Prasser and contributors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.deidentifier.arx.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.deidentifier.arx.ARXConfiguration;
import org.deidentifier.arx.ARXConfiguration.Monotonicity;
import org.deidentifier.arx.Data;
import org.deidentifier.arx.DataDefinition;
import org.deidentifier.arx.DataHandle;
import org.deidentifier.arx.aggregates.HierarchyBuilder;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased;
import org.deidentifier.arx.aggregates.StatisticsQuality;
import org.deidentifier.arx.aggregates.quality.QualityConfiguration;
import org.deidentifier.arx.aggregates.quality.QualityDomainShare;
import org.deidentifier.arx.aggregates.quality.QualityDomainShareRaw;
import org.deidentifier.arx.aggregates.quality.QualityDomainShareRedaction;
import org.deidentifier.arx.aggregates.quality.QualityModelColumnOrientedLoss;
import org.deidentifier.arx.common.Groupify;
import org.deidentifier.arx.common.TupleWrapper;
import org.deidentifier.arx.common.WrappedBoolean;
import org.deidentifier.arx.common.WrappedInteger;
import org.deidentifier.arx.criteria.EDDifferentialPrivacy;
import org.deidentifier.arx.exceptions.RollbackRequiredException;

/**
 * Distributed anonymizer
 * @author Fabian Prasser
 *
 */
public class ARXDistributedAnonymizer {
    
    /**
     * Distribution strategy
     * @author Fabian Prasser
     */
    public enum DistributionStrategy {
        LOCAL
    }

    /**
     * Partitioning strategy
     * @author Fabian Prasser
     */
    public enum PartitioningStrategy {
        RANDOM,
        SORTED
    }
    
    /**
     * Strategy for defining common transformation levels
     * @author Fabian Prasser
     */
    public enum TransformationStrategy {
        GLOBAL_AVERAGE,
        GLOBAL_MINIMUM,
        LOCAL
    }

    /** O_min */
    private static final double          O_MIN     = 0.05d;

    /** Wait time */
    private static final int             WAIT_TIME = 100;

    /** Number of nodes to use */
    private final int                    nodes;
    /** Partitioning strategy */
    private final PartitioningStrategy   partitioningStrategy;
    /** Distribution strategy */
    private final DistributionStrategy   distributionStrategy;
    /** Distribution strategy */
    private final TransformationStrategy transformationStrategy;
    /** Track memory consumption */
    private final boolean                trackMemoryConsumption;

    /**
     * Creates a new instance
     * @param nodes
     * @param partitioningStrategy
     * @param distributionStrategy
     * @param transformationStrategy
     */
    public ARXDistributedAnonymizer(int nodes,
                                    PartitioningStrategy partitioningStrategy,
                                    DistributionStrategy distributionStrategy,
                                    TransformationStrategy transformationStrategy) {
        this(nodes, partitioningStrategy, distributionStrategy, transformationStrategy, false);
    }
    
    /**
     * Creates a new instance
     * @param nodes
     * @param partitioningStrategy
     * @param distributionStrategy
     * @param transformationStrategy
     * @param trackMemoryConsumption Handle with care. Will negatively impact execution times.
     */
    public ARXDistributedAnonymizer(int nodes,
                                    PartitioningStrategy partitioningStrategy,
                                    DistributionStrategy distributionStrategy,
                                    TransformationStrategy transformationStrategy,
                                    boolean trackMemoryConsumption) {
        this.nodes = nodes;
        this.partitioningStrategy = partitioningStrategy;
        this.distributionStrategy = distributionStrategy;
        this.transformationStrategy = transformationStrategy;
        this.trackMemoryConsumption = trackMemoryConsumption;
    }
    
    /**
     * Performs data anonymization.
     *
     * @param data The data
     * @param config The privacy config
     * @return ARXResult
     * @throws IOException
     * @throws RollbackRequiredException 
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    public ARXDistributedResult anonymize(Data data, 
                                          ARXConfiguration config) throws IOException, RollbackRequiredException, InterruptedException, ExecutionException {
        
        // Track memory consumption
        MemoryTracker memoryTracker = null;
        if (trackMemoryConsumption) {
            memoryTracker = new MemoryTracker();
        }
        
        // Store definition
        DataDefinition definition = data.getDefinition().clone();
        
        // Sanity check
        if (data.getHandle().getNumRows() < 2) {
            throw new IllegalArgumentException("Dataset must contain at least two rows");
        }
        if (data.getHandle().getNumRows() < nodes) {
            throw new IllegalArgumentException("Dataset must contain at least as many records as nodes");
        }
        
        // #########################################
        // STEP 1: PARTITIONING
        // #########################################
        
        long timePrepare = System.currentTimeMillis();
        List<DataHandle> partitions = null;
        switch (partitioningStrategy) {
        case RANDOM:
            partitions = ARXPartition.getPartitionsRandom(data, this.nodes);
            break;
        case SORTED:
            partitions = ARXPartition.getPartitionsSorted(data, this.nodes);
            break;
        }
        timePrepare = System.currentTimeMillis() - timePrepare;
        
        // #########################################
        // STEP 2: ANONYMIZATION
        // #########################################
        
        // Start time measurement
        long timeAnonymize = System.currentTimeMillis();
        
        // ##########################################
        // STEP 2a: IF GLOBAL, RETRIEVE COMMON SCHEME
        // ##########################################
        
        // Global transformation
        int[] transformation = null;
        if (!config.isPrivacyModelSpecified(EDDifferentialPrivacy.class) &&
            transformationStrategy != TransformationStrategy.LOCAL) {
            transformation = getTransformation(partitions, 
                                               config,
                                               distributionStrategy, 
                                               transformationStrategy);
        }
        
        // ###############################################
        // STEP 2b: PERFORM LOCAL OR GLOBAL TRANSFORMATION
        // ###############################################
        long timeStep2A = System.currentTimeMillis();

        // Anonymize
        List<Future<DataHandle>> futures = getAnonymization(partitions, 
                                                            config, 
                                                            distributionStrategy, 
                                                            transformation);
        
        // Wait for execution
        List<DataHandle> handles = getResults(futures);

        // ###############################################
        // STEP 3: HANDLE NON-MONOTONIC SETTINGS
        // ###############################################
        long timeStep2B = System.currentTimeMillis();
        if (!config.isPrivacyModelSpecified(EDDifferentialPrivacy.class) &&
             config.getMonotonicityOfPrivacy() != Monotonicity.FULL) {
            
            // Prepare merged dataset
            ARXDistributedResult mergedResult = new ARXDistributedResult(ARXPartition.getData(handles));
            Data merged = ARXPartition.getData(mergedResult.getOutput());
            merged.getDefinition().read(definition);
            
            // Partition sorted while keeping sure to assign records 
            // within equivalence classes to exactly one partition
            // Also removes all hierarchies
            partitions = ARXPartition.getPartitionsByClass(merged, nodes);
            
            // Fix transformation scheme: all zero
            config = config.clone();
            config.setSuppressionLimit(1d);
            transformation = new int[definition.getQuasiIdentifyingAttributes().size()];
            
            // Suppress equivalence classes
            futures = getAnonymization(partitions, config, distributionStrategy, transformation);
            
            // Wait for execution
            handles = getResults(futures);
        }
        long timeStep3 = System.currentTimeMillis() - timeStep2B;
        timeStep2B = timeStep2B - timeStep2A;
        timeStep2A = timeStep2A - timeAnonymize;
        // ###############################################
        // STEP 4: FINALIZE
        // ###############################################
        
        timeAnonymize = System.currentTimeMillis() - timeAnonymize;

        long timeQuality = System.currentTimeMillis();

        // Preparation for calculating loss:
        QualityConfiguration configuration = new QualityConfiguration();
        int[] indices = getIndicesOfQuasiIdentifiers(data.getHandle().getDefinition().getQuasiIdentifyingAttributes(), data.getHandle());
        String[][][] hierarchies = getHierarchies(data.getHandle(), indices, configuration);
        QualityDomainShare[] shares = getDomainShares(data.getHandle(), indices, hierarchies, configuration);
        int suppressedData = getSuppressed(data.getHandle());
        Groupify<TupleWrapper> groupifyData = getGroupify(data.getHandle(), indices);

        Map<String, List<Double>> qualityMetrics = new HashMap<>();
        for (DataHandle handle : handles) {
            StatisticsQuality quality = handle.getStatistics().getQualityStatistics();
            storeQuality(qualityMetrics, "AverageClassSize", quality.getAverageClassSize().getValue());
            storeQuality(qualityMetrics, "GeneralizationIntensity", quality.getGeneralizationIntensity().getArithmeticMean());
            double granularityLoss = calculateLossDirectly(data.getHandle(), handle, configuration, indices, hierarchies, shares, suppressedData, groupifyData);
            //Double granularityLossOld = calculateLossDirectly(data.getHandle(), handle);
            //System.out.println(granularityLoss + " ??? "+ granularityLossOld);
            // TODO: Fix bug described below
            // Following if-statement avoids a bug in calculation of granularity when each QI is fully suppressed
            if (Double.isNaN(granularityLoss)) {
                storeQuality(qualityMetrics, "Granularity", 0.0d);
            } else {
                storeQuality(qualityMetrics, "Granularity", granularityLoss);
            }
            storeQuality(qualityMetrics, "NumRows", handle.getNumRows());
        }
        timeQuality = System.currentTimeMillis() - timeQuality;

        // Merge
        long timePostprocess = System.currentTimeMillis();
        Data result = ARXPartition.getData(handles);
        timePostprocess =  System.currentTimeMillis() - timePostprocess;
        
        // Track memory consumption
        long maxMemory = Long.MIN_VALUE;
        if (trackMemoryConsumption) {
            maxMemory = memoryTracker.getMaxBytesUsed();
        }
        
        // Done
        return new ARXDistributedResult(result, timePrepare, timeAnonymize, timeStep2A, timeStep2B, timeStep3, timeQuality, timePostprocess, qualityMetrics, maxMemory);
    }

    QualityConfiguration configuration = new QualityConfiguration();


    private static double calculateLossDirectly(DataHandle input, DataHandle output) {
        QualityConfiguration configuration = new QualityConfiguration();
        int[] indices = getIndicesOfQuasiIdentifiers(input.getDefinition().getQuasiIdentifyingAttributes(), input);
        String[][][] hierarchies = getHierarchies(input, indices, configuration);
        QualityDomainShare[] shares = getDomainShares(input, indices, hierarchies, configuration);
        QualityModelColumnOrientedLoss qualityModelColumnOrientedLoss = new QualityModelColumnOrientedLoss(new WrappedBoolean(),new WrappedInteger(), 0, input, output,
                getSuppressed(input), getSuppressed(output), getGroupify(input, indices), getGroupify(output, indices), hierarchies, shares, indices, configuration);
        return qualityModelColumnOrientedLoss.evaluate().getGranularity();
    }

    private static double calculateLossDirectly(DataHandle input,
                                                DataHandle output,
                                                QualityConfiguration configuration,
                                                int[] indices,
                                                String[][][] hierarchies,
                                                QualityDomainShare[] shares,
                                                int suppressedInput,
                                                Groupify<TupleWrapper> groupifyInput) {
        QualityModelColumnOrientedLoss qualityModelColumnOrientedLoss = new QualityModelColumnOrientedLoss(new WrappedBoolean(),new WrappedInteger(), 0, input, output,
                suppressedInput, getSuppressed(output), groupifyInput, getGroupify(output, indices), hierarchies, shares, indices, configuration);
        return qualityModelColumnOrientedLoss.evaluate().getGranularity();
    }


    /**
     * TODO: Copied from StatisticsQuality
     */
    private static int getSuppressed(DataHandle handle) {
        int suppressed = 0;
        for (int row = 0; row < handle.getNumRows(); row++) {
            suppressed += handle.isOutlier(row) ? 1 : 0;
        }
        return suppressed;
    }

    /**
     * TODO: Copied from StatisticsQuality
     */
    private static Groupify<TupleWrapper> getGroupify(DataHandle handle, int[] indices) {

        // Prepare
        int capacity = handle.getNumRows() / 10;
        capacity = Math.max(capacity, 10);
        Groupify<TupleWrapper> groupify = new Groupify<TupleWrapper>(capacity);
        int numRows = handle.getNumRows();
        for (int row = 0; row < numRows; row++) {
            if (!handle.isOutlier(row)) {
                TupleWrapper tuple = new TupleWrapper(handle, indices, row);
                groupify.add(tuple);
            }
        }

        return groupify;
    }

    /**
     * TODO: Copied from StatisticsQuality
     */
    private static int[] getIndicesOfQuasiIdentifiers(Set<String> userdefined, DataHandle handle) {
        int[] result = new int[handle.getDefinition().getQuasiIdentifyingAttributes().size()];
        int index = 0;
        for (String qi : handle.getDefinition().getQuasiIdentifyingAttributes()) {
            if (userdefined == null || userdefined.isEmpty() || userdefined.contains(qi)) {
                result[index++] = handle.getColumnIndexOf(qi);
            }
        }
        Arrays.sort(result);
        return result;
    }

    /**
     * TODO: Copied from StatisticsQuality
     */
    private static String[][][] getHierarchies(DataHandle handle,
                                               int[] indices,
                                               QualityConfiguration config) {

        String[][][] hierarchies = new String[indices.length][][];

        // Collect hierarchies
        for (int i=0; i<indices.length; i++) {

            // Extract and store
            String attribute = handle.getAttributeName(indices[i]);
            String[][] hierarchy = handle.getDefinition().getHierarchy(attribute);

            // If not empty
            if (hierarchy != null && hierarchy.length != 0 && hierarchy[0] != null && hierarchy[0].length != 0) {

                // Clone
                hierarchies[i] = hierarchy.clone();

            } else {

                // Create trivial hierarchy
                String[] values = handle.getDistinctValues(indices[i]);
                hierarchies[i] = new String[values.length][2];
                for (int j = 0; j < hierarchies[i].length; j++) {
                    hierarchies[i][j][0] = values[j];
                    hierarchies[i][j][1] = config.getSuppressedValue();
                }
            }
        }

        // Fix hierarchy (if suppressed character is not contained in generalization hierarchy)
        for (int j=0; j<indices.length; j++) {

            // Access
            String[][] hierarchy = hierarchies[j];

            // Check if there is a problem
            Set<String> values = new HashSet<String>();
            for (int i = 0; i < hierarchy.length; i++) {
                String[] levels = hierarchy[i];
                values.add(levels[levels.length - 1]);
            }

            // There is a problem
            if (values.size() > 1) {
                for(int i = 0; i < hierarchy.length; i++) {
                    hierarchy[i] = Arrays.copyOf(hierarchy[i], hierarchy[i].length + 1);
                    hierarchy[i][hierarchy[i].length - 1] = config.getSuppressedValue();
                }
            }

            // Replace
            hierarchies[j] = hierarchy;
        }

        // Return
        return hierarchies;
    }

    /**
     * TODO: Copied from StatisticsQuality
     */
    private static QualityDomainShare[] getDomainShares(DataHandle handle,
                                                        int[] indices,
                                                        String[][][] hierarchies,
                                                        QualityConfiguration config) {

        // Prepare
        QualityDomainShare[] shares = new QualityDomainShare[indices.length];

        // Compute domain shares
        for (int i=0; i<shares.length; i++) {

            try {

                // Extract info
                String[][] hierarchy = hierarchies[i];
                String attribute = handle.getAttributeName(indices[i]);
                HierarchyBuilder<?> builder = handle.getDefinition().getHierarchyBuilder(attribute);

                // Create shares for redaction-based hierarchies
                if ((builder instanceof HierarchyBuilderRedactionBased) &&
                        ((HierarchyBuilderRedactionBased<?>) builder).isDomainPropertiesAvailable()){
                    shares[i] = new QualityDomainShareRedaction((HierarchyBuilderRedactionBased<?>)builder);

                    // Create fallback-shares for materialized hierarchies
                    // TODO: Interval-based hierarchies are currently not compatible
                } else {
                    shares[i] = new QualityDomainShareRaw(hierarchy, config.getSuppressedValue());
                }

            } catch (Exception e) {
                // Ignore silently
                shares[i] = null;
            }
        }

        // Return
        return shares;
    }

    /**
     * Aonymizes the partitions
     * @param partitions
     * @param config
     * @param distributionStrategy2
     * @param transformation
     * @return
     * @throws RollbackRequiredException 
     * @throws IOException 
     */
    private List<Future<DataHandle>> getAnonymization(List<DataHandle> partitions,
                                                      ARXConfiguration config,
                                                      DistributionStrategy distributionStrategy2,
                                                      int[] transformation) throws IOException, RollbackRequiredException {
        List<Future<DataHandle>> futures = new ArrayList<>();
        for (DataHandle partition : partitions) {
            switch (distributionStrategy) {
            case LOCAL:
                if (transformation != null) {
                    
                    // Get handle
                    Set<String> quasiIdentifiers = partition.getDefinition().getQuasiIdentifyingAttributes();
                    
                    // Fix transformation levels
                    int count = 0;
                    for (int column = 0; column < partition.getNumColumns(); column++) {
                        String attribute = partition.getAttributeName(column);
                        if (quasiIdentifiers.contains(attribute)) {
                            int level = transformation[count];
                            partition.getDefinition().setMinimumGeneralization(attribute, level);
                            partition.getDefinition().setMaximumGeneralization(attribute, level);
                            count++;
                        }
                    }
                    
                    futures.add(new ARXWorkerLocal().anonymize(partition, config));
                } else {
                    futures.add(new ARXWorkerLocal().anonymize(partition, config, O_MIN));
                }
                break;
            default:
                throw new IllegalStateException("Unknown distribution strategy");
            }
        }
        
        // Done
        return futures;
    }

    /**
     * Collects results from the futures
     * @param <T>
     * @param futures
     * @return
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    private <T> List<T> getResults(List<Future<T>> futures) throws InterruptedException, ExecutionException {
        ArrayList<T> results = new ArrayList<>();
        while (!futures.isEmpty()) {
            Iterator<Future<T>> iter = futures.iterator();
            while (iter.hasNext()) {
                Future<T> future = iter.next();
                if (future.isDone()) {
                    results.add(future.get());
                    iter.remove();
                }
            }
            Thread.sleep(WAIT_TIME);
        }
        return results;
    }
    
    /**
     * Retrieves the transformation scheme using the current strategy
     * @param partitions
     * @param config
     * @param distributionStrategy
     * @param transformationStrategy
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private int[] getTransformation(List<DataHandle> partitions, ARXConfiguration config, DistributionStrategy distributionStrategy, TransformationStrategy transformationStrategy) throws IOException, InterruptedException, ExecutionException {
        
        // Calculate schemes
        List<Future<int[]>> futures = new ArrayList<>();
        for (DataHandle partition : partitions) {
            switch (distributionStrategy) {
            case LOCAL:
                futures.add(new ARXWorkerLocal().transform(partition, config));
                break;
            default:
                throw new IllegalStateException("Unknown distribution strategy");
            }
            
        }

        // Collect schemes
        List<int[]> schemes = getResults(futures);
        
        // Apply strategy
        switch (transformationStrategy) {
            case GLOBAL_AVERAGE:
                // Sum up all levels
                int[] result = new int[schemes.get(0).length];
                for (int[] scheme : schemes) {
                    for (int i=0; i < result.length; i++) {
                        result[i] += scheme[i];
                    }
                }
                // Divide by number of levels
                for (int i=0; i < result.length; i++) {
                    result[i] = (int)Math.round((double)result[i] / (double)schemes.size());
                }
                return result;
            case GLOBAL_MINIMUM:
                // Find minimum levels
                result = new int[schemes.get(0).length];
                Arrays.fill(result, Integer.MAX_VALUE);
                for (int[] scheme : schemes) {
                    for (int i=0; i < result.length; i++) {
                        result[i] = Math.min(result[i], scheme[i]);
                    }
                }
                return result;
            case LOCAL:
                throw new IllegalStateException("Must not be executed when doing global transformation");
            default:
                throw new IllegalStateException("Unknown transformation strategy");
        }
    }

    /**
     * Store metrics
     * @param map
     * @param label
     * @param value
     */
    private void storeQuality(Map<String, List<Double>> map, String label, double value) {
        if (!map.containsKey(label)) {
            map.put(label, new ArrayList<Double>());
        }
        map.get(label).add(value);
    }
}
