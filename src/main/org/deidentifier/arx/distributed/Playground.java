package org.deidentifier.arx.distributed;

import org.deidentifier.arx.*;
import org.deidentifier.arx.aggregates.HierarchyBuilder;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased;
import org.deidentifier.arx.aggregates.StatisticsQuality;
import org.deidentifier.arx.aggregates.quality.*;
import org.deidentifier.arx.common.Groupify;
import org.deidentifier.arx.common.TupleWrapper;
import org.deidentifier.arx.common.WrappedBoolean;
import org.deidentifier.arx.common.WrappedInteger;
import org.deidentifier.arx.criteria.KAnonymity;
import org.deidentifier.arx.exceptions.RollbackRequiredException;
import org.deidentifier.arx.metric.Metric;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class Playground {

    public static void main(String[] args) throws IOException, RollbackRequiredException, InterruptedException, ExecutionException {
        Data input = Data.create("data/mintest1.csv", Charset.defaultCharset());
        input.getDefinition().setDataType("KLASSE", DataType.STRING);
        input.getDefinition().setHierarchy("KLASSE", AttributeType.Hierarchy.create("data/mintest_KLASSE.csv", Charset.defaultCharset(), ';'));
        input.getDefinition().setAttributeType("KLASSE", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        input.getDefinition().setHierarchy("AGE", AttributeType.Hierarchy.create("data/mintest_AGE.csv", Charset.defaultCharset(), ';'));
        input.getDefinition().setAttributeType("AGE", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        Data output = Data.create("data/mintest2.csv", Charset.defaultCharset());
        output.getDefinition().setDataType("KLASSE", DataType.STRING);
        output.getDefinition().setHierarchy("KLASSE", AttributeType.Hierarchy.create("data/mintest_KLASSE.csv", Charset.defaultCharset(), ';'));
        output.getDefinition().setAttributeType("KLASSE", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        input.getDefinition().setHierarchy("AGE", AttributeType.Hierarchy.create("data/mintest_AGE.csv", Charset.defaultCharset(), ';'));
        input.getDefinition().setAttributeType("AGE", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);


        // Test anonymize
        ARXConfiguration config = ARXConfiguration.create();
        //config.setQualityModel(Metric.createLossMetric(0.05d));
        config.addPrivacyModel(new KAnonymity(2));
        config.setSuppressionLimit(0d);
        config.setAlgorithm(ARXConfiguration.AnonymizationAlgorithm.BEST_EFFORT_TOP_DOWN);

        // Anonymize
        ARXAnonymizer anonymizer = new ARXAnonymizer();
        ARXResult result;
        try {
            result = anonymizer.anonymize(input, config);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        DataHandle output1 = result.getOutput();
        output1.save("data/mintest_result.csv");
        StatisticsQuality qualityStatistics = output1.getStatistics().getQualityStatistics();
        System.out.println("Anonymization Granularity: " + qualityStatistics.getGranularity().getGranularity());
        System.out.println(qualityStatistics.getGranularity().getGeometricMean());


        calculateLossDirectly(input.getHandle(), output.getHandle());
    }




    private static int calculateLossDirectly(DataHandle input, DataHandle output) {
        QualityConfiguration configuration = new QualityConfiguration();
        int[] indices = getIndicesOfQuasiIdentifiers(input.getDefinition().getQuasiIdentifyingAttributes(), input);
        String[][][] hierarchies = getHierarchies(input, indices, configuration);
        QualityDomainShare[] shares = getDomainShares(input, indices, hierarchies, configuration);
        QualityModelColumnOrientedLoss qualityModelColumnOrientedLoss = new QualityModelColumnOrientedLoss(new WrappedBoolean(),new WrappedInteger(), 0, input, output,
                getSuppressed(input), getSuppressed(output), getGroupify(input, indices), getGroupify(output, indices), hierarchies, shares, indices, configuration);
        System.out.println(qualityModelColumnOrientedLoss.evaluate().getArithmeticMean());
        //new StatisticsQuality(data, anonymize, ARXConfiguration.create(), new WrappedBoolean(), new WrappedInteger(), data.getDefinition().getQuasiIdentifyingAttributes());
        return 1;

    }

    // Everything below here is copy and paste from some internal ARX classes:

    private static int getSuppressed(DataHandle handle) {
        int suppressed = 0;
        for (int row = 0; row < handle.getNumRows(); row++) {
            suppressed += handle.isOutlier(row) ? 1 : 0;
        }
        return suppressed;
    }


    /**
     * Returns a groupified version of the dataset
     *
     * @param handle
     * @param indices
     * @return
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
     * Returns indices of quasi-identifiers
     * @param userdefined
     *
     * @param handle
     * @return
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
     * Returns hierarchies, creates trivial hierarchies if no hierarchy is found.
     * Adds an additional level, if there is no root node
     *
     * @param handle
     * @param indices
     * @param config
     * @return
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
     * Returns domain shares for the handle
     * @param handle
     * @param indices
     * @param hierarchies
     * @param config
     * @return
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
                if (builder != null && (builder instanceof HierarchyBuilderRedactionBased) &&
                        ((HierarchyBuilderRedactionBased<?>)builder).isDomainPropertiesAvailable()){
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

}
