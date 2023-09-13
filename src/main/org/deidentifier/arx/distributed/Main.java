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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.deidentifier.arx.*;
import org.deidentifier.arx.AttributeType.Hierarchy;
import org.deidentifier.arx.aggregates.StatisticsFrequencyDistribution;
import org.deidentifier.arx.criteria.*;
import org.deidentifier.arx.distributed.ARXDistributedAnonymizer.DistributionStrategy;
import org.deidentifier.arx.distributed.ARXDistributedAnonymizer.PartitioningStrategy;
import org.deidentifier.arx.distributed.ARXDistributedAnonymizer.TransformationStrategy;
import org.deidentifier.arx.exceptions.RollbackRequiredException;
import org.deidentifier.arx.io.CSVHierarchyInput;
import org.deidentifier.arx.metric.Metric;

/**
 * Example
 *
 * @author Fabian Prasser
 */
public class Main {

    private static final int MAX_THREADS = 3;

    private static final boolean AGGREGATION = true;

    private static abstract class BenchmarkConfiguration {
        protected final String datasetName;
        protected final String sensitiveAttribute;
        private Data data;

        private BenchmarkConfiguration(String datasetName, String sensitiveAttribute) {
            this.datasetName = datasetName;
            this.sensitiveAttribute = sensitiveAttribute;
        }

        public Data getDataset(int numVariations) throws IOException {
            Data data = createData(datasetName, numVariations);
            if (sensitiveAttribute != null) {
                data.getDefinition().setAttributeType(sensitiveAttribute, AttributeType.SENSITIVE_ATTRIBUTE);
            }
            this.data = data;
            return data;
        };

        public Data getDataset() {
            if (this.data == null) {
                throw new RuntimeException("Dataset not created yet");
            }
            return this.data;
        }

        public abstract ARXConfiguration getConfig(boolean local, int threads);
        public abstract String getName();
        public String getDataName() {
            return datasetName;
        };
    }


    /**
     * Loads a dataset from disk
     * @param dataset
     * @return
     * @throws IOException
     */
    public static Data createData(final String dataset) throws IOException {
        return createData(dataset, dataset);
    }


        /**
         * Loads a dataset from disk
         * @param dataset
         * @return
         * @throws IOException
         */
    public static Data createData(final String dataset, final String datasetPath) throws IOException {

        Data data = Data.create("data/" + datasetPath + ".csv", StandardCharsets.UTF_8, ';');

        // Read generalization hierarchies
        FilenameFilter hierarchyFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.matches(dataset + "_hierarchy_(.)+.csv");
            }
        };

        // Create definition
        File testDir = new File("data/");
        File[] genHierFiles = testDir.listFiles(hierarchyFilter);
        Pattern pattern = Pattern.compile("_hierarchy_(.*?).csv");
        for (File file : genHierFiles) {
            Matcher matcher = pattern.matcher(file.getName());
            if (matcher.find()) {
                CSVHierarchyInput hier = new CSVHierarchyInput(file, StandardCharsets.UTF_8, ';');
                String attributeName = matcher.group(1);
                data.getDefinition().setAttributeType(attributeName, Hierarchy.create(hier.getHierarchy()));
                if (AGGREGATION) {
                    data.getDefinition().setMicroAggregationFunction(attributeName, AttributeType.MicroAggregationFunction.createSet(), true);
                }
                //TODO: data.getDefinition().setMicroAggregationFunction(attributeName, AttributeType.MicroAggregationFunction.createSet(), true);
            }
        }
        return data;
    }

    /**
     * Loads a dataset from disk
     * @param dataset
     * @return
     * @throws IOException
     */
    public static Data createData(final String dataset, int numberOfEnlargements) throws IOException {
        if (numberOfEnlargements < 1) {
            return createData(dataset);
        }
        // writes
        CSVVariations.createVariations("data/" + dataset + ".csv", numberOfEnlargements);
        return createData(dataset, dataset + "-enlarged-" + numberOfEnlargements);
    }

    /**
     * Entry point.
     *
     * @param args the arguments
     * @throws IOException
     * @throws RollbackRequiredException 
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws IOException, RollbackRequiredException, InterruptedException, ExecutionException {
        //playground();
        // Running multiple benchmarks will overwrite results from previous runs!
        System.out.println("You can pass following four required arguments: measureMemory?, testScalability?, datasetName, sensitiveAttribute");
        if (args.length >= 4) {
            benchmark(Boolean.parseBoolean(args[0]), Boolean.parseBoolean(args[1]), args[2], args[3]);

        } else {
            System.out.println("Using default: false, false, adult, education");
            benchmark(false, false, "adult", "education");
        }
        //benchmark(false, "ihis", "EDUC");

    }
    
    /**
     * Benchmarking
     * @param measureMemory
     * @throws IOException
     * @throws RollbackRequiredException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private static void benchmark(boolean measureMemory, boolean testScalability, String datasetName, String sensitiveAttribute) throws IOException, RollbackRequiredException, InterruptedException, ExecutionException {

        String resultFileName = "result.csv";
        if (measureMemory) {
            resultFileName = "result_memory.csv";
        }
        // Prepare output file
        BufferedWriter out = new BufferedWriter(new FileWriter(resultFileName));
        if (measureMemory) {
            out.write("Dataset;Config;Local;Sorted;Threads;Granularity;Memory;Measurements\n");
        } else {
            out.write("Dataset;Config;Local;Sorted;Threads;Granularity;Time;TimePrepare;TimeComplete;TimeAnonymize;TimeGlobalTransform;TimePartitionByClass;TimeSuppress;TimeQuality;TimePostprocess\n");
        }

        out.flush();
        
        // Prepare configs
        List<BenchmarkConfiguration> configs = new ArrayList<>();

        configs.add(new BenchmarkConfiguration(datasetName, null) {
            public String getName() {return "5-map";}
            public ARXConfiguration getConfig(boolean local, int threads) {
                ARXConfiguration config = ARXConfiguration.create();
                DataHandle handle = getDataset().getHandle();
                ARXPopulationModel populationModel = ARXPopulationModel.create(handle.getNumRows(), 0.01d);
                config.addPrivacyModel(new KMap(5, 0.01, populationModel, KMap.CellSizeEstimator.POISSON));
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        configs.add(new BenchmarkConfiguration(datasetName, null) {
            public String getName() {return "sample-uniqueness";}
            public ARXConfiguration getConfig(boolean local, int threads) {
                ARXConfiguration config = ARXConfiguration.create();
                DataHandle handle = getDataset().getHandle();
                config.addPrivacyModel(new SampleUniqueness(0.3));
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        configs.add(new BenchmarkConfiguration(datasetName, null) {
            public String getName() {return "population-uniqueness";}
            public ARXConfiguration getConfig(boolean local, int threads) {
                ARXConfiguration config = ARXConfiguration.create();
                DataHandle handle = getDataset().getHandle();
                config.addPrivacyModel(new PopulationUniqueness(0.3, ARXPopulationModel.create(ARXPopulationModel.Region.USA)));
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        configs.add(new BenchmarkConfiguration(datasetName, null) {
            public String getName() {return "profitability";}
            public ARXConfiguration getConfig(boolean local, int threads) {
                ARXConfiguration config = ARXConfiguration.create();
                DataHandle handle = getDataset().getHandle();
                config.addPrivacyModel(new ProfitabilityProsecutor());
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        configs.add(new BenchmarkConfiguration(datasetName, null) {
            public String getName() {return "5-anonymity";}
            public ARXConfiguration getConfig(boolean local, int threads) {
                ARXConfiguration config = ARXConfiguration.create();
                config.addPrivacyModel(new KAnonymity(5));
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        configs.add(new BenchmarkConfiguration(datasetName, null) {
            public String getName() {return "11-anonymity";}
            public ARXConfiguration getConfig(boolean local, int threads) {
                ARXConfiguration config = ARXConfiguration.create();
                config.addPrivacyModel(new KAnonymity(5));
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        configs.add(new BenchmarkConfiguration(datasetName, sensitiveAttribute) {
            public String getName() {return "distinct-3-diversity";}
            public ARXConfiguration getConfig(boolean local, int threads) {
                ARXConfiguration config = ARXConfiguration.create();
                config.addPrivacyModel(new DistinctLDiversity(sensitiveAttribute, 3));
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        configs.add(new BenchmarkConfiguration(datasetName, sensitiveAttribute) {
            public String getName() {return "distinct-5-diversity";}
            public ARXConfiguration getConfig(boolean local, int threads) {
                ARXConfiguration config = ARXConfiguration.create();
                config.addPrivacyModel(new DistinctLDiversity(sensitiveAttribute, 5));
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        configs.add(new BenchmarkConfiguration(datasetName, sensitiveAttribute) {
            public String getName() {return "entropy-3-diversity";}
            public ARXConfiguration getConfig(boolean local, int threads) {
                ARXConfiguration config = ARXConfiguration.create();
                config.addPrivacyModel(new EntropyLDiversity(this.sensitiveAttribute, 3));
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        configs.add(new BenchmarkConfiguration(datasetName, sensitiveAttribute) {
            public String getName() {return "entropy-5-diversity";}
            public ARXConfiguration getConfig(boolean local, int threads) {
                ARXConfiguration config = ARXConfiguration.create();
                config.addPrivacyModel(new EntropyLDiversity(this.sensitiveAttribute, 5));
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        // -------------------
        // LOCAL DISTRIBUTION
        // -------------------

        //configs.add(new BenchmarkConfiguration(datasetName, sensitiveAttribute) {
        //    public String getName() {return "0.2-equal-closeness";}
        //    public ARXConfiguration getConfig(boolean local, int threads) {
        //        ARXConfiguration config = ARXConfiguration.create();
        //        config.addPrivacyModel(new EqualDistanceTCloseness(this.sensitiveAttribute, 0.2d));
        //        config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
        //        config.setSuppressionLimit(1d);
        //        return config;
        //    }
        //});
//
        //configs.add(new BenchmarkConfiguration(datasetName, sensitiveAttribute) {
        //    public String getName() {return "0.5-equal-closeness";}
        //    public ARXConfiguration getConfig(boolean local, int threads) {
        //        ARXConfiguration config = ARXConfiguration.create();
        //        config.addPrivacyModel(new EqualDistanceTCloseness(this.sensitiveAttribute, 0.5d));
        //        config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
        //        config.setSuppressionLimit(1d);
        //        return config;
        //    }
        //});

      // -------------------
      // GLOBAL DISTRIBUTION
      // -------------------

        configs.add(new BenchmarkConfiguration(datasetName, sensitiveAttribute) {
            public String getName() {return "0.2-equal-closeness (global distribution)";}
            public ARXConfiguration getConfig(boolean local, int threads) {

                // Variable
                String VARIABLE = this.sensitiveAttribute;

                // Obtain global distribution
                StatisticsFrequencyDistribution distribution;
                DataHandle handle = getDataset().getHandle();
                int column = handle.getColumnIndexOf(VARIABLE);
                distribution = handle.getStatistics().getFrequencyDistribution(column);

                ARXConfiguration config = ARXConfiguration.create();
                config.addPrivacyModel(new EqualDistanceTCloseness(VARIABLE, 0.2d, distribution));
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        configs.add(new BenchmarkConfiguration(datasetName, sensitiveAttribute) {
            public String getName() {return "0.5-equal-closeness (global distribution)";}
            public ARXConfiguration getConfig(boolean local, int threads) {

                // Variable
                String VARIABLE = this.sensitiveAttribute;

                // Obtain global distribution
                StatisticsFrequencyDistribution distribution;
                DataHandle handle = getDataset().getHandle();
                int column = handle.getColumnIndexOf(VARIABLE);
                distribution = handle.getStatistics().getFrequencyDistribution(column);

                ARXConfiguration config = ARXConfiguration.create();
                config.addPrivacyModel(new EqualDistanceTCloseness(VARIABLE, 0.5d, distribution));
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        configs.add(new BenchmarkConfiguration(datasetName, sensitiveAttribute) {
            public String getName() {return "1-disclosure-privacy";}
            public ARXConfiguration getConfig(boolean local, int threads) {
                ARXConfiguration config = ARXConfiguration.create();
                config.addPrivacyModel(new DDisclosurePrivacy(this.sensitiveAttribute, 1));
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        configs.add(new BenchmarkConfiguration(datasetName, sensitiveAttribute) {
            public String getName() {return "2-disclosure-privacy";}
            public ARXConfiguration getConfig(boolean local, int threads) {
                ARXConfiguration config = ARXConfiguration.create();
                config.addPrivacyModel(new DDisclosurePrivacy(this.sensitiveAttribute, 2));
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        // -------------------
        // LOCAL DISTRIBUTION
        // -------------------

        //configs.add(new BenchmarkConfiguration(datasetName, sensitiveAttribute) {
        //    public String getName() {return "1-enhanced-likeness";}
        //    public ARXConfiguration getConfig(boolean local, int threads) {
        //        ARXConfiguration config = ARXConfiguration.create();
        //        config.addPrivacyModel(new EnhancedBLikeness(this.sensitiveAttribute, 1));
        //        config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
        //        config.setSuppressionLimit(1d);
        //        return config;
        //    }
        //});
//
        //configs.add(new BenchmarkConfiguration(datasetName, sensitiveAttribute) {
        //    public String getName() {return "2-enhanced-likeness";}
        //    public ARXConfiguration getConfig(boolean local, int threads) {
        //        ARXConfiguration config = ARXConfiguration.create();
        //        config.addPrivacyModel(new EnhancedBLikeness(this.sensitiveAttribute, 2));
        //        config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
        //        config.setSuppressionLimit(1d);
        //        return config;
        //    }
        //});
//
        // -------------------
        // GLOBAL DISTRIBUTION
        // -------------------

        configs.add(new BenchmarkConfiguration(datasetName, sensitiveAttribute) {
            public String getName() {return "1-enhanced-likeness (global distribution)";}
            public ARXConfiguration getConfig(boolean local, int threads) {

                // Variable
                String VARIABLE = this.sensitiveAttribute;

                // Obtain global distribution
                StatisticsFrequencyDistribution distribution;
                DataHandle handle = getDataset().getHandle();
                int column = handle.getColumnIndexOf(VARIABLE);
                distribution = handle.getStatistics().getFrequencyDistribution(column);

                ARXConfiguration config = ARXConfiguration.create();
                config.addPrivacyModel(new EnhancedBLikeness(VARIABLE, 1, distribution));
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        configs.add(new BenchmarkConfiguration(datasetName, sensitiveAttribute) {
            public String getName() {return "2-enhanced-likeness (global distribution)";}
            public ARXConfiguration getConfig(boolean local, int threads) {

                // Variable
                String VARIABLE = this.sensitiveAttribute;

                // Obtain global distribution
                StatisticsFrequencyDistribution distribution;
                DataHandle handle = getDataset().getHandle();
                int column = handle.getColumnIndexOf(VARIABLE);
                distribution = handle.getStatistics().getFrequencyDistribution(column);

                ARXConfiguration config = ARXConfiguration.create();
                config.addPrivacyModel(new EnhancedBLikeness(VARIABLE, 2, distribution));
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        configs.add(new BenchmarkConfiguration(datasetName, null) {
            public String getName() {return "0.05-average-risk";}
            public ARXConfiguration getConfig(boolean local, int threads) {
                ARXConfiguration config = ARXConfiguration.create();
                config.addPrivacyModel(new AverageReidentificationRisk(0.05d));
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        configs.add(new BenchmarkConfiguration(datasetName, null) {
            public String getName() {return "0.01-average-risk";}
            public ARXConfiguration getConfig(boolean local, int threads) {
                ARXConfiguration config = ARXConfiguration.create();
                config.addPrivacyModel(new AverageReidentificationRisk(0.01d));
                config.setQualityModel(Metric.createLossMetric(local ? 0d : 0.5d));
                config.setSuppressionLimit(1d);
                return config;
            }
        });

        //configs.add(new BenchmarkConfiguration(datasetName, null) {
        //    final double EPSILON = 1d;
        //    public String getName() {return "(e10-6, "+EPSILON+")-differential privacy";}
        //    public ARXConfiguration getConfig(boolean local, int threads) {
        //        ARXConfiguration config = ARXConfiguration.create();
        //        config.addPrivacyModel(new EDDifferentialPrivacy(EPSILON / (double) threads, 0.000001d, null, true));
        //        config.setDPSearchBudget(0.1d * (EPSILON / (double) threads));
        //        config.setHeuristicSearchStepLimit(300);
        //        config.setQualityModel(Metric.createLossMetric(0.5d));
        //        config.setSuppressionLimit(1d);
        //        return config;
        //    }
        //});
//
        //configs.add(new BenchmarkConfiguration(datasetName, null) {
        //    final double EPSILON = 2d;
        //    public String getName() {return "(e10-6, "+EPSILON+")-differential privacy";}
        //    public ARXConfiguration getConfig(boolean local, int threads) {
        //        ARXConfiguration config = ARXConfiguration.create();
        //        config.addPrivacyModel(new EDDifferentialPrivacy(EPSILON / (double) threads, 0.000001d, null, true));
        //        config.setDPSearchBudget(0.1d * (EPSILON / (double) threads));
        //        config.setHeuristicSearchStepLimit(300);
        //        config.setQualityModel(Metric.createLossMetric(0.5d));
        //        config.setSuppressionLimit(1d);
        //        return config;
        //    }
        //});
//
        //configs.add(new BenchmarkConfiguration(datasetName, null) {
        //    final double EPSILON = 3d;
        //    public String getName() {return "(e10-6, "+EPSILON+")-differential privacy";}
        //    public ARXConfiguration getConfig(boolean local, int threads) {
        //        ARXConfiguration config = ARXConfiguration.create();
        //        config.addPrivacyModel(new EDDifferentialPrivacy(EPSILON / (double) threads, 0.000001d, null, true));
        //        config.setDPSearchBudget(0.1d * (EPSILON / (double) threads));
        //        config.setHeuristicSearchStepLimit(300);
        //        config.setQualityModel(Metric.createLossMetric(0.5d));
        //        config.setSuppressionLimit(1d);
        //        return config;
        //    }
        //});
//
        //configs.add(new BenchmarkConfiguration(datasetName, null) {
        //    final double EPSILON = 4d;
        //    public String getName() {return "(e10-6, "+EPSILON+")-differential privacy";}
        //    public ARXConfiguration getConfig(boolean local, int threads) {
        //        ARXConfiguration config = ARXConfiguration.create();
        //        config.addPrivacyModel(new EDDifferentialPrivacy(EPSILON / (double) threads, 0.000001d, null, true));
        //        config.setDPSearchBudget(0.1d * (EPSILON / (double) threads));
        //        config.setHeuristicSearchStepLimit(300);
        //        config.setQualityModel(Metric.createLossMetric(0.5d));
        //        config.setSuppressionLimit(1d);
        //        return config;
        //    }
        //});

        // Configs
        System.out.println("Using " + configs.size() + " benchmark configs.");
        int benchmark_count = 0;
        for (BenchmarkConfiguration benchmark : configs) {
            benchmark_count++;
            if (testScalability) {
                int threads = MAX_THREADS;
                System.out.println("Benchmark " + benchmark_count + "/" + configs.size());
                for (int numVariations = 1; numVariations <= 50; numVariations += 5) {
                    System.out.println("Running with " + numVariations + " number of enlargements.");
                    run(benchmark, threads, false, true, out, measureMemory, numVariations);
                    if (!benchmark.getConfig(true, threads).isPrivacyModelSpecified(EDDifferentialPrivacy.class)) {
                        run(benchmark, threads, true, true, out, measureMemory, numVariations);
                    }
                }
            } else {
                System.out.println("Benchmark " + benchmark_count + "/" + configs.size());
                for (int threads = 1; threads <= MAX_THREADS; threads++) {
                    System.out.println("Running with " + threads + " threads.");
                    run(benchmark, threads, false, true, out, measureMemory, 0);
                    if (!benchmark.getConfig(true, threads).isPrivacyModelSpecified(EDDifferentialPrivacy.class)) {
                        run(benchmark, threads, true, true, out, measureMemory, 0);
                    }
                }
            }
        }
        
        // Done
        out.close();
    }
    
    /**
     * Run benchmark
     * @param threads 
     * @param local
     * @param sorted
     * @param out
     * @param measureMemory
     * @throws ExecutionException 
     * @throws InterruptedException 
     * @throws RollbackRequiredException 
     * @throws IOException 
     */
    private static void run(BenchmarkConfiguration benchmark,
                            int threads,
                            boolean local,
                            boolean sorted,
                            BufferedWriter out,
                            boolean measureMemory,
                            int numVariations) throws IOException,
                                                RollbackRequiredException,
                                                InterruptedException,
                                                ExecutionException {
        
        System.out.println("Config: " + benchmark.getDataName() + "." + benchmark.getName() + " local: " + local + (!measureMemory ? "" : " [MEMORY]"));
        
        double time = 0d;
        double timePrepare = 0d;
        double timeComplete = 0d;
        double timeAnonymize = 0d;
        double timeGlobalTransform = 0d;
        double timePartitionByClass = 0d;
        double timeSuppress = 0d;
        double timeQuality = 0d;
        double timePostprocess = 0d;
        double granularity = 0d;
        long memory = 0;
        long numberOfMemoryMeasurements = 0;
        long delay = 1000L;
        int REPEAT = 5;
        int WARMUP = 2;
        if (measureMemory) {
            REPEAT = 1;
            WARMUP = 0;
        }

        // Repeat
        for (int i = 0; i < REPEAT; i++) {
            
            // Report
            System.out.println("- Run " + (i+1) + " of " + REPEAT);
            
            // Get
            Data data;
            data = benchmark.getDataset(numVariations);

            ARXConfiguration config = benchmark.getConfig(local, threads);
            
            // Anonymize
            ARXDistributedAnonymizer anonymizer = new ARXDistributedAnonymizer(threads,
                                                                               sorted ? PartitioningStrategy.SORTED : PartitioningStrategy.RANDOM, 
                                                                               DistributionStrategy.LOCAL,
                                                                               local ? TransformationStrategy.LOCAL : TransformationStrategy.GLOBAL_AVERAGE,
                                                                               measureMemory);
            ARXDistributedResult result = anonymizer.anonymize(data, config, delay);
            memory = result.getMaxMemoryConsumption();
            numberOfMemoryMeasurements = result.getNumberOfMemoryMeasurements();
            if (measureMemory && numberOfMemoryMeasurements < 20) {
                // With default delay of 1000L we might not measure anything, because anonymization runs too quick
                // Ensure delay is small enough to get around 20 measurements during anonymization
                delay = (long) Math.floor(result.getTimeAnonymize() * 0.046);

                System.out.println("Rerunning with lower delay: " + delay);
                result = anonymizer.anonymize(data, config, delay);
                memory = result.getMaxMemoryConsumption();
                numberOfMemoryMeasurements = result.getNumberOfMemoryMeasurements();
                System.out.println("Memory: " + memory + " from " + numberOfMemoryMeasurements + "measurements");
            }
            result.getOutput().save("./" + benchmark.getName() + "_" + threads + "_" + local + ".csv");


            // First two are warmup
            if (i >= WARMUP) {
                granularity += getWeightedAverageForGranularities(result.getQuality().get("Granularity"), result.getQuality().get("NumRows"));
                time += result.getTimePrepare() + result.getTimeComplete() + result.getTimePostprocess() + result.getTimeQuality();
                timePrepare += result.getTimePrepare();
                timeComplete += result.getTimeComplete();
                timeAnonymize += result.getTimeAnonymize();
                timeGlobalTransform += result.getTimeGlobalTransform();
                timePartitionByClass += result.getTimePartitionByClass();
                timeSuppress += result.getTimeSuppress();
                timeQuality += result.getTimeQuality();
                timePostprocess += result.getTimePostprocess();
            }
        }

        // Average
        time /= REPEAT-WARMUP;
        timePrepare /= REPEAT-WARMUP;
        timeComplete /= REPEAT-WARMUP;
        timeAnonymize /= REPEAT-WARMUP;
        timeGlobalTransform /= REPEAT-WARMUP;
        timePartitionByClass /= REPEAT-WARMUP;
        timeSuppress /= REPEAT-WARMUP;
        timeQuality /= REPEAT-WARMUP;
        timePostprocess /= REPEAT-WARMUP;
        granularity /= REPEAT-WARMUP;

        // Store
        if (numVariations > 0) {
            out.write(benchmark.getDataName() + "-enlarged-" + numVariations + ";");
        } else {
            out.write(benchmark.getDataName() + ";");
        }
        out.write(benchmark.getName() + ";");
        out.write(local + ";");
        out.write(sorted + ";");
        out.write(threads + ";");
        out.write(granularity + ";");
        if (measureMemory) {
            out.write(memory + ";");
            out.write(numberOfMemoryMeasurements + "\n");
        } else {
            out.write(time + ";");
            out.write(timePrepare + ";");
            out.write(timeComplete + ";");
            out.write(timeAnonymize + ";");
            out.write(timeGlobalTransform + ";");
            out.write(timePartitionByClass + ";");
            out.write(timeSuppress + ";");
            out.write(timeQuality + ";");
            out.write(timePostprocess + "\n");
        }
        out.flush();
    }

    /**
     * Returns the average of the given values
     * @param values
     * @return
     */
    private static double getAverage(List<Double> values) {
        double result = 0d;
        for (Double value : values) {
            result += value;
        }
        return result / (double)values.size();
    }

    /**
     * Returns the weighted average of the given values
     * @param values
     * @param weights
     * @return
     */
    private static double getWeightedAverageForGranularities(List<Double> values, List<Double> weights) {
        double result = 0d;
        double weightSum = 0d;
        for (int i = 0; i<values.size(); i++) {
            result += values.get(i) * weights.get(i);
            weightSum += weights.get(i);
        }
        return result / weightSum;
    }

}
