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

import java.util.*;

import org.deidentifier.arx.Data;
import org.deidentifier.arx.DataDefinition;
import org.deidentifier.arx.DataHandle;


/**
 * Class providing operations for partitions
 * @author Fabian Prasser
 *
 */
public class ARXPartition {

    /** Data*/
    private final DataHandle data;
    /** Subset*/
    private final int[] subset;

    /**
     * Creates a new instance
     * @param handle
     * @param subset
     */
    public ARXPartition(DataHandle handle, int[] subset) {
        this.data = handle;
        this.subset = subset;
    }

    /**
     * Returns the data
     * @return
     */
    public DataHandle getData() {
        return data;
    }

    /**
     * Returns the subset, if any
     * @return
     */
    public int[] getSubset() {
        return subset;
    }

    /** Random */
    private static Random random = new Random();

    public static void setRandomSeed(long seed) {
        random = new Random(seed);
    }


    /**
     * Converts handle to data
     * @param handle
     * @return
     */
    public static Data getData(DataHandle handle) {
        // TODO: Ugly that this is needed, because it is costly
        Data data = Data.create(handle.iterator());
        data.getDefinition().read(handle.getDefinition());
        return data;
    }
    
    /**
     * Merges several handles
     * @param handles
     * @return
     */
    public static Data getData(List<DataHandle> handles) {
        // TODO: Ugly that this is needed, because it is costly
        List<Iterator<String[]>> iterators = new ArrayList<>();
        for (DataHandle handle : handles) {
            Iterator<String[]> iterator = handle.iterator();
            if (!iterators.isEmpty()) {
                // Skip header
                iterator.next();
            }
            iterators.add(iterator);
        }
        return Data.create(new CombinedIterator<String[]>(iterators));
    }

    /**
     * Splits a given dataset into a specified number of partitions, ensuring that each partition
     * is within the same equivalence class based on quasi-identifying attributes. Each partition will have
     * the same header as the original file.
     *
     * @param data           The dataset to be partitioned. (Expected to have a header)
     * @param subsetIndices  A sorted array of indices specifying a subset of the data for partitioning.
     * @param number         The number of partitions to create.
     * @return               A list of {@link ARXPartition} objects representing the divided partitions.
     */
    public static List<ARXPartition> getPartitionsByClass(Data data, int[] subsetIndices, int number) {
        DataHandle handle = data.getHandle();

        DataDefinition definition = data.getDefinition().clone();
        Set<String> qi = handle.getDefinition().getQuasiIdentifyingAttributes();
        for (String attribute : qi) {
            definition.resetHierarchy(attribute);
        }

        int[] qiIndices = createIndexArray(handle, qi);

        // Shuffle
        List<String[]> markedRows = sortData(handle, subsetIndices, qiIndices);

        // Prepare
        List<ARXPartition> result = new ArrayList<>();

        // Split
        Iterator<String[]> iter = markedRows.iterator();
        String[] header = iter.next();
        header = Arrays.copyOf(header, header.length - 1); // Remove the marker

        int size = (int)Math.floor((double)handle.getNumRows() / (double)number);
        int partitionStartIndex = 0; // To track the start of each partition

        while (iter.hasNext()) {
            // Build this partition
            List<String[]> partitionData = new ArrayList<>();
            List<Integer> partitionSubsetIndices = new ArrayList<>();
            partitionData.add(header);
            int partitionEndIndex = partitionStartIndex; // To track the end of each partition

            String[] current = iter.next();
            String[] next = iter.hasNext() ? iter.next() : null;

            // Loop while too small or in same equivalence class
            int j = 0;
            while (current != null && (partitionData.size() < size + 1 || equals(current, next, qiIndices))) {
                // Add
                if ("1".equalsIgnoreCase(current[current.length - 1])){
                    partitionSubsetIndices.add(j-partitionStartIndex);
                }
                partitionData.add(current);
                partitionEndIndex++;

                // Proceed
                current = next;
                next = iter.hasNext() ? iter.next() : null;
                j++;
            }
            
            // Add to partitions
            Data _data = Data.create(partitionData);
            _data.getDefinition().read(definition.clone());
            DataHandle _handle = _data.getHandle();
            int[] partitionSubsetArray = partitionSubsetIndices.stream().mapToInt(Integer::intValue).toArray();
            result.add(new ARXPartition(_handle, partitionSubsetArray));

            // Update the start index for the next partition
            partitionStartIndex = partitionEndIndex;
        }

        // Done
        return result;
    }

    private static int[] createIndexArray(DataHandle handle, Set<String> qi) {
        // Collect indices to use for sorting
        int[] indices = new int[qi.size()];
        int num = 0;
        for (int column = 0; column < handle.getNumColumns(); column++) {
            if (qi.contains(handle.getAttributeName(column))) {
                indices[num++] = column;
            }
        }
        return indices;
    }

    /**
     * Randomly splits a dataset evenly into a specified number of partitions, ensuring that each partition
     * includes the original header. The method also considers a subset of indices to track those rows
     * across the partitions.
     *
     * @param data           The dataset to be partitioned.
     * @param subsetIndices  A  sorted array of indices specifying a subset of the data for tracking.
     * @param number         The number of partitions to create.
     * @return               A list of {@link ARXPartition} objects representing the randomly divided partitions,
     *                       each including the original header.
     */
    public static List<ARXPartition> getPartitionsRandom(Data data, int[] subsetIndices, int number) {
        DataHandle handle = data.getHandle();

        // Shuffle
        List<String[]> markedRows = shuffleData(handle, subsetIndices);
        return createPartitions(markedRows, handle.getDefinition(), number);
    }

    /**
     * Splits a dataset into a specified number of partitions, ensuring that each partition
     * includes the original header and is evenly distributed based on the total number of rows.
     * The dataset is sorted before the splitting.
     *
     * @param data           The dataset to be partitioned.
     * @param subsetIndices  An array of indices specifying a subset that is used during anonymization
     * @param number         The number of partitions to create.
     * @return               A list of {@link ARXPartition} objects representing the divided partitions,
     *                       each including the original header.
     */
    public static List<ARXPartition> getPartitionsSorted(Data data, int[] subsetIndices, int number) {
        DataHandle handle = data.getHandle();

        // Collect indices to use for sorting
        Set<String> qi = handle.getDefinition().getQuasiIdentifyingAttributes();
        int[] qiIndices = createIndexArray(handle, qi);

        // Sort
        List<String[]> markedRows = sortData(handle, subsetIndices, qiIndices);
        return createPartitions(markedRows, handle.getDefinition(), number);
    }

    /**
     * Splits a list of marked rows into a specified number of partitions, creating ARXPartition objects.
     * This method also handles the removal of markers from the rows and includes the header in each partition.
     *
     * @param markedRows      The list of data rows, each extended with a marker indicating whether it's part of the subset.
     * @param definition      The DataDefinition object cloned from the original data, used for creating new Data objects.
     * @param number          The number of partitions to create from the markedRows.
     * @return                A list of {@link ARXPartition} objects representing the divided partitions.
     *                        Each partition includes the original header and a subset of the data rows.
     */
    private static List<ARXPartition> createPartitions(List<String[]> markedRows, DataDefinition definition, int number) {
        List<ARXPartition> result = new ArrayList<>();

        String[] header = markedRows.remove(0);
        header = Arrays.copyOf(header, header.length - 1); // Remove the marker

        // Split into partitions
        double size = (double) markedRows.size() / (double) number;
        for (int i = 0; i < number; i++) {
            int start = (int) Math.round(i * size);
            int end = (int) Math.round((i + 1) * size);
            List<String[]> partitionData = new ArrayList<>();
            List<Integer> partitionSubsetIndices = new ArrayList<>();

            partitionData.add(header); // Add header to each partition

            for (int j = start; j<Math.min(end, markedRows.size()); j++) {
                String[] row = markedRows.get(j);
                if ("1".equalsIgnoreCase(row[row.length - 1])){
                    partitionSubsetIndices.add(j-start);
                }
                partitionData.add(Arrays.copyOf(row, row.length - 1));
            }

            // Create ARXPartition and add to result
            Data _data = Data.create(partitionData);
            _data.getDefinition().read(definition.clone());
            DataHandle _handle = _data.getHandle();
            int[] partitionSubsetArray = partitionSubsetIndices.stream().mapToInt(Integer::intValue).toArray();
            result.add(new ARXPartition(_handle, partitionSubsetArray));
        }
        return result;
    }

    /**
     * Sorts the data from a DataHandle based on specified quasi-identifying indices (qiIndices)
     * after marking rows specified in subsetIndices. Resulting rows marked with 1 were part of subset.
     *
     * @param handle         The DataHandle from which data is extracted.
     * @param subsetIndices  Array of indices specifying a subset of rows to be marked.
     * @param qiIndices      Array of indices used to determine the columns for sorting the data.
     * @return               A List of String arrays representing sorted rows, including a header row.
     */
    private static List<String[]> sortData(DataHandle handle, int[] subsetIndices, int[] qiIndices) {
        List<String[]> markedRows = markData(handle, subsetIndices);

        String[] header = markedRows.remove(0);

        // Sort the rows based on qiIndices
        markedRows.sort((row1, row2) -> {
            for (int qiIndex : qiIndices) {
                int compare = row1[qiIndex].compareTo(row2[qiIndex]);
                if (compare != 0) {
                    return compare;
                }
            }
            return 0; // Rows are equal based on qiIndices
        });

        // Add the header back at the beginning of the list
        markedRows.add(0, header);

        return markedRows;
    }

    /**
     * Shuffles the data from a DataHandle after marking rows specified in subsetIndices.
     * Resulting rows marked with 1 were part of subset.
     *
     * @param handle         The DataHandle from which data is extracted.
     * @param subsetIndices  Array of indices specifying a subset of rows to be marked.
     * @return               A List of String arrays representing shuffled rows, including a header row.
     */
    private static List<String[]> shuffleData(DataHandle handle, int[] subsetIndices) {
        List<String[]> markedRows = markData(handle, subsetIndices);

        String[] header = markedRows.remove(0);

        // Sort the rows based on qiIndices
        Collections.shuffle(markedRows, random);

        // Add the header back at the beginning of the list
        markedRows.add(0, header);

        return markedRows;
    }


    /**
     * Prepares data from a DataHandle by marking rows based on subset indices.
     * Resulting rows marked with 1 were part of subset.
     *
     * @param handle         The DataHandle from which data is extracted.
     * @param subsetIndices  Array of indices specifying a subset of rows to be marked.
     * @return               A List of String arrays representing rows, with each row extended by a marker.
     */
    private static List<String[]> markData(DataHandle handle, int[] subsetIndices) {
        // Convert subsetIndices to a set for efficient lookup
        Set<Integer> subsetIndexSet = new HashSet<>();
        for (int index : subsetIndices) {
            subsetIndexSet.add(index);
        }

        // Iterate over the data handle and create a new list of rows with markers
        List<String[]> markedRows = new ArrayList<>();
        Iterator<String[]> iterator = handle.iterator();
        String[] header = null;
        if (iterator.hasNext()) {
            header = iterator.next(); // Extract header
        }
        header = Arrays.copyOf(header, header.length + 1); // Extend row to include marker
        header[header.length - 1] = "marker";

        int rowIndex = 0;
        while (iterator.hasNext()) {
            String[] row = iterator.next();
            // Check if this row index is in subsetIndices
            String marker = subsetIndexSet.contains(rowIndex) ? "1" : "0";  // 1 corresponds to marked
            row = Arrays.copyOf(row, row.length + 1); // Extend row to include marker
            row[row.length - 1] = marker;
            markedRows.add(row);
            rowIndex++;
        }

        // Add the header back at the beginning of the list
        markedRows.add(0, header);

        return markedRows;
    }

    /**
     * Checks equality of strings regarding the given indices
     * @param array1
     * @param array2
     * @param indices
     * @return
     */
    private static boolean equals(String[] array1, String[] array2, int[] indices) {
        if (array1 == null) {
            return (array2 == null);
        }
        if (array2 == null) {
            return (array1 == null);
        }
        for (int index : indices) {
            if (!array1[index].equals(array2[index])) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Print
     * @param handle
     */
    public static void print(DataHandle handle) {
        Iterator<String[]> iterator = handle.iterator();
        System.out.println(Arrays.toString(iterator.next()));
        System.out.println(Arrays.toString(iterator.next()));
        System.out.println("- Records: " + handle.getNumRows());
    }
}