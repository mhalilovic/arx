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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.deidentifier.arx.Data;
import org.deidentifier.arx.DataHandle;

public class ARXDistributedResult {
    
    /** Quality metrics */
    private Map<String, List<Double>> qualityMetrics = new HashMap<>();
    /** Data */
    private Data                      data;
    /** Timing */
    private long                      timePrepare;
    /** Timing */
    private long                      timeAnonymize;
    /** Timing */
    private long                      timeStep2A;
    /** Timing */
    private long                      timeStep2B;
    /** Timing */
    private long                      timeStep3;
    /** Timing */
    private long                      timeQuality;
    /** Timing */
    private long                      timePostprocess;
    /** Max memory consumption */
    private long                      maxMemoryConsumption = Long.MIN_VALUE;

    /**
     * Creates a new instance
     * 
     * @param data
     */
    public ARXDistributedResult(Data data) {
        this(data, 0, 0, 0, 0, 0, 0, 0,null, Long.MIN_VALUE);
    }

    /**
     * Creates a new instance
     * @param data
     * @param timePrepare
     * @param timeAnonymize
     * @param timeStep2A
     * @param timeStep2B
     * @param timeStep3
     * @param timeQuality
     * @param timePostprocess
     */
    public ARXDistributedResult(Data data, 
                                long timePrepare, 
                                long timeAnonymize,
                                long timeStep2A,
                                long timeStep2B,
                                long timeStep3,
                                long timeQuality,
                                long timePostprocess) {
        this(data, timePrepare, timeAnonymize, timeStep2A, timeStep2B, timeStep3, timeQuality, timePostprocess, null, Long.MIN_VALUE);
    }

    /**
     * Creates a new instance
     * @param data
     * @param timePrepare
     * @param timeAnonymize
     * @param timeStep2A
     * @param timeStep2B
     * @param timeStep3
     * @param timeQuality
     * @param timePostprocess
     * @param qualityMetrics
     * @param maxMemoryConsumption
     */
    public ARXDistributedResult(Data data, 
                                long timePrepare, 
                                long timeAnonymize,
                                long timeStep2A,
                                long timeStep2B,
                                long timeStep3,
                                long timeQuality,
                                long timePostprocess,
                                Map<String, List<Double>> qualityMetrics,
                                long maxMemoryConsumption) {
        
        // Store
        this.timePrepare = timePrepare;
        this.timeAnonymize = timeAnonymize;
        this.timeStep2A = timeStep2A;
        this.timeStep2B = timeStep2B;
        this.timeStep3 = timeStep3;
        this.timeQuality = timeQuality;
        this.timePostprocess = timePostprocess;
        this.maxMemoryConsumption = maxMemoryConsumption;
        this.data = data;
        
        // Collect statistics
        if (qualityMetrics != null) {
            this.qualityMetrics.putAll(qualityMetrics);
        }
        
        // Done
        timePostprocess = System.currentTimeMillis() - timePostprocess;
    }
    
    /**
     * Returns the maximum memory consumed in bytes
     * @return the max memory consumed in bytes
     */
    public long getMaxMemoryConsumption() {
        return maxMemoryConsumption;
    }

    /**
     * Returns a handle to the data obtained by applying the optimal transformation. This method will fork the buffer, 
     * allowing to obtain multiple handles to different representations of the data set. Note that only one instance can
     * be obtained for each transformation.
     * 
     * @return
     */
    public DataHandle getOutput() {
        return data.getHandle();
    }
    
    
    /**
     * Returns quality estimates
     * @return
     */
    public Map<String, List<Double>> getQuality() {
        return qualityMetrics;
    }

    /**
     * Returns the time needed for anonymization
     * @return the timeAnonymize
     */
    public long getTimeAnonymize() {
        return timeAnonymize;
    }

    /**
     * Returns the time needed for step 2A of anonymization
     * @return the timeAnonymize
     */
    public long getTimeStep2A() {
        return timeStep2A;
    }

    /**
     * Returns the time needed for step 2B of anonymization
     * @return the timeAnonymize
     */
    public long getTimeStep2B() {
        return timeStep2B;
    }

    /**
     * Returns the time needed for step 3 of anonymization
     * @return the timePostprocess
     */
    public double getTimeStep3() {
        return timeStep3;
    }

    /**
     * Returns the time needed for step timeQuality of anonymization
     * @return the timePostprocess
     */
    public double getTimeQuality() {
        return timeQuality;
    }

    /**
     * Returns the time needed for postprocessing
     * @return the timePostprocess
     */
    public long getTimePostprocess() {
        return timePostprocess;
    }

    /**
     * Returns the time needed for preparation
     * @return the timePrepare
     */
    public long getTimePrepare() {
        return timePrepare;
    }
    
    /**
     * Returns whether max memory measurement is available
     * @return
     */
    public boolean isMaxMemoryAvailable() {
        return maxMemoryConsumption != Long.MIN_VALUE;
    }
}
