package org.broadinstitute.hellbender.tools.spark.pipelines.metrics;

import org.apache.spark.api.java.JavaRDD;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.utils.read.GATKRead;

/**
 * WIP Interface implemented by Spark SAM metrics collectors that imposes a
 * specialized tool structure that separates the analysis of the RDD from the
 * acquisition of the RDD. This allows collector tools to run either standalone
 * (i.e. CollectInsertSizeMetrics), or under the control of
 * CollectMultipleMetricsSpark, which reuses a single input RDD to run multiple
 * collectors.
 *
 * NOTE: the current implementation of CollectMultipleMetricsSpark in this branch
 * iterates through the collectors and passes each one a filtered version of the
 * input RDD to be fully processed. An alternative approach would be to process the
 * RDD directly, and pass each (filtered) record to each collector, similar to the
 * way the Picard multiple collector works, but this requires a different factoring
 * of the collectors. The former works better for collectors that want to see (and
 * cache) the entire filtered RDD before proceeding. i.e., the way the current
 * CollectInsertSizeMetricSpark collector works, but doesn't require the separate
 * get/collect/save methods as defined below; so these could be collapsed into a
 * single method (which in turn would eliminate the need for the type parameters).
 * Needs more discussion to get the factoring right.
 *
 * @param <INPUT_PARAMS>
 * @param <OUTPUT_METRICS>
 */
public interface SamMetricsCollectorSpark<INPUT_PARAMS, OUTPUT_METRICS> {
    default INPUT_PARAMS getMetricsParams(GATKSparkTool containerTool, JavaRDD<GATKRead> reads) { return null; };
    abstract OUTPUT_METRICS collectMetrics(GATKSparkTool containerTool, JavaRDD<GATKRead> reads, INPUT_PARAMS input);
    abstract void saveMetrics(GATKSparkTool containerTool, JavaRDD<GATKRead> reads, INPUT_PARAMS input, OUTPUT_METRICS results);

    // The read filter needs to be able to use the GATKSparkTool passed to it (which will
    // either be a single collector or the multiple collector, in order to access the input
    // header, read groups, etc.
    abstract ReadFilter getReadFilter(GATKSparkTool containerTool);
}
