package org.broadinstitute.hellbender.tools.spark.pipelines.metrics;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.reference.ReferenceSequenceFileWalker;
import htsjdk.samtools.util.CloserUtil;
import htsjdk.samtools.util.IOUtil;
import htsjdk.samtools.util.SequenceUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.PicardCommandLineProgram;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.spark.sv.InsertSizeMetricsCollectorSpark;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.runtime.ProgressLogger;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

/**
 *
 * WIP base class for individual Spark metrics collector tools, which are specializations
 * of GATKSparkTool that implement the SamMetricsCollectorSpark interface.
 *
 */
public abstract class SamMetricsCollectorToolSpark<INPUT_PARAMS, OUTPUT_METRICS>
        extends GATKSparkTool
        implements SamMetricsCollectorSpark<INPUT_PARAMS, OUTPUT_METRICS> {

    private static final long serialVersionUID = 1L;

    @Argument(fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, doc = "File to write the output to.")
    public File OUTPUT;

    /**
     * The runTool method used when the derived metrics collector tool is run
     * "standalone". The filtered RDD is passed the doMetricsCollection
     * method which does the bulk of the analysis. When a metrics collector is
     * run on behalf of CollectMultipleMetricsSpark, this will bypassed  and
     * doMetricsCollection will be passed the input RDD obtained by
     * CollectMultipleMetricsSpark.
     *
     * @param ctx our Spark context
     */
    @Override
    protected void runTool( JavaSparkContext ctx ) {
        final JavaRDD<GATKRead> filteredReads = getReads();
        doMetricsCollection(this, filteredReads);
    }

    public void doMetricsCollection(GATKSparkTool containerTool, JavaRDD<GATKRead> filteredReads) {
        INPUT_PARAMS input = getMetricsParams(containerTool, filteredReads);
        OUTPUT_METRICS output = collectMetrics(containerTool, filteredReads, input);
        saveMetrics(containerTool, filteredReads, input, output);
    }
}
