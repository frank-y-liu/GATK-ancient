package org.broadinstitute.hellbender.tools.spark.pipelines.metrics;

import htsjdk.samtools.util.CollectionUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.PicardCommandLineProgram;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.QCProgramGroup;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.tools.picard.analysis.*;
import org.broadinstitute.hellbender.tools.spark.sv.InsertSizeMetricsCollectorSpark;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

// WIP implementation of CollectMultipleMetricsSpark.

// TODO: need a way to pass arguments in to the individual collectors

/**
 * Class that is designed to instantiate and execute multiple metrics programs that extend
 * SinglePassSamProgram while making only a single pass through the SAM file and supplying
 * each program with the records as it goes.
 *
 */
@CommandLineProgramProperties(
        summary = "Takes an input SAM/BAM file and reference sequence and runs one or more Picard " +
                "metrics modules at the same time to cut down on I/O. Currently all programs are run with " +
                "default options and fixed output extesions, but this may become more flexible in future.",
        oneLineSummary = "A \"meta-metrics\" calculating program that produces multiple metrics for the provided SAM/BAM file",
        programGroup = QCProgramGroup.class
)
public final class CollectMultipleMetricsSpark extends GATKSparkTool {

    private static final long serialVersionUID = 1L;

    /**
     * This interface allows developers to create Programs to run in addition to the ones defined in the Program enum.
     */
    public static interface ProgramInterface {
        SamMetricsCollectorToolSpark<Object, InsertSizeMetricsCollectorSpark> makeInstance(final String outbase);
    }

    public static enum Program implements ProgramInterface {
        // Since there is currently only one collector implemented as a SamMetricsCollectorTool
        // (CollectInsertSizeMetricsCollector), run it twice in order to test/simulate running multiple
        // collectors on the same RDD.
        CollectInsertSizeMetricsCollector {
            @Override
            public SamMetricsCollectorToolSpark<Object, InsertSizeMetricsCollectorSpark> makeInstance(final String outbase) {
                final org.broadinstitute.hellbender.tools.spark.pipelines.metrics.CollectInsertSizeMetricsCollectorSpark
                        program = new CollectInsertSizeMetricsCollectorSpark();
                //TODO: we need to disambiguate the output names used by each collector will
                // but not yet
                //program.OUTPUT = new File(outbase + ".alignment_summary_metrics");
                program.OUTPUT = new File(outbase);
                program.HISTOGRAM_PLOT_FILE = outbase + ".pdf";
                program.useEnd = CollectInsertSizeMetricsCollectorSpark.UseEnd.SECOND;
                return program;
            }
        },
        CollectInsertSizeMetricsCollector2 {
            @Override
            public SamMetricsCollectorToolSpark<Object, InsertSizeMetricsCollectorSpark> makeInstance(final String outbase) {
                final org.broadinstitute.hellbender.tools.spark.pipelines.metrics.CollectInsertSizeMetricsCollectorSpark
                        program = new CollectInsertSizeMetricsCollectorSpark();
                //TODO: we need to disambiguate the output names used by each collector will
                // but not yet
                //program.OUTPUT = new File(outbase + ".alignment_summary_metrics");
                program.OUTPUT = new File(outbase);
                program.HISTOGRAM_PLOT_FILE = outbase + ".pdf";
                program.useEnd = CollectInsertSizeMetricsCollectorSpark.UseEnd.SECOND;
                return program;
            }
        }
    }

    @Argument(doc = "If true (default), then the sort order in the header file will be ignored.",
            shortName = StandardArgumentDefinitions.ASSUME_SORTED_SHORT_NAME)
    public boolean ASSUME_SORTED = true;

    @Argument(doc = "Stop after processing N reads, mainly for debugging.")
    public int STOP_AFTER = 0;

    @Argument(fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            doc = "Base name of output files.")
    public String OUTPUT;

    @Argument(doc = "List of metrics programs to apply during the pass through the SAM file.")
    public List<Program> PROGRAM = CollectionUtil.makeList(Program.values());

    /**
     * Contents of PROGRAM list is transferred to this list during command-line validation, so that an outside
     * developer can invoke this class programmatically and provide alternative Programs to run by calling
     * setProgramsToRun().
     */
    private List<ProgramInterface> programsToRun;

    @Override
    protected String[] customCommandLineValidation() {
        programsToRun = new ArrayList<>(PROGRAM);
        return super.customCommandLineValidation();
    }

    /**
     * Use this method when invoking CollectMultipleMetrics programmatically to run programs other than the ones
     * available via enum.  This must be called before doWork().
     */
    public void setProgramsToRun(List<ProgramInterface> programsToRun) {
        this.programsToRun = programsToRun;
    }

    @Override
    protected void runTool( JavaSparkContext ctx ) {
        JavaRDD<GATKRead> unFilteredReads = getUnfilteredReads();
        if (programsToRun.size() > 0) {
            // if there is more than one program to run, cache the unfiltered RDD so we don't
            // recompute it for each collector
            unFilteredReads = unFilteredReads.cache();
        }

        // delegate entire process of metrics collection to each tool in turn
        for (ProgramInterface program : programsToRun) {
            SamMetricsCollectorToolSpark<Object, InsertSizeMetricsCollectorSpark> instance = program.makeInstance(OUTPUT);

            // use the read filter provided by the collector instance
            ReadFilter readFilter = instance.getReadFilter(this);
            instance.doMetricsCollection(this, unFilteredReads.filter(r -> readFilter.test(r)));
        }
    }

}
