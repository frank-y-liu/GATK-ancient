package org.broadinstitute.hellbender.tools.spark.bwa;

import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;

@CommandLineProgramProperties(summary = "Runs BWA",
        oneLineSummary = "BWA on Spark",
        programGroup = SparkProgramGroup.class)
public final class BwaSpark extends GATKSparkTool {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean requiresReads() { return false; }

    @Override
    protected void runTool(final JavaSparkContext ctx) {
        System.out.println("hello");
    }
}
