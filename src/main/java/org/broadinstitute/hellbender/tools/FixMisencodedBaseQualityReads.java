package org.broadinstitute.hellbender.tools;

import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.ReadProgramGroup;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.ReadWalker;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.transformers.MisencodedBaseQualityReadTransformer;
import org.broadinstitute.hellbender.transformers.ReadTransformer;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.SAMFileGATKReadWriter;

import java.io.File;

@CommandLineProgramProperties(
	summary = "Fixes Illumina base quality scores in a SAM/BAM/CRAM file",
    oneLineSummary = "Fix Illumina base quality scores in a SAM/BAM/CRAM file",
    programGroup = ReadProgramGroup.class
)
public final class FixMisencodedBaseQualityReads extends ReadWalker {

    @Argument(fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, doc="Write output to this file")
    public File OUTPUT;

    private SAMFileGATKReadWriter outputWriter;

    private ReadTransformer transform;

    @Override
    public void onTraversalStart() {
        outputWriter = createSAMWriter(OUTPUT, true);
        transform = new MisencodedBaseQualityReadTransformer();
    }

    @Override
    public void apply( GATKRead read, ReferenceContext referenceContext, FeatureContext featureContext ) {
        outputWriter.addRead(transform.apply(read));
    }

    @Override
    public void onTraversalDone() {
        if ( outputWriter != null ) {
            outputWriter.close();
        }
    }
}
