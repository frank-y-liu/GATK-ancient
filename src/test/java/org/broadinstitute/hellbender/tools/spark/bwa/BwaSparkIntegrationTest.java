package org.broadinstitute.hellbender.tools.spark.bwa;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.utils.test.ArgumentsBuilder;
import org.testng.annotations.Test;

import java.io.File;

public final class BwaSparkIntegrationTest extends CommandLineProgramTest {

    @Override
    public String getTestedClassName() {
        return BwaSpark.class.getSimpleName();
    }

    @Test
    public void test() throws Exception {
        final File ref = new File("/Users/tom/workspace/jbwa/test/ref.fa");
        final File fasta = new File("/Users/tom/workspace/jbwa/test/R1.fq");
        final File output = createTempFile("bwa", ".bam");
        ArgumentsBuilder args = new ArgumentsBuilder();
        args.addFileArgument("ref", ref);
        args.addFileArgument("fasta", fasta);
        args.addOutput(output);
        this.runCommandLine(args.getArgsArray());
    }

}
