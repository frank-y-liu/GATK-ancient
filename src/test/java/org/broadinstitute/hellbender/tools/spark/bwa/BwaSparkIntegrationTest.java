package org.broadinstitute.hellbender.tools.spark.bwa;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.utils.test.ArgumentsBuilder;
import org.broadinstitute.hellbender.utils.test.SamAssertionUtils;
import org.testng.annotations.Test;

import java.io.File;

public final class BwaSparkIntegrationTest extends CommandLineProgramTest {

    @Override
    public String getTestedClassName() {
        return BwaSpark.class.getSimpleName();
    }

    @Test
    public void test() throws Exception {
        final File expectedSam = new File(getTestDataDir(), "bwa.sam");

        final File ref = new File("/Users/tom/workspace/jbwa/test/ref.fa");
        final File fq1 = new File("/Users/tom/workspace/jbwa/test/R1.fq");
        final File fq2 = new File("/Users/tom/workspace/jbwa/test/R1.fq");
        final File output = new File("/tmp/bwa.sam"); //createTempFile("bwa", ".bam");
        ArgumentsBuilder args = new ArgumentsBuilder();
        args.addFileArgument("ref", ref);
        args.addFileArgument("fq1", fq1);
        args.addFileArgument("fq2", fq2);
        args.add("numReducers=1");
        args.addOutput(output);
        this.runCommandLine(args.getArgsArray());

        SamAssertionUtils.assertSamsEqual(output, expectedSam);
    }

}
