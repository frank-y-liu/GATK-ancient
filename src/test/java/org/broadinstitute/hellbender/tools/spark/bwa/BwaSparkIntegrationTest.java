package org.broadinstitute.hellbender.tools.spark.bwa;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.utils.test.ArgumentsBuilder;
import org.testng.annotations.Test;

public final class BwaSparkIntegrationTest extends CommandLineProgramTest {

    @Override
    public String getTestedClassName() {
        return BwaSpark.class.getSimpleName();
    }

    @Test
    public void test() throws Exception {
        ArgumentsBuilder args = new ArgumentsBuilder();
        this.runCommandLine(args.getArgsArray());
    }

}
