package org.broadinstitute.hellbender.tools.spark.sv;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.utils.test.ArgumentsBuilder;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests RunMinimalBWAMEM.
 */
public final class RunMinimalBWAMEMTest extends CommandLineProgramTest {

    private static final File TEST_DATA_DIR = new File(getTestDataDir(), "spark/sv/RunMinimalBWAMEM");
    private static final File expectedSam = new File(TEST_DATA_DIR, "expected.sam");
    private static final Path ref = Paths.get(b37_reference_20_21);
    private static final Path bwaPath;
    static {
        if (System.getProperty("user.name").equalsIgnoreCase("shuang")) {
            bwaPath = Paths.get("/usr/local/bin/bwa");
        } else {
            bwaPath = Paths.get("/home/travis/bwaApache2/bwa");
        }
    }

    @Test(groups="sv")
    public void testSeparate() throws IOException {

        final ArgumentsBuilder args = new ArgumentsBuilder();
        final File samOutput = boilerPlate(args);

        // input arguments
        final File input = new File(TEST_DATA_DIR, "input_1.fastq");
        args.add("-" + StandardArgumentDefinitions.INPUT_SHORT_NAME);
        args.add(input.getAbsolutePath());
        final File secondInput = new File(TEST_DATA_DIR, "input_2.fastq");
        args.add("-" + "I2");
        args.add(secondInput.getAbsolutePath());

        this.runCommandLine(args.getArgsArray());

        Assert.assertEquals(getSamRecords(expectedSam), getSamRecords(samOutput));
    }

    @Test(groups="sv")
    public void testInterLeaved() throws IOException {

        final ArgumentsBuilder args = new ArgumentsBuilder();
        final File samOutput = boilerPlate(args);

        // input arguments
        args.add("-p");
        final File input = new File(TEST_DATA_DIR, "interleaved.fastq");
        args.add("-" + StandardArgumentDefinitions.INPUT_SHORT_NAME);
        args.add(input.getAbsolutePath());

        this.runCommandLine(args.getArgsArray());

        Assert.assertEquals(getSamRecords(expectedSam), getSamRecords(samOutput));
    }

    private static File boilerPlate(final ArgumentsBuilder args) throws IOException{

        args.add("-" + "bwaPath");
        args.add(bwaPath.toString());

        final File wkDir = BaseTest.createTempDir("dummy");
        args.add("-" + "outDir");
        args.add(wkDir.getAbsolutePath());

        final File output = new File(wkDir, "test.sam");
        output.createNewFile();
        args.add("-" + StandardArgumentDefinitions.OUTPUT_SHORT_NAME);
        args.add(output.getName());

        final File REF = ref.toFile();
        args.add("-" + StandardArgumentDefinitions.REFERENCE_SHORT_NAME);
        args.add(REF.getAbsolutePath());

        return output;
    }

    // utility function to extract just alignment records
    // header information depends on environment in which the test is run and expected sam is generated, so is un-reproducible
    private static List<String> getSamRecords (final File samFile) throws IOException{
        final List<String> samRecords = new ArrayList<>();
        try(final BufferedReader reader = new BufferedReader(new FileReader(samFile))) {
            String line;
            while ( (line = reader.readLine()) != null) {
                if(!line.startsWith("@")){
                    samRecords.add(line);
                }
            }
        }catch(final IOException e) {
            throw new IOException("Errors occurred parsing sam file: " + samFile.toString() + e.getMessage());
        }
        return samRecords;
    }
}
