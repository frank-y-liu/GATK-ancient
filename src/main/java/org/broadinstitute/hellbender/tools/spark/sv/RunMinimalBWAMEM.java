package org.broadinstitute.hellbender.tools.spark.sv;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgram;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariationSparkProgramGroup;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import scala.Tuple3;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.ArrayList;

// TODO: choose which parameters allowed to be tunable
// TODO: if throws, would temp files be cleaned up automatically?
@CommandLineProgramProperties(
        summary        = "Minimal program to call BWAMEM for performing alignment, allowing limited options.",
        oneLineSummary = "Minimal work to call BWAMEM.",
        programGroup   = StructuralVariationSparkProgramGroup.class)
public final class RunMinimalBWAMEM extends CommandLineProgram {

    @Argument(doc       = "Absolute path to BWA program.",
              shortName = "bwaPath",
              fullName  = "fullPathToBWA",
              optional  = false)
    public String pathToBWA = null;

    @Argument(doc       = "Absolute path to input FASTQ/A file to be aligned.",
              shortName = StandardArgumentDefinitions.INPUT_SHORT_NAME,
              fullName  = StandardArgumentDefinitions.INPUT_LONG_NAME,
              optional  = false)
    public String input = null;

    @Argument(doc       = "Absolute path to second input FASTQ/A file to be aligned." +
                            "Sequences in this file are assumed to have the same read name as they appear in the first input, and in the same order.",
              shortName = "I2",
              fullName  = "secondInput",
              optional  = true)
    public String secondInput = null;

    @Argument(doc       = "If set to true, performs smart pairing (input FASTQ assumed interleaved), and ignores second fastq input.",
              shortName = "p",
              fullName  = "interLeaved",
              optional  = true)
    public boolean interLeaved = false;

    @Argument(doc       = "If set to true, assumes input are single ended data.",
              shortName = "se",
              fullName  = "singleEnd",
              optional  = true)
    public boolean SEInput = false;

    @Argument(doc       = "Absolute path to reference of the target organism, if alignment of assembled contigs to reference is desired.",
              shortName = StandardArgumentDefinitions.REFERENCE_SHORT_NAME,
              fullName  = StandardArgumentDefinitions.REFERENCE_LONG_NAME,
              optional  = false)
    public String reference = null;

    @Argument(doc       = "Sam file to write results to.",
              shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
              fullName  = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
              optional  = false)
    public String samOutput = null;

    @Argument(doc       = "A path to a directory to write results to.",
              shortName = "outDir",
              fullName  = "outputDirectory",
              optional  = false)
    public String outputDir = null;

    @Argument(doc       = "File name where stderr of running bwa should be redirected to.",
              shortName = "eFile",
              fullName  = "stderFile",
              optional  = true)
    public String stderrDestFileName = null;

    @Argument(doc       = "Number of threads to use when running bwa.",
              shortName = "t",
              fullName  = "threads",
              optional  = true)
    public int threads = 1;

    @Argument(doc       = "Number of threads to use when running bwa.",
              shortName = "K",
              fullName  = "chunkSizeEachThread",
              optional  = true)
    public long chunkSize = 0;

    /**
     * Validates user options. Throws UserException if arguments provided don't make logical sense.
     * Runs bwa mem. Throws GATKException if the bwa mem process erred.
     * @return  stderr message from underlying program (e.g. logs, performance, etc) upon successful execution,
     *          empty if caller decides to redirect the stderr message to a file
     */
    @Override
    public String doWork(){

        validateUserOptions();

        final BWAMEMModule bwamem = new BWAMEMModule();
        final Tuple3<Integer, String, String> result = bwamem.run(Paths.get(pathToBWA), new File(outputDir), makeArgs());

        return validateResults(result);
    }

    private void validateUserOptions() throws UserException {
        final boolean validOptions = (null==secondInput) ? !(interLeaved && SEInput) : (!interLeaved || SEInput);
        if(!validOptions){
            throw new UserException("CMD line argument options on input (paired, interleaved, or SE) don't make sense. Please check.");
        }
    }

    private String validateResults(final Tuple3<Integer, String, String> result) throws GATKException {

        switch (result._1()){
            case 0:  return writeSamFile(result);
            case 1:  throw new GATKException("Failed to start bwa mem." + System.getProperty("line.separator") + result._3());
            case 2:  throw new GATKException("The bwa mem process was interrupted." + System.getProperty("line.separator") + result._3());
            case 3:  throw new GATKException("Failed to capture bwa stdout/stderr message" + System.getProperty("line.separator") + result._3());
            default: throw new GATKException("bwa mem returned non-zero code: " + Integer.toString(result._1() - 10) +
                                                System.getProperty("line.separator") + result._3());
        }
    }

    /**
     * Writes stdout from bwa mem to designated file.
     * Return stderr message as string, which will be empty if user decides to redirect stderr message to file (i.e. stderrDestFileName is set)
     */
    private String writeSamFile(final Tuple3<Integer, String, String> result){

        try{
            final File wkDir = new File(outputDir);
            final File samFile = new File(wkDir, samOutput);
            FileUtils.writeStringToFile(samFile, result._2());

        } catch (final IOException e){
            throw new GATKException("Failed to dump bwa mem stdout to designated sam file." +
                                        System.getProperty("line.separator") + e.getMessage());
        }

        if(null==stderrDestFileName){
            return result._2();
        }else{
            return "";
        }
    }

    private List<String> makeArgs(){
        final List<String> args = new ArrayList<>();

        final Path pathToReference = Paths.get(reference);
        final Path pathToInput = Paths.get(input);

        args.add("-t");
        args.add(Integer.toString(threads));

        if(0!=chunkSize){
            args.add("-K");
            args.add(Long.toString(this.chunkSize));
        }

        if(interLeaved){ // paired reads, interleaved
            args.add("-p");
        }else if(null==secondInput) { // SE reads
            args.add("-S"); // skips mate rescuing and pairing
            args.add("-P");
        }
        args.add(pathToReference.toString());
        args.add(pathToInput.toString());

        if(null!=secondInput){ // paired reads, separate files
            final Path pathToSecondInput = Paths.get(secondInput);
            args.add(pathToSecondInput.toString());
        }

        return args;
    }
}