package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.fastq.FastqRecord;
import htsjdk.samtools.fastq.FastqWriter;
import htsjdk.samtools.fastq.FastqWriterFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariationSparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.io.IOUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadUtils;
import scala.Tuple2;
import scala.Tuple3;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// TODO: choose which parameters allowed to be tunable
// TODO: if throws, would temp files be cleaned up automatically?
// TODO: most robust way to check OS in method SGASerialRunner (Mac doesn't support multi-thread mechanism in SGA)?
@CommandLineProgramProperties(
        summary        = "Program to call SGA for performing local assembly and outputs assembled contigs.",
        oneLineSummary = "Perform SGA-based local assembly on fasta files on Spark",
        programGroup   = StructuralVariationSparkProgramGroup.class)
public final class RunSGANaivelySpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Argument(doc       = "Absolute path to SGA installation.",
              shortName = "sgaPath",
              fullName  = "fullPathToSGA",
              optional  = false)
    public String pathToSGA = null;

    @Argument(doc       = "A path to a directory to write results to.",
              shortName = "outDir",
              fullName  = "outputDirectory",
              optional  = false)
    public String outputDir = null;

    @Argument(doc       = "Number of threads to use when running sga.",
              shortName = "t",
              fullName  = "threads",
              optional  = true)
    public int threads = 1;

    @Argument(doc       = "To run k-mer based read correction, filter and duplication removal in SGA or not, with default parameters.",
              shortName = "correct",
              fullName  = "correctNFilter",
              optional  = true)
    public boolean runCorrectionSteps = false;

    @Override
    public boolean requiresReads(){
        return true;
    }

    @Override
    public void runTool(final JavaSparkContext ctx){

        final JavaRDD<GATKRead> reads = getReads();
        // TODO: experimental interface to caller, should be getting this from JavaSparkContext
        final JavaPairRDD<Long, Iterable<GATKRead>> readsAroundBreakpoints = reads.groupBy( read -> 1L);

        final Path sgaPath = Paths.get(pathToSGA);

        // map paired reads to FASTA file (temp files live in temp dir that cleans self up automatically) then feed to SGA for assembly
        final JavaPairRDD<Long, SGAAssemblyResultType> assembledContigs = readsAroundBreakpoints.mapToPair(RunSGANaivelySpark::convertReadsToFASTQ)
                                                                                                .mapToPair(entry -> RunSGANaivelySpark.performAssembly(entry, sgaPath, threads, runCorrectionSteps));

        validateAndSaveResults(assembledContigs, outputDir);
    }

    // validates the returned result:
    //   if all steps return 0, save the contig file and may discard the runtime information
    //   if any step returns non-zero code, save the runtime information for that break point
    private static void validateAndSaveResults(final JavaPairRDD<Long, SGAAssemblyResultType> results, final String outputDir){
        results.cache(); // cache because Spark doesn't have an efficient RDD.split(predicate) yet
        final JavaPairRDD<Long, SGAAssemblyResultType> success = results.filter(entry -> entry._1()!=null);
        success.mapToPair(entry -> new Tuple2<>(entry._1(), new ContigsCollection(entry._2().assembledContigFile)))
                .saveAsObjectFile(outputDir);
        final JavaPairRDD<Long, SGAAssemblyResultType> failure = results.filter(entry -> entry._2()==null);
        failure.mapToPair(entry -> new Tuple2<>(entry._1(), entry._2().runtimeInformation))
                .saveAsObjectFile(outputDir);
    }

    /**
     * Converts an entry in an JavaPairRDD whose first is an breakpoint id and second is an iterable of paired reads
     *    where either end is "mapped" (which ever way the read picker think mapping means) to a place near
     *    the breakpoint represented by the id.
     * @param orderedReadsOfABreakPoint  the entry to be converted
     * @return                           a tuple corresponding to this breakpoint where the first is still the breakpoint id,
     *                                   and the second is another tuple of the temporary directory and a temporary FASTQ file
     *                                   containing sequence information.
     * @throws IOException               an IOException is thrown if the conversion fails to create the FASTQ file
     */
    @VisibleForTesting
    static Tuple2<Long, File> convertReadsToFASTQ(final Tuple2<Long, Iterable<GATKRead>> orderedReadsOfABreakPoint) throws IOException{

        final Long breakpointId = orderedReadsOfABreakPoint._1();

        File workingDir = new File( Files.createTempDirectory("assembly" + orderedReadsOfABreakPoint._1().toString()).toString() );
        if(null==workingDir){
            throw new IOException("Failed to create temporary directory to perform assembly in.");
        }
        IOUtils.deleteRecursivelyOnExit(workingDir);

        final File fastq = new File(workingDir, "assembly" + breakpointId + ".fastq");
        if(null==fastq){
            throw new IOException("Failed to create temporary FASTQ file to feed to SGA.");
        }

        try(final FastqWriter writer = new FastqWriterFactory().newWriter(fastq)){
            Iterator<GATKRead> it = orderedReadsOfABreakPoint._2().iterator();
            while(it.hasNext()){
                writeToTempFASTQ(it.next(), writer);
                writeToTempFASTQ(it.next(), writer);
            }
        }

        return new Tuple2<>(breakpointId, fastq);
    }

    // copied from FindSVBreakpointsSpark
    private static void writeToTempFASTQ(final GATKRead read, final FastqWriter writer){
        final String readName      = read.getName();// TODO: hack for unit testing, where read names have /1 and /2. should be:   + (read.isSecondOfPair() ? "/2" : "/1");
        final String readBases     = read.getBasesString();
        final String baseQualities = ReadUtils.getBaseQualityString(read);
        writer.write(new FastqRecord(readName, readBases, "", baseQualities));
    }

    /**
     * Final return type of the whole process of SGA local assembly.
     * assembledContigFile is the file containing the assembled contigs, if the process executed successfully, or null if not.
     * runtimeInformation contains the runtime information logged along the process up until the process erred, if errors happen,
     *  or until the last step if no errors occur along the line.
     *  The list is organized along the process of executing the assembly pipeline, inside each entry of the list is
     *      the name of the module,
     *      return status of the module,
     *      stdout message of the module, if any,
     *      stderr message of the module, if any
     */
    @VisibleForTesting
    static final class SGAAssemblyResultType implements Serializable{
        private static final long serialVersionUID = 1L;

        public final File assembledContigFile;
        public final List<Tuple2<String, Tuple3<Integer, String, String>>> runtimeInformation;

        public SGAAssemblyResultType(final File contigFile, final List<Tuple2<String, Tuple3<Integer, String, String>>> collectedRuntimeInfo){
            assembledContigFile = contigFile;
            runtimeInformation = collectedRuntimeInfo;
        }
    }

    /**
     * Performs assembly on the FASTA files. Essentially a wrapper around a limited number of modules in SGA.
     * @param fastqFilesForEachBreakPoint temporary FASTA file of sequences around the putative breakpoint
     * @param sgaPath                     full path to SGA
     * @param threads                     number of threads to use in various modules, if the module supports parallelism
     * @param runCorrections              user's decision to run sga's corrections (with default parameter values) or not
     * @return                            contig file and runtime information, associated with the breakpoint id
     */
    private static Tuple2<Long, SGAAssemblyResultType> performAssembly(final Tuple2<Long, File> fastqFilesForEachBreakPoint,
                                                                       final Path sgaPath,
                                                                       final int threads,
                                                                       final boolean runCorrections){

        final File rawFASTQ = fastqFilesForEachBreakPoint._2();

        final SGAAssemblyResultType assembledContigsFileAndRuntimeInfo = SGASerialRunner(sgaPath, rawFASTQ, threads, runCorrections);

        return new Tuple2<>(fastqFilesForEachBreakPoint._1(), assembledContigsFileAndRuntimeInfo);
    }

    @VisibleForTesting
    static SGAAssemblyResultType SGASerialRunner(final Path sgaPath, final File rawFASTQ, final int threads, final boolean runCorrections){

        int threadsToUse = threads;
        if( System.getProperty("os.name").toLowerCase().contains("mac") && threads>1){
            System.err.println("Running on Mac OS X, which doesn't provide unnamed semaphores used by SGA. " +
                               "Resetting threads argument to 1. ");
            threadsToUse = 1;
        }
        // store every intermediate file in the same temp directory as the raw fastq file
        final File workingDir = rawFASTQ.getParentFile();

        // the index module is used frequently, so make single instance and pass around
        final SGAModule indexer = new SGAModule("index");
        final ArrayList<String> indexerArgs = new ArrayList<>();
        indexerArgs.add("--algorithm"); indexerArgs.add("ropebwt");
        indexerArgs.add("--threads");   indexerArgs.add(Integer.toString(threadsToUse));
        indexerArgs.add("--check");
        indexerArgs.add("");

        // collect runtime information along the way
        final List<Tuple2<String, Tuple3<Integer, String, String>>> runtimeInfo = new ArrayList<>();

        String preppedFileName = runAndStopEarly("preprocess", rawFASTQ, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
        if( null == preppedFileName ){
            return new SGAAssemblyResultType(null, runtimeInfo);
        }

        final File preprocessedFile = new File(workingDir, preppedFileName);

        if(runCorrections){// correction, filter, and remove duplicates stringed together
            preppedFileName = runAndStopEarly("correct", preprocessedFile, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
            if( null == preppedFileName ){
                return new SGAAssemblyResultType(null, runtimeInfo);
            }
            final File correctedFile = new File(workingDir, preppedFileName);

            preppedFileName = runAndStopEarly("filter", correctedFile, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
            if( null == preppedFileName ){
                return new SGAAssemblyResultType(null, runtimeInfo);
            }
            final File filterPassingFile = new File(workingDir, preppedFileName);

            preppedFileName = runAndStopEarly("rmdup", filterPassingFile, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
            if( null == preppedFileName ){
                return new SGAAssemblyResultType(null, runtimeInfo);
            }
        }

        final File fileToMerge      = new File(workingDir, preppedFileName);
        final String fileNameToAssemble = runAndStopEarly("fm-merge", fileToMerge, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
        if(null == fileNameToAssemble){
            return new SGAAssemblyResultType(null, runtimeInfo);
        }

        final File fileToAssemble   = new File(workingDir, fileNameToAssemble);
        final String contigsFileName = runAndStopEarly("assemble", fileToAssemble, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
        if(null == contigsFileName){
            return new SGAAssemblyResultType(null, runtimeInfo);
        }

        // if code reaches here, all things should went smoothly
        final File assembledContigsFile = new File(workingDir, contigsFileName);
        return new SGAAssemblyResultType(assembledContigsFile, runtimeInfo);
    }

    /**
     * Call the right sga module, log runtime information, and return the output file name if succeed.
     * If process erred, the string returned is null.
     */
    private static String runAndStopEarly(final String moduleName, final File inputFASTQFile, final Path sgaPath, final File workingDir,
                                          final int threads, final SGAModule indexer, final ArrayList<String> indexerArgs,
                                          final List<Tuple2<String, Tuple3<Integer, String, String>>> collectiveRuntimeInfo){

        SGAUnitRuntimeInfoStruct thisRuntime = null;
        if(moduleName.equalsIgnoreCase("preprocess")){
            thisRuntime = SGAPreprocess(sgaPath, inputFASTQFile, workingDir, indexer, indexerArgs);
        }else if(moduleName.equalsIgnoreCase("correct")){
            thisRuntime = SGACorrect(sgaPath, inputFASTQFile, workingDir, threads, indexer, indexerArgs);
        }else if(moduleName.equalsIgnoreCase("filter")){
            thisRuntime = SGAFilter(sgaPath, inputFASTQFile, workingDir, threads);
        }else if(moduleName.equalsIgnoreCase("rmdup")){
            thisRuntime = SGARmDuplicate(sgaPath, inputFASTQFile, workingDir, threads, indexer, indexerArgs);
        }else if(moduleName.equalsIgnoreCase("fm-merge")){
            thisRuntime = SGAFMMerge(sgaPath, inputFASTQFile, workingDir, threads, indexer, indexerArgs);
        }else if(moduleName.equalsIgnoreCase("assemble")){
            thisRuntime = SGAAssemble(sgaPath, inputFASTQFile, workingDir, threads);
        }else{
            throw new GATKException("Wrong module called");
        }

        collectiveRuntimeInfo.addAll(thisRuntime.runtimeInfo);

        if( thisRuntime.runtimeInfo.stream().mapToInt(entry -> entry._2()._1()).sum()!=0 ){
            return null;
        }else{
            return thisRuntime.fileNameToReturn;
        }
    }

    @VisibleForTesting
    static SGAUnitRuntimeInfoStruct SGAPreprocess(final Path sgaPath,
                                                  final File inputFASTQFile,
                                                  final File outputDirectory,
                                                  final SGAModule indexer,
                                                  final ArrayList<String> indexerArgs){

        final String prefix = extractBaseNameWithoutExtension(inputFASTQFile);
        final String preprocessedFASTAFileName = prefix+".pp.fa";

        final SGAModule preprocess = new SGAModule("preprocess");
        final ArrayList<String> ppArgs = new ArrayList<>();
        ppArgs.add("--pe-mode");    ppArgs.add("2");
        ppArgs.add("--pe-orphans"); ppArgs.add(prefix+".pp.orphan.fa");
        ppArgs.add("--out");        ppArgs.add(preprocessedFASTAFileName);
        ppArgs.add(inputFASTQFile.getName());

        final Tuple3<Integer, String, String> ppInfo = preprocess.run(sgaPath, outputDirectory, ppArgs);

        indexerArgs.set(indexerArgs.size()-1, preprocessedFASTAFileName);
        final Tuple3<Integer, String, String> indexerInfo = indexer.run(sgaPath, outputDirectory, indexerArgs);

        final SGAUnitRuntimeInfoStruct ppUnitInfo = new SGAUnitRuntimeInfoStruct("preprocess", ppInfo, preprocessedFASTAFileName);
        ppUnitInfo.add("index", indexerInfo);

        return ppUnitInfo;
    }

    @VisibleForTesting
    static SGAUnitRuntimeInfoStruct SGACorrect(final Path sgaPath,
                                               final File inputFASTAFile,
                                               final File outputDirectory,
                                               final int threads,
                                               final SGAModule indexer,
                                               final ArrayList<String> indexerArgs){
        return simpleModuleRun(sgaPath, "correct", ".ec.fa", inputFASTAFile, outputDirectory, threads, indexer, indexerArgs);
    }

    @VisibleForTesting
    static SGAUnitRuntimeInfoStruct SGAFilter(final Path sgaPath,
                                              final File inputFASTAFile,
                                              final File outputDirectory,
                                              final int threads){
        final String prefix = extractBaseNameWithoutExtension(inputFASTAFile);
        final SGAModule filter = new SGAModule("filter");
        final ArrayList<String> filterArgs = new ArrayList<>();
        filterArgs.add("--threads"); filterArgs.add(Integer.toString(threads));
        filterArgs.add(prefix+".fa");
        final Tuple3<Integer, String, String> filterInfo = filter.run(sgaPath, outputDirectory, filterArgs);

        return new SGAUnitRuntimeInfoStruct("filer", filterInfo, prefix+".filter.pass.fa");
    }

    @VisibleForTesting
    static SGAUnitRuntimeInfoStruct SGARmDuplicate(final Path sgaPath,
                                                   final File inputFASTAFile,
                                                   final File outputDirectory,
                                                   final int threads,
                                                   final SGAModule indexer,
                                                   final ArrayList<String> indexerArgs){
        return simpleModuleRun(sgaPath, "rmdup", ".rmdup.fa", inputFASTAFile, outputDirectory, threads, indexer, indexerArgs);
    }

    @VisibleForTesting
    static SGAUnitRuntimeInfoStruct SGAFMMerge(final Path sgaPath,
                                               final File inputFASTAFile,
                                               final File outputDirectory,
                                               final int threads,
                                               final SGAModule indexer,
                                               final ArrayList<String> indexerArgs){
        return simpleModuleRun(sgaPath, "fm-merge", ".merged.fa", inputFASTAFile, outputDirectory, threads, indexer, indexerArgs);
    }

    @VisibleForTesting
    static SGAUnitRuntimeInfoStruct SGAAssemble(final Path sgaPath,
                                                final File inputFASTAFile,
                                                final File outputDirectory,
                                                final int threads){

        final SGAModule overlap = new SGAModule("overlap");
        final ArrayList<String> overlapArgs = new ArrayList<>();
        overlapArgs.add("--threads"); overlapArgs.add(Integer.toString(threads));
        overlapArgs.add(inputFASTAFile.getName());
        final Tuple3<Integer, String, String> overlapInfo = overlap.run(sgaPath, outputDirectory, overlapArgs);

        final String prefix = extractBaseNameWithoutExtension(inputFASTAFile);

        final SGAModule assemble = new SGAModule("assemble");
        final ArrayList<String> assembleArgs = new ArrayList<>();
        assembleArgs.add("--out-prefix"); assembleArgs.add(prefix);
        assembleArgs.add(prefix+".asqg.gz");
        final Tuple3<Integer, String, String> assembleInfo = assemble.run(sgaPath, outputDirectory, assembleArgs);

        final SGAUnitRuntimeInfoStruct assembleUnitInfo = new SGAUnitRuntimeInfoStruct("assemble", assembleInfo, prefix+"-contigs.fa");
        assembleUnitInfo.add("overlap", overlapInfo);
        return assembleUnitInfo;
    }

    // boiler plate code for running simple sga modules
    private static SGAUnitRuntimeInfoStruct simpleModuleRun(final Path sgaPath,
                                                            final String moduleName,
                                                            final String extensionToAppend,
                                                            final File inputFASTAFile,
                                                            final File outputDirectory,
                                                            final int threads,
                                                            final SGAModule indexer,
                                                            final ArrayList<String> indexerArgs){

        final SGAModule module = new SGAModule(moduleName);
        final ArrayList<String> args = new ArrayList<>();
        args.add("--threads");   args.add(Integer.toString(threads));
        args.add(inputFASTAFile.getName());
        final Tuple3<Integer, String, String> moduleInfo = module.run(sgaPath, outputDirectory, args);

        final String outputFileName = extractBaseNameWithoutExtension(inputFASTAFile) + extensionToAppend;

        final SGAUnitRuntimeInfoStruct unitInfo = new SGAUnitRuntimeInfoStruct(moduleName, moduleInfo, outputFileName);

        indexerArgs.set(indexerArgs.size()-1, outputFileName);
        final Tuple3<Integer, String, String> indexerInfo = indexer.run(sgaPath, outputDirectory, indexerArgs);
        unitInfo.add("index", indexerInfo);

        return unitInfo;
    }

    // From https://www.stackoverflow.com/questions/4545937
    @VisibleForTesting
    static String extractBaseNameWithoutExtension(final File file){
        final String[] tokens = file.getName().split("\\.(?=[^\\.]+$)");
        return tokens[0];
    }

    /**
     * A struct to represent the runtime information returned by running a unit of sga modules.
     * Because some of the modules are designed to be run together in serial form in a unit (e.g. correction followed by indexing)
     *   all return status and stdout stderr messages in this unit are logged.
     * But only one file name is important for consumption by downstream units/modules, so only that file name is kept.
     * The ctor and the add() method are designed asymmetrically, because when a unit is consisted of multiple modules,
     *   only one generates the file name to be returned, and the other modules are simply to be logged.
     */
    @VisibleForTesting
    static final class SGAUnitRuntimeInfoStruct implements Serializable{
        private static final long serialVersionUID = 1L;

        public final String fileNameToReturn;
        public final List<Tuple2<String, Tuple3<Integer, String, String>>> runtimeInfo;

        public SGAUnitRuntimeInfoStruct(final String moduleName, final Tuple3<Integer, String, String> entry, final String fileName){
            fileNameToReturn = fileName;
            runtimeInfo = new ArrayList<>();
            add(moduleName, entry);
        }

        public void add(final String moduleName, final Tuple3<Integer, String, String> entry){
            runtimeInfo.add(new Tuple2<>(moduleName, entry));
        }
    }

    /**
     * Represents a collection of assembled contigs in the final output of "sga assemble".
     */
    @VisibleForTesting
    static final class ContigsCollection implements Serializable{
        private static final long serialVersionUID = 1L;

        @VisibleForTesting
        static final class ContigSequence implements Serializable{
            private static final long serialVersionUID = 1L;

            private final String sequence;
            public ContigSequence(final String sequence){ this.sequence = sequence; }
            public String getSequenceAsString(){ return sequence; }
        }

        @VisibleForTesting
        static final class ContigID implements Serializable{
            private static final long serialVersionUID = 1L;

            private final String id;
            public ContigID(final String idString) { this.id = idString; }
            public String getId() { return id; }
        }

        private List<Tuple2<ContigID, ContigSequence>> contents;

        public List<Tuple2<ContigID, ContigSequence>> getContents(){
            return contents;
        }

        public ContigsCollection(final File fastaFile) throws IOException{

            final List<String> lines = Files.readAllLines(Paths.get(fastaFile.getAbsolutePath()));

            contents = new ArrayList<>();
            for(int i=0; i<lines.size(); i+=2){
                contents.add(new Tuple2<>(new ContigID(lines.get(i)), new ContigSequence(lines.get(i+1))));
            }
        }
    }
}