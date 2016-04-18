package org.broadinstitute.hellbender.tools.spark.bwa;

import com.github.lindenb.jbwa.jni.BwaIndex;
import com.github.lindenb.jbwa.jni.BwaMem;
import com.github.lindenb.jbwa.jni.ShortRead;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import htsjdk.samtools.*;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.reference.ReferenceSequenceFile;
import htsjdk.samtools.reference.ReferenceSequenceFileFactory;
import htsjdk.samtools.util.StringUtil;
import org.apache.commons.collections4.iterators.IteratorIterable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.SAMRecordToGATKReadAdapter;
import org.seqdoop.hadoop_bam.BAMInputFormat;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.broadinstitute.hellbender.utils.Utils.calcMD5;

@CommandLineProgramProperties(summary = "Runs BWA",
        oneLineSummary = "BWA on Spark",
        programGroup = SparkProgramGroup.class)
public final class BwaSpark extends GATKSparkTool {

    private static final long serialVersionUID = 1L;

    @Argument(doc = "the reference", shortName = "ref",
            fullName = "ref", optional = false)
    private String ref;

    @Argument(doc = "the output bam", shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, optional = false)
    private String output;

    @Override
    public boolean requiresReads() {
        return true;
    }

    @Override
    protected void runTool(final JavaSparkContext ctx) {
        System.loadLibrary("bwajni");
        try {
            ctx.hadoopConfiguration().setBoolean(BAMInputFormat.KEEP_PAIRED_READS_TOGETHER_PROPERTY, true); // TODO: need to make this work even for non-queryname sorted BAM
            JavaRDD<GATKRead> unalignedReads = getReads();
            JavaRDD<List<GATKRead>> unalignedPairs = unalignedReads.mapPartitions(iter -> () -> pairwise(iter));
            JavaRDD<Tuple2<ShortRead, ShortRead>> shortReadPairs = unalignedPairs.map(p -> {
                GATKRead read1 = p.get(0);
                GATKRead read2 = p.get(1);
                String name1 = read1.getName();
                String name2 = read2.getName();
                byte[] baseQualities1 = SAMUtils.phredToFastq(read1.getBaseQualities()).getBytes();
                byte[] baseQualities2 = SAMUtils.phredToFastq(read2.getBaseQualities()).getBytes();
                return new Tuple2<>(
                        new ShortRead(name1, read1.getBases(), baseQualities1),
                        new ShortRead(name2, read2.getBases(), baseQualities2));
            });

            // Load native library in each task VM
            shortReadPairs = shortReadPairs.mapPartitions(p -> {
                System.loadLibrary("bwajni");
                return new IteratorIterable<>(p);
            });

            BwaIndex index = new BwaIndex(new File(ref));
            BwaMem mem = new BwaMem(index);

            final Broadcast<BwaMem> memBroadcast = ctx.broadcast(mem); // TODO: does this work with native code?

            int batchSize = 50; // TODO: the whole partition?
            JavaRDD<String> samLines = shortReadPairs.mapPartitions(iter -> () -> concat(batchIterator(memBroadcast, iter, batchSize)));

            // TODO: is there a better way to build a header? E.g. from the BAM
            final SAMSequenceDictionary sequences = makeSequenceDictionary(new File(ref));
            final SAMFileHeader readsHeader = new SAMFileHeader();
            readsHeader.setSequenceDictionary(sequences);

            final SAMLineParser samLineParser = new SAMLineParser(new DefaultSAMRecordFactory(), ValidationStringency.SILENT, readsHeader, null, null);
            Broadcast<SAMLineParser> samLineParserBroadcast = ctx.broadcast(samLineParser);

            JavaRDD<GATKRead> reads = samLines.map(r -> new SAMRecordToGATKReadAdapter(samLineParserBroadcast.getValue().parseLine(r)));

            SAMFileWriterFactory samFileWriterFactory = new SAMFileWriterFactory();
            try (SAMFileWriter samFileWriter = samFileWriterFactory.makeSAMWriter(readsHeader, true, new File(output))) {
                for (GATKRead r : reads.collect()) {
                    samFileWriter.addAlignment(r.convertToSAMRecord(readsHeader));
                }
            }

            index.close();
            mem.dispose();
        } catch (IOException e) {
            throw new GATKException(e.toString());
        }
    }

    private <U> Iterator<List<GATKRead>> pairwise(Iterator<GATKRead> iter) {
        return Iterators.partition(iter, 2);
    }

    private Iterator<List<String>> batchIterator(final Broadcast<BwaMem> memBroadcast, Iterator<Tuple2<ShortRead, ShortRead>> iter, int batchSize) {
        return Iterators.transform(Iterators.partition(iter, batchSize), input -> {
            List<ShortRead> reads1 = new ArrayList<>();
            List<ShortRead> reads2 = new ArrayList<>();
            for (Tuple2<ShortRead, ShortRead> p : input) {
                reads1.add(p._1);
                reads2.add(p._2);
            }
            try {
                String[] alignments = memBroadcast.getValue().align(reads1, reads2);
                return Arrays.asList(alignments);
            } catch (IOException e) {
                throw new GATKException(e.toString());
            }
        });
    }

    static <T> Iterator<T> concat(Iterator<? extends Iterable<T>> iterator) {
        return new AbstractIterator<T>() {
            Iterator<T> subIterator;
            @Override
            protected T computeNext() {
                if (subIterator != null && subIterator.hasNext()) {
                    return subIterator.next();
                }
                while (iterator.hasNext()) {
                    subIterator = iterator.next().iterator();
                    if (subIterator.hasNext()) {
                        return subIterator.next();
                    }
                }
                return endOfData();
            }
        };
    }

    // From CreateSequenceDictionary

    public int NUM_SEQUENCES = Integer.MAX_VALUE;

    SAMSequenceDictionary makeSequenceDictionary(final File referenceFile) {
        final ReferenceSequenceFile refSeqFile =
                ReferenceSequenceFileFactory.getReferenceSequenceFile(referenceFile, true);
        ReferenceSequence refSeq;
        final List<SAMSequenceRecord> ret = new ArrayList<>();
        final Set<String> sequenceNames = new HashSet<>();
        for (int numSequences = 0; numSequences < NUM_SEQUENCES && (refSeq = refSeqFile.nextSequence()) != null; ++numSequences) {
            if (sequenceNames.contains(refSeq.getName())) {
                throw new UserException.MalformedFile(referenceFile,
                        "Sequence name appears more than once in reference: " + refSeq.getName());
            }
            sequenceNames.add(refSeq.getName());
            ret.add(makeSequenceRecord(refSeq));
        }
        return new SAMSequenceDictionary(ret);
    }

    /**
     * Create one SAMSequenceRecord from a single fasta sequence
     */
    private SAMSequenceRecord makeSequenceRecord(final ReferenceSequence refSeq) {
        final SAMSequenceRecord ret = new SAMSequenceRecord(refSeq.getName(), refSeq.length());

        // Compute MD5 of upcased bases
        final byte[] bases = refSeq.getBases();
        for (int i = 0; i < bases.length; ++i) {
            bases[i] = StringUtil.toUpperCase(bases[i]);
        }

        ret.setAttribute(SAMSequenceRecord.MD5_TAG, calcMD5(bases));
//        if (GENOME_ASSEMBLY != null) {
//            ret.setAttribute(SAMSequenceRecord.ASSEMBLY_TAG, GENOME_ASSEMBLY);
//        }
//        ret.setAttribute(SAMSequenceRecord.URI_TAG, URI);
//        if (SPECIES != null) {
//            ret.setAttribute(SAMSequenceRecord.SPECIES_TAG, SPECIES);
//        }
        return ret;
    }
}
