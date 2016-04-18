package org.broadinstitute.hellbender.tools.spark.bwa;

import com.github.lindenb.jbwa.jni.BwaIndex;
import com.github.lindenb.jbwa.jni.BwaMem;
import com.github.lindenb.jbwa.jni.ShortRead;
import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import htsjdk.samtools.*;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.reference.ReferenceSequenceFile;
import htsjdk.samtools.reference.ReferenceSequenceFileFactory;
import htsjdk.samtools.util.StringUtil;
import org.apache.commons.collections4.iterators.IteratorIterable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
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
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Tuple2;

import javax.annotation.Nullable;
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
            // TODO: set property to keep paired reads together in same split
            JavaRDD<GATKRead> unalignedReads = getReads();
            JavaRDD<List<GATKRead>> unalignedPairs = unalignedReads.mapPartitions(iter -> () -> pairwise(iter));
            JavaRDD<Tuple2<ShortRead, ShortRead>> shortReadPairs = unalignedPairs.map(p -> {
                GATKRead read1 = p.get(0);
                GATKRead read2 = p.get(1);
                String name1 = pairedEndPrefix(read1.getName());
                String name2 = pairedEndPrefix(read2.getName());
                return new Tuple2<>(
                        new ShortRead(name1, read1.getBases(), SAMUtils.phredToFastq(read1.getBaseQualities()).getBytes()),
                        new ShortRead(name2, read2.getBases(), SAMUtils.phredToFastq(read2.getBaseQualities()).getBytes()));
            });

            // Load native library in each task VM
            shortReadPairs = shortReadPairs.mapPartitions(p -> {
                System.loadLibrary("bwajni");
                return new IteratorIterable<>(p);
            });

            BwaIndex index = new BwaIndex(new File(ref));
            BwaMem mem = new BwaMem(index);

            final Broadcast<BwaMem> memBroadcast = ctx.broadcast(mem); // TODO: does this work with native code?

            JavaRDD<String> samLines = shortReadPairs.mapPartitions(iter -> () -> concat(batchIterator(memBroadcast, iter)));

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

    private Iterator<List<String>> batchIterator(final Broadcast<BwaMem> memBroadcast, Iterator<Tuple2<ShortRead, ShortRead>> iter) {
        UnmodifiableIterator<List<Tuple2<ShortRead, ShortRead>>> batches = Iterators.partition(iter, 50);
        Iterator<List<String>> it = Iterators.transform(batches, new Function<List<Tuple2<ShortRead, ShortRead>>, List<String>>() {
            @Nullable
            @Override
            public List<String> apply(@Nullable List<Tuple2<ShortRead, ShortRead>> input) {
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
            }
        });
        return it;
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

    private static String pairedEndPrefix(String pairedEndName) {
        return pairedEndName.substring(0, pairedEndName.length() - 2); // TODO: do something like Picard's FastqToSam#getBaseName
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
