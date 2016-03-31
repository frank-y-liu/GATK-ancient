package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.SerializableComparator;
import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.*;
import htsjdk.samtools.fastq.FastqRecord;
import htsjdk.samtools.fastq.FastqWriter;
import htsjdk.samtools.fastq.FastqWriterFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKRegistrator;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.utils.HopscotchHashSet;
import org.broadinstitute.hellbender.tools.spark.utils.MapPartitioner;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadUtils;
import scala.Tuple2;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tool to describe reads that support a hypothesis of a genomic breakpoint.
 */
@CommandLineProgramProperties(summary="Find reads that evidence breakpoints.",
        oneLineSummary="Dump a description of all reads that support a hypothesis of a genomic breakpoint.",
        programGroup = SparkProgramGroup.class)
public final class FindBreakpointEvidenceSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;
    private static final int MIN_MAPQ = 20;
    private static final int MIN_MATCH_LEN = 45; // minimum length of matched portion of an interesting alignment
    //private static final int MAX_FRAGMENT_LEN = 2000;

    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Argument(doc = "directory for fastq output", shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, optional = false)
    private String outputDir;

    @Argument(doc = "directory for evidence output", fullName = "breakpointEvidenceDir", optional = false)
    private String evidenceDir;

    @Argument(doc = "file for breakpoint intervals output", fullName = "breakpointIntervals", optional = false)
    private String intervalFile;

    /**
     * This is a path to a file of kmers that appear too frequently in the reference to be usable as probes to localize
     * reads.  We don't calculate it here, because it depends only on the reference.
     * The program FindBadGenomicKmersSpark can produce such a list for you.
     */
    @Argument(doc = "file containing ubiquitous kmer list", fullName = "kmersToIgnore", optional = false)
    private String kmersToIgnoreFilename;

    private int nIntervals;

    @Override
    public boolean requiresReads()
    {
        return true;
    }

    @Override
    protected void runTool( final JavaSparkContext ctx ) {
        if ( getHeaderForReads().getSortOrder() != SAMFileHeader.SortOrder.coordinate ) {
            throw new GATKException("The reads must be coordinate sorted.");
        }

        // Process the input again, this time pulling all reads that contain kmers associated with a given breakpoint,
        // and writing those reads into a separate FASTQ for each breakpoint.
        final Broadcast<HopscotchHashSet<KmerAndInterval>> broadcastKmerAndIntervals =
                ctx.broadcast(new HopscotchHashSet<>(getKmerIntervals(ctx)));
        getUnfilteredReads()
            .filter(read ->
                    !read.isSecondaryAlignment() && !read.isSupplementaryAlignment() &&
                            !read.isDuplicate() && !read.failsVendorQualityCheck())
            .mapPartitions(readItr ->
                    new MapPartitioner<>(readItr, new ReadsForIntervalFinder(broadcastKmerAndIntervals.value())), false)
            .repartition(nIntervals)
            .saveAsTextFile(outputDir);
    }

    private List<KmerAndInterval> getKmerIntervals( final JavaSparkContext ctx ) {
        final Broadcast<Set<SVKmer>> broadcastKmerKillList =
                ctx.broadcast(SVKmer.readKmersFile(new File(kmersToIgnoreFilename)));
        final Broadcast<HopscotchHashSet<QNameAndInterval>> broadcastQNameAndIntervals =
                ctx.broadcast(new HopscotchHashSet<>(getQNames(ctx)));

        final List<KmerAndInterval> kmerIntervals =
                getUnfilteredReads()
                        .filter(read -> !read.isDuplicate() && !read.failsVendorQualityCheck() &&
                                !read.isSecondaryAlignment() && !read.isSupplementaryAlignment())
                        .mapPartitions(readItr ->
                                new MapPartitioner<>(readItr,new QNameKmerizer(broadcastQNameAndIntervals.value(),
                                        broadcastKmerKillList.value())), false)
                        .distinct()
                        .collect();

        broadcastQNameAndIntervals.destroy();
        broadcastKmerKillList.destroy();

        nIntervals = kmerIntervals.size();
        System.out.println(dateFormatter.format(System.currentTimeMillis())+" Discovered "+nIntervals+" kmers.");
        return kmerIntervals;
    }

    private List<QNameAndInterval> getQNames( final JavaSparkContext ctx ) {
        final Broadcast<ReadMetadata> broadcastMetadata = ctx.broadcast(getMetadata());
        final Broadcast<List<Interval>> broadcastIntervals = ctx.broadcast(getIntervals(broadcastMetadata));

        final List<QNameAndInterval> qNames =
                getUnfilteredReads()
                        .filter(read -> !read.isDuplicate() && !read.failsVendorQualityCheck() && !read.isUnmapped())
                        .mapPartitions(readItr ->
                                new MapPartitioner<>(readItr,
                                        new QNameFinder(broadcastMetadata.value(), broadcastIntervals.value())), false)
                        .distinct()
                        .collect();

        broadcastIntervals.destroy();
        broadcastMetadata.destroy();

        System.out.println(dateFormatter.format(System.currentTimeMillis())+" Discovered "+qNames.size()+" template names.");
        return qNames;
    }

    private List<Interval> getIntervals( final Broadcast<ReadMetadata> broadcastMetadata ) {
        // find all breakpoint evidence, then filter for pile-ups
        final int maxFragmentSize = broadcastMetadata.value().getMaxMedianFragmentSize();
        List<SAMSequenceRecord> contigs = getHeaderForReads().getSequenceDictionary().getSequences();
        final int nContigs = contigs.size();
        final JavaRDD<BreakpointEvidence> evidenceRDD =
                getUnfilteredReads()
                        .filter(read -> !read.isDuplicate() && !read.failsVendorQualityCheck() && !read.isUnmapped() &&
                                read.getMappingQuality() >= MIN_MAPQ &&
                                read.getCigar().getCigarElements()
                                        .stream()
                                        .filter(ele -> ele.getOperator()==CigarOperator.MATCH_OR_MISMATCH)
                                        .mapToInt(CigarElement::getLength)
                                        .sum() >= MIN_MATCH_LEN)
                        .mapPartitions(readItr ->
                                new MapPartitioner<>(readItr, new ReadClassifier(broadcastMetadata.value())), true)
                        .mapPartitions(evidenceItr ->
                                new MapPartitioner<>(evidenceItr, new BreakpointClusterer(2*maxFragmentSize)), true)
                        .mapPartitions(evidenceItr ->
                                new MapPartitioner<>(evidenceItr,
                                        new WindowSorter(3*maxFragmentSize), new BreakpointEvidence(nContigs)), true);
        evidenceRDD.cache();

        // record the evidence
        evidenceRDD.saveAsTextFile(evidenceDir);

        // find discrete intervals that contain the breakpoint evidence
        final Iterator<Interval> intervalItr =
                evidenceRDD
                        .mapPartitions(evidenceItr ->
                                new MapPartitioner<>(evidenceItr,
                                        new IntervalMapper(maxFragmentSize), new BreakpointEvidence(nContigs)), true)
                        .collect()
                        .iterator();

        // coalesce overlapping intervals (can happen at partition boundaries)
        final List<Interval> intervals = new ArrayList<>();
        if ( intervalItr.hasNext() ) {
            Interval prev = intervalItr.next();
            while ( intervalItr.hasNext() ) {
                final Interval next = intervalItr.next();
                if ( prev.isDisjointFrom(next) ) {
                    intervals.add(prev);
                    prev = next;
                } else {
                    prev = prev.join(next);
                }
            }
            intervals.add(prev);
        }
        evidenceRDD.unpersist();

        // record the intervals
        final PipelineOptions pipelineOptions =  getAuthenticatedGCSOptions();
        try ( final OutputStreamWriter writer = new OutputStreamWriter(new BufferedOutputStream(
                BucketUtils.createFile(intervalFile, pipelineOptions))) ) {
            for ( final Interval interval : intervals ) {
                final String seqName = contigs.get(interval.getContig()).getSequenceName();
                writer.write(seqName+" "+interval.getStart()+" "+interval.getEnd()+"\n");
            }
        }
        catch ( final IOException ioe ) {
            throw new GATKException("Can't write intervals file "+intervalFile, ioe);
        }

        System.out.println(dateFormatter.format(System.currentTimeMillis())+" Discovered "+intervals.size()+" intervals.");
        return intervals;
    }

    private ReadMetadata getMetadata() {
        final SAMFileHeader header = getHeaderForReads();
        final List<SAMReadGroupRecord> groups = header.getReadGroups();
        final int nGroups = groups.size();
        final List<ReadMetadata.ReadGroupFragmentStatistics> stats = new ArrayList<>(nGroups);
        for ( int idx = 0; idx != nGroups; ++idx ) {
            stats.add(new ReadMetadata.ReadGroupFragmentStatistics(400.f,75.f));
        }
        ReadMetadata readMetadata = new ReadMetadata(header, stats);
        System.out.println(dateFormatter.format(System.currentTimeMillis())+" Metadata retrieved.");
        return readMetadata;
    }

    /**
     * A class that acts as a filter for breakpoint evidence.
     * It passes only that evidence that is part of a putative cluster.
     */
    @VisibleForTesting static final class BreakpointClusterer implements Function<BreakpointEvidence, Iterator<BreakpointEvidence>> {
        private final int staleEventDistance;
        private final SortedMap<BreakpointEvidence, Boolean> locMap = new TreeMap<>();
        private final List<Map.Entry<BreakpointEvidence, Boolean>> reportableEntries = new ArrayList<>(2*MIN_EVIDENCE);
        private int currentContig = -1;

        @VisibleForTesting static final int MIN_EVIDENCE = 15; // minimum evidence count in a cluster

        BreakpointClusterer( final int staleEventDistance ) {
            this.staleEventDistance = staleEventDistance;
        }

        public Iterator<BreakpointEvidence> apply( final BreakpointEvidence evidence ) {
            if ( evidence.getContigIndex() != currentContig ) {
                currentContig = evidence.getContigIndex();
                locMap.clear();
            }

            locMap.put(evidence, true);

            final int locusStart = evidence.getContigStart();
            final int locusEnd = evidence.getContigEnd();
            final int staleEnd = locusStart - staleEventDistance;
            int evidenceCount = 0;
            reportableEntries.clear();
            final Iterator<Map.Entry<BreakpointEvidence, Boolean>> itr = locMap.entrySet().iterator();
            while ( itr.hasNext() ) {
                final Map.Entry<BreakpointEvidence, Boolean> entry = itr.next();
                final BreakpointEvidence evidence2 = entry.getKey();
                final int contigEnd = evidence2.getContigEnd();
                if ( contigEnd <= staleEnd ) itr.remove();
                else if ( evidence2.getContigStart() >= locusEnd ) break;
                else if ( contigEnd > locusStart ) {
                    evidenceCount += 1;
                    if ( entry.getValue() ) reportableEntries.add(entry);
                }
            }

            if ( evidenceCount >= MIN_EVIDENCE ) {
                return reportableEntries.stream()
                        .map(entry -> { entry.setValue(false); return entry.getKey(); })
                        .iterator();
            }
            return Collections.emptyIterator();
        }
    }

    /**
     * Class to fully sort a stream of nearly sorted BreakpointEvidences.
     */
    @VisibleForTesting static final class WindowSorter implements Function<BreakpointEvidence, Iterator<BreakpointEvidence>> {
        private final SortedSet<BreakpointEvidence> recordSet = new TreeSet<>();
        private final List<BreakpointEvidence> reportableEvidence = new ArrayList<>();
        private final int windowSize;
        private int currentContig = -1;

        WindowSorter( final int windowSize ) {
            this.windowSize = windowSize;
        }

        public Iterator<BreakpointEvidence> apply( final BreakpointEvidence evidence ) {
            reportableEvidence.clear();
            if ( evidence.getContigIndex() != currentContig ) {
                reportableEvidence.addAll(recordSet);
                recordSet.clear();
                currentContig = evidence.getContigIndex();
            } else {
                final int reportableEnd = evidence.getContigStart() - windowSize;
                final Iterator<BreakpointEvidence> itr = recordSet.iterator();
                while ( itr.hasNext() ) {
                    final BreakpointEvidence evidence2 = itr.next();
                    if ( evidence2.getContigStart() >= reportableEnd ) break;
                    reportableEvidence.add(evidence2);
                    itr.remove();
                }
            }
            recordSet.add(evidence);
            return reportableEvidence.iterator();
        }
    }

    /**
     * Minimalistic simple interval.
     */
    @VisibleForTesting static final class Interval implements Serializable {
        private static final long serialVersionUID = 1L;
        private final int contig;
        private final int start;
        private final int end;

        public Interval( final int contig, final int start, final int end ) {
            this.contig = contig;
            this.start = start;
            this.end = end;
        }

        public int getContig() { return contig; }
        public int getStart() { return start; }
        public int getEnd() { return end; }

        public boolean isDisjointFrom( final Interval that ) {
            return this.contig != that.contig || this.end < that.start || that.end < this.start;
        }

        public Interval join( final Interval that ) {
            if ( this.contig != that.contig ) throw new GATKException("Joining across contigs.");
            return new Interval(contig, Math.min(this.start, that.start), Math.max(this.end, that.end));
        }
    }

    /**
     * A class to examine a stream of BreakpointEvidence, and group it into Intervals.
     */
    @VisibleForTesting static final class IntervalMapper implements Function<BreakpointEvidence, Iterator<Interval>> {
        private final int gapSize;
        private int contig = -1;
        private int start;
        private int end;

        IntervalMapper( final int gapSize ) {
            this.gapSize = gapSize;
        }

        public Iterator<Interval> apply( final BreakpointEvidence evidence ) {
            Iterator<Interval> result = Collections.emptyIterator();
            if ( evidence.getContigIndex() != contig ) {
                if ( contig != -1 ) {
                    result = Collections.singletonList(new Interval(contig, start, end)).iterator();
                }
                contig = evidence.getContigIndex();
                start = evidence.getContigStart();
                end = evidence.getContigEnd();
            } else if ( evidence.getContigStart() >= end+gapSize ) {
                result = Collections.singletonList(new Interval(contig, start, end)).iterator();
                start = evidence.getContigStart();
                end = evidence.getContigEnd();
            } else {
                end = Math.max(end, evidence.getContigEnd());
            }
            return result;
        }
    }

    /**
     * A template name and an intervalId.
     * Note:  hashCode does not depend on intervalId, and that's on purpose.
     * This is actually a compacted (K,V) pair, and the hashCode is on K only.
     */
    @VisibleForTesting static final class QNameAndInterval implements Serializable {
        private static final long serialVersionUID = 1L;
        private final byte[] qName;
        private final int hashVal;
        private final int intervalId;

        QNameAndInterval( final String qName, final int intervalId ) {
            this.qName = qName.getBytes();
            this.hashVal = qName.hashCode();
            this.intervalId = intervalId;
        }

        public String getQName() { return new String(qName); }
        public int getIntervalId() { return intervalId; }

        @Override
        public int hashCode() { return hashVal; }

        @Override
        public boolean equals( final Object obj ) {
            return obj instanceof QNameAndInterval && equals((QNameAndInterval) obj);
        }

        public boolean equals( final QNameAndInterval that ) {
            return Arrays.equals(this.qName, that.qName) && this.intervalId == that.intervalId;
        }

        public boolean sameName( final byte[] name ) { return Arrays.equals(qName,name); }
    }

    /**
     * Class to find the template names associated with reads in specified intervals.
     */
    @VisibleForTesting static final class QNameFinder implements Function<GATKRead, Iterator<QNameAndInterval>> {
        private final ReadMetadata metadata;
        private final List<Interval> intervals;
        private int intervalsIndex = 0;

        QNameFinder( final ReadMetadata metadata, final List<Interval> intervals ) {
            this.metadata = metadata;
            this.intervals = intervals;
        }

        @Override
        public Iterator<QNameAndInterval> apply( final GATKRead read ) {
            final int readContigId = metadata.getContigID(read.getContig());
            final int readStart = read.getUnclippedStart();
            final int intervalsSize = intervals.size();
            while ( intervalsIndex < intervalsSize ) {
                final Interval interval = intervals.get(intervalsIndex);
                if ( interval.getContig() > readContigId ) break;
                if ( interval.getContig() == readContigId && interval.getEnd() > read.getStart() ) break;
                intervalsIndex += 1;
            }
            if ( intervalsIndex >= intervalsSize ) return new SVUtils.SingletonIterator<>();
            final Interval indexedInterval = intervals.get(intervalsIndex);
            final Interval readInterval = new Interval(readContigId, readStart, read.getUnclippedEnd());
            if ( indexedInterval.isDisjointFrom(readInterval) ) return new SVUtils.SingletonIterator<>();
            return new SVUtils.SingletonIterator<>(new QNameAndInterval(read.getName(), intervalsIndex));
        }
    }

    /**
     * A <SVKmer,IntervalId> pair.
     * Note:  hashCode is not overridden, and thus does not depend on intervalId, and that's on purpose.
     * This is actually a compacted (K,V) pair, and the hashCode is on K only.
     */
    @VisibleForTesting final static class KmerAndInterval extends SVKmer {
        private static final long serialVersionUID = 1L;
        private final int intervalId;

        KmerAndInterval( final SVKmer kmer, final int intervalId ) {
            super(kmer);
            this.intervalId = intervalId;
        }

        @Override
        public boolean equals( final Object obj ) {
            return obj instanceof KmerAndInterval && equals((KmerAndInterval)obj);
        }

        public final boolean equals( final KmerAndInterval that ) {
            return equals((SVKmer)that) && this.intervalId == that.intervalId;
        }

        public int getIntervalId() { return intervalId; }
    }

    /**
     * Class that acts as a mapper from a stream of reads to a stream of KmerAndIntervals.
     * The template names of reads to kmerize, along with a set of kmers to ignore are passed in (by broadcast).
     */
    @VisibleForTesting static final class QNameKmerizer implements Function<GATKRead, Iterator<KmerAndInterval>> {
        private final HopscotchHashSet<QNameAndInterval> qNameAndIntervalSet;
        private final Set<SVKmer> kmersToIgnore;

        QNameKmerizer( final HopscotchHashSet<QNameAndInterval> qNameAndIntervalSet,
                              final Set<SVKmer> kmersToIgnore ) {
            this.qNameAndIntervalSet = qNameAndIntervalSet;
            this.kmersToIgnore = kmersToIgnore;
        }

        public Iterator<KmerAndInterval> apply( final GATKRead read ) {
            final String qName = read.getName();
            final int qNameHash = qName.hashCode();
            final byte[] qNameBytes = qName.getBytes();
            final Iterator<QNameAndInterval> names = qNameAndIntervalSet.bucketIterator(qNameHash);
            while ( names.hasNext() ) {
                final QNameAndInterval qNameAndInterval = names.next();
                if ( qNameAndInterval.hashCode() == qNameHash && qNameAndInterval.sameName(qNameBytes) ) {
                    final int nKmers = read.getLength() - SVConstants.KMER_SIZE + 1;
                    return SVKmerizer.stream(read.getBases(), SVConstants.KMER_SIZE)
                            .map(kmer -> kmer.canonical(SVConstants.KMER_SIZE))
                            .filter(kmer -> !kmersToIgnore.contains(kmer))
                            .map(kmer -> new KmerAndInterval(kmer, qNameAndInterval.getIntervalId()))
                            .collect(Collectors.toCollection(() -> new ArrayList<>(nKmers)))
                            .iterator();
                }
            }
            return Collections.emptyIterator();
        }
    }

    /**
     * Class to represent an intervalId and read data necessary to write FASTQ.
     */
    @VisibleForTesting static final class IntervalAndRead implements Serializable {
        private static final long serialVersionUID = 1L;
        private final int intervalId;
        private final String readName;
        private final byte[] readSeq;
        private final byte[] readQuals;

        IntervalAndRead( final int intervalId, final GATKRead read ) {
            this.intervalId = intervalId;
            this.readName = !read.isPaired() ? read.getName() : read.getName() + (read.isSecondOfPair() ? "/2" : "/1");
            this.readSeq = read.getBases();
            this.readQuals = read.getBaseQualities();
        }

        public int getIntervalId() { return intervalId; }
        public String getReadName() { return readName; }
        public byte[] getReadSeq() { return readSeq; }
        public byte[] getReadQuals() { return readQuals; }

        @Override
        public int hashCode() { return intervalId; }

        @Override
        public boolean equals( final Object obj ) {
            return obj instanceof IntervalAndRead && equals((IntervalAndRead)obj);
        }

        public boolean equals( final IntervalAndRead that ) {
            return this.intervalId == that.intervalId && this.readName == that.readName;
        }

        @Override
        public String toString() {
            return "@"+intervalId+":"+readName+"\n"+
                    new String(readSeq)+"\n"+
                    "+\n"+
                    SAMUtils.phredToFastq(readQuals);
        }
    }

    /**
     * Class that acts as a mapper from a stream of reads to a stream of <intervalId,read> pairs.
     * It knows which breakpoint(s) a read belongs to (if any) by kmerizing the read, and looking up each SVKmer in
     * a multi-map of SVKmers onto breakpoints.
     */
    @VisibleForTesting static final class ReadsForIntervalFinder
            implements Function<GATKRead, Iterator<IntervalAndRead>> {
        private final HopscotchHashSet<KmerAndInterval> kmerAndIntervalSet;

        ReadsForIntervalFinder(final HopscotchHashSet<KmerAndInterval> kmerAndIntervalSet ) {
            this.kmerAndIntervalSet = kmerAndIntervalSet;
        }

        public Iterator<IntervalAndRead> apply( final GATKRead read ) {
            final Set<Integer> intervalIds = SVKmerizer.stream(read.getBases(), SVConstants.KMER_SIZE)
                    .map(kmer -> kmer.canonical(SVConstants.KMER_SIZE))
                    .flatMap(this::kmerToIntervals)
                    .collect(Collectors.toCollection(HashSet::new));
            final int nBreakpoints = intervalIds.size();
            return intervalIds
                    .stream()
                    .map(intervalId -> new IntervalAndRead(intervalId, read))
                    .collect(Collectors.toCollection(() -> new ArrayList<>(nBreakpoints)))
                    .iterator();
        }

        private Stream<Integer> kmerToIntervals(final SVKmer kmer ) {
            List<Integer> intervalIds = new ArrayList<>(2);
            Iterator<KmerAndInterval> itr = kmerAndIntervalSet.bucketIterator(kmer.hashCode());
            while ( itr.hasNext() ) {
                final KmerAndInterval kmerAndInterval = itr.next();
                if ( kmer.equals(kmerAndInterval) ) intervalIds.add(kmerAndInterval.getIntervalId());
            }
            return intervalIds.stream();
        }
    }

    static {
        GATKRegistrator.registerRegistrator(kryo -> {
                kryo.register(Interval.class);
                kryo.register(QNameAndInterval.class);
                kryo.register(KmerAndInterval.class);
            });
    }
}
