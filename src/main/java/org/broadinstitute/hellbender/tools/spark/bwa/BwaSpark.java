package org.broadinstitute.hellbender.tools.spark.bwa;

import com.github.lindenb.jbwa.jni.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;

import java.io.File;
import java.io.IOException;

@CommandLineProgramProperties(summary = "Runs BWA",
        oneLineSummary = "BWA on Spark",
        programGroup = SparkProgramGroup.class)
public final class BwaSpark extends GATKSparkTool {

    private static final long serialVersionUID = 1L;

    @Argument(doc = "the reference", shortName = "ref",
            fullName = "ref", optional = false)
    private String ref;

    @Argument(doc = "the input fasta", shortName = "fasta",
            fullName = "fasta", optional = false)
    private String fasta;

    @Argument(doc = "the output bam", shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, optional = false)
    private String output;

    @Override
    protected void runTool(final JavaSparkContext ctx) {
        System.out.println("hello");
        System.loadLibrary("bwajni");
        try {
            BwaIndex index = new BwaIndex(new File(ref));
            BwaMem mem = new BwaMem(index);
            KSeq kseq = new KSeq(new File(fasta));

            ShortRead read;
            while((read=kseq.next())!=null)
            {
                for(AlnRgn a: mem.align(read))
                {
                    if(a.getSecondary()>=0) continue;
                    System.out.println(
                            read.getName()+"\t"+
                                    a.getStrand()+"\t"+
                                    a.getChrom()+"\t"+
                                    a.getPos()+"\t"+
                                    a.getMQual()+"\t"+
                                    a.getCigar()+"\t"+
                                    a.getNm()
                    );
                }
            }
            kseq.dispose();
            index.close();
            mem.dispose();
        } catch (IOException e) {
            throw new GATKException(e.toString());
        }
    }
}
