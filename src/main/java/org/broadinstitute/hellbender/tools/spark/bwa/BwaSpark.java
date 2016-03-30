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
import java.util.ArrayList;
import java.util.List;

@CommandLineProgramProperties(summary = "Runs BWA",
        oneLineSummary = "BWA on Spark",
        programGroup = SparkProgramGroup.class)
public final class BwaSpark extends GATKSparkTool {

    private static final long serialVersionUID = 1L;

    @Argument(doc = "the reference", shortName = "ref",
            fullName = "ref", optional = false)
    private String ref;

    @Argument(doc = "fastq 1", shortName = "fq1",
            fullName = "fq1", optional = false)
    private String fq1;

    @Argument(doc = "fastq 2", shortName = "fq2",
            fullName = "fq2", optional = false)
    private String fq2;

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
            KSeq kseq1=new KSeq(new File(fq1));
            KSeq kseq2=new KSeq(new File(fq2));

            List<ShortRead> L1=new ArrayList<ShortRead>();
            List<ShortRead> L2=new ArrayList<ShortRead>();
            for(;;)
            {
                ShortRead read1=kseq1.next();
                ShortRead read2=kseq2.next();

                if(read1==null || read2==null || L1.size()>100)
                {
                    if(!L1.isEmpty())
                        for(String sam:mem.align(L1,L2))
                        {
                            System.out.print(sam);
                        }
                    if(read1==null || read2==null) break;
                    L1.clear();
                    L2.clear();
                }
                L1.add(read1);
                L2.add(read2);
            }
            kseq1.dispose();
            kseq2.dispose();
            index.close();
            mem.dispose();
        } catch (IOException e) {
            throw new GATKException(e.toString());
        }
    }
}
