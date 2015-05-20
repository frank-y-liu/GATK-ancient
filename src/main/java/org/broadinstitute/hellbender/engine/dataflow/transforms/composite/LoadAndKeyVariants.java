package org.broadinstitute.hellbender.engine.dataflow.transforms.composite;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import htsjdk.samtools.util.Locatable;
import org.broadinstitute.hellbender.engine.dataflow.*;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.Read;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;
import org.broadinstitute.hellbender.utils.variant.Variant;

import java.net.URI;
import java.util.List;

/**
 * Created by davidada on 5/19/15.
 */
public class LoadAndKeyVariants extends PTransform<PCollection<Read>, PCollection<KV<Read, Iterable<Variant>>>> {
    private final List<String> variantSources;
    private final Pipeline pipeline;

    public LoadAndKeyVariants(final List<String> variantSources, final Pipeline pipeline ) {
        this.variantSources = variantSources;
        this.pipeline = pipeline;
    }

    @Override
    public PCollection<KV<Read, Iterable<Variant>>> apply(PCollection<Read> input) {
        VariantsSource source = new VariantsSource(variantSources, pipeline);
        PCollection<SimpleInterval> readIntervals = input.apply(new ReadToIntervalDataflowTransform());
        // Dear god why?
        PCollectionList<Variant> variants = readIntervals.apply(
                new DoFn<SimpleInterval, Variant>() {
                    @Override
                    public void processElement(ProcessContext c) throws Exception {
                        for (Variant vv : source.query(c.element())) {
                            
                        }

                    }
                                                                    source.query()
                                                                }
                PCollection < Variant > variants = source.getVariantCollection(
                        readIntervals.apply(Combine.globally(new Concatenate<>)));
        PCollection<KV<VariantShard, Variant>> variantShards =
                variants.apply(new KeyLocatableByVariantShardDataflowTransform()).apply(ParDo.of(
                        new DoFn<KV<VariantShard, Locatable>, KV<VariantShard, Variant>>() {
                            @Override
                            public void processElement(ProcessContext c) throws Exception {
                                c.output(KV.of(c.element().getKey(), (Variant) c.element().getValue()));
                                c.element();
                            }
                        }));

        PCollection<KV<VariantShard, Read>> readShards =
                input.apply(new KeyLocatableByVariantShardDataflowTransform()).apply(ParDo.of(
                        new DoFn<KV<VariantShard, Locatable>, KV<VariantShard, Read>>() {
                            @Override
                            public void processElement(ProcessContext c) throws Exception {
                                c.output(KV.of(c.element().getKey(), (Read) c.element().getValue()));
                                c.element();
                            }
                        }));

        // GroupBy VariantShard
        final TupleTag<Variant> variantTag = new TupleTag<>();
        final TupleTag<Read> readTag = new TupleTag<>();
        PCollection<KV<VariantShard, CoGbkResult>> coGbkInput = KeyedPCollectionTuple
                .of(variantTag, variantShards)
                .and(readTag, readShards).apply(CoGroupByKey.<VariantShard>create());

        // GroupBy Read
        PCollection<KV<Read, Variant>> readVariants = coGbkInput.apply(ParDo.of(
                new DoFn<KV<VariantShard, CoGbkResult>, KV<Read, Variant>>() {
                    @Override
                    public void processElement(ProcessContext c) throws Exception {
                        Iterable<Variant> variants = c.element().getValue().getAll(variantTag);
                        Iterable<Read> reads = c.element().getValue().getAll(readTag);
                        // Compute overlap.
                        for (Read r : reads) {
                            SimpleInterval readInterval = new SimpleInterval(r);
                            for (Variant v : variants) {
                                if (readInterval.overlaps(new SimpleInterval(v))) {
                                    c.output(KV.of(r, v));
                                }
                            }
                        }
                    }
                }));
        // Remove Duplicates
        return readVariants.apply(RemoveDuplicates.<KV<Read, Variant>>create()).apply(GroupByKey.<Read,Variant>create());
    }
}