package org.jag.hll.dsketch;

import java.io.Serializable;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.memory.Memory;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Aggregator;

/** Aggregator implementing a union of two hll sketches. */
public class DatasketchesHLLMergeAggregator extends Aggregator<byte[], byte[], byte[]>
    implements Serializable {
  private static final long serialVersionUID = -6236717949842155469L;
  private final int logK;

  public static final String HLL_MERGE_FUNC_NAME = "hll_merge";
  private final TgtHllType tgtHllType;

  public DatasketchesHLLMergeAggregator(int logK, TgtHllType tgtHllType) {
    if (logK < 0) {
      logK = 13;
    }
    this.logK = logK;
    this.tgtHllType = tgtHllType;
  }

  public DatasketchesHLLMergeAggregator() {
    this(13, TgtHllType.HLL_4);
  }

  @Override
  public byte[] zero() {
    HllSketch emptyHll = new HllSketch(logK, tgtHllType);
    return emptyHll.toUpdatableByteArray();
  }

  @Override
  public byte[] reduce(byte[] b, byte[] elem) {
    return merge(b, elem);
  }

  @Override
  public byte[] merge(byte[] b1, byte[] b2) {
    HllSketch h1 = HllSketch.heapify(Memory.wrap(b1));
    HllSketch h2 = HllSketch.heapify(Memory.wrap(b2));
    org.apache.datasketches.hll.Union union = new Union(logK);
    union.update(h1);
    union.update(h2);
    HllSketch unionResult = union.getResult(tgtHllType);
    return unionResult.toUpdatableByteArray();
  }

  @Override
  public byte[] finish(byte[] reduction) {
    return HllSketch.heapify(Memory.wrap(reduction)).toCompactByteArray();
  }

  @Override
  public Encoder<byte[]> bufferEncoder() {
    return Encoders.BINARY();
  }

  @Override
  public Encoder<byte[]> outputEncoder() {
    return Encoders.BINARY();
  }

  public static void registerHllMergeUdfs(SparkSession spark, int logK, TgtHllType tgtHllType) {
    DatasketchesHLLMergeAggregator DatasketchesHLLMergeAggregator =
        new DatasketchesHLLMergeAggregator(logK, tgtHllType);

    spark
        .udf()
        .register(
            HLL_MERGE_FUNC_NAME,
            org.apache.spark.sql.functions.udaf(DatasketchesHLLMergeAggregator, Encoders.BINARY()));
  }
}
