package org.jag.hll.dsketch;

import java.io.Serializable;
import org.apache.commons.lang3.StringUtils;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.memory.Memory;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Aggregator;

/** User defined function that returns a HLL structure implementation using sketches */
public class DatasketchesHllAggregator extends Aggregator<String, byte[], byte[]>
    implements Serializable {
  private static final long serialVersionUID = 7736442229150756647L;

  public static final String HLL_FUNCTION_NAME = "hll_of";
  private final int logK;
  private final TgtHllType tgtHllType;

  public DatasketchesHllAggregator() {
    this(10, TgtHllType.HLL_4);
  }

  public DatasketchesHllAggregator(int logK, TgtHllType tgtHllType) {
    this.logK = logK;
    this.tgtHllType = tgtHllType;
  }

  @Override
  public byte[] zero() {
    HllSketch emptyHll = new HllSketch(logK, tgtHllType);
    return emptyHll.toUpdatableByteArray();
  }

  @Override
  public byte[] reduce(byte[] b, String elem) {
    if (StringUtils.isBlank(elem)) {
      elem = "";
    }
    HllSketch hllToUpdate = HllSketch.heapify(Memory.wrap(b));
    hllToUpdate.update(elem);
    return hllToUpdate.toUpdatableByteArray();
  }

  @Override
  public byte[] merge(byte[] b1, byte[] b2) {
    HllSketch h1 = HllSketch.heapify(Memory.wrap(b1));
    HllSketch h2 = HllSketch.heapify(Memory.wrap(b2));
    Union union = new Union(logK);
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

  public static class EstimateHllUdf implements UDF1<byte[], Double> {

    private static final long serialVersionUID = 7784941089239172250L;

    public EstimateHllUdf() {}

    @Override
    public Double call(byte[] hll) throws Exception {
      return HllSketch.heapify(Memory.wrap(hll)).getEstimate();
    }
  }
}
