package org.jag.hll.aggKnw;

import java.io.Serializable;
import net.agkn.hll.HLL;
import net.agkn.hll.HLLType;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

/**
 * User defined function that does union of multiple HLL structures implementation using <a
 * href="https://github.com/aggregateknowledge/java-hll">...</a> <a
 */
public class AggregateKnowledgeHllUnionAggregator extends Aggregator<byte[], byte[], byte[]>
    implements Serializable {
  private static final long serialVersionUID = 7549226011302903813L;

  private int log2m = 13;
  private int regWidth = 4;
  private int explicitThreshold = -1;

  private boolean sparseOn = true;
  private HLLType startingHllType = HLLType.EMPTY;

  public AggregateKnowledgeHllUnionAggregator() {}

  @Override
  public byte[] zero() {
    HLL hll = new HLL(log2m, regWidth, explicitThreshold, sparseOn, startingHllType);
    return hll.toBytes();
  }

  @Override
  public byte[] reduce(byte[] b, byte[] elem) {
    if (elem == null) {
      elem = zero();
    }
    HLL hll = HLL.fromBytes(b);
    hll.union(HLL.fromBytes(elem));
    return hll.toBytes();
  }

  @Override
  public byte[] merge(byte[] b1, byte[] b2) {
    HLL hll1 = HLL.fromBytes(b1);
    HLL hll2 = HLL.fromBytes(b2);
    hll1.union(hll2);
    return hll1.toBytes();
  }

  @Override
  public byte[] finish(byte[] reduction) {
    return reduction;
  }

  @Override
  public Encoder<byte[]> bufferEncoder() {
    return Encoders.BINARY();
  }

  @Override
  public Encoder<byte[]> outputEncoder() {
    return Encoders.BINARY();
  }

  // setter getter

  public int getLog2m() {
    return log2m;
  }

  public void setLog2m(int log2m) {
    this.log2m = log2m;
  }

  public int getRegWidth() {
    return regWidth;
  }

  public void setRegWidth(int regWidth) {
    this.regWidth = regWidth;
  }

  public int getExplicitThreshold() {
    return explicitThreshold;
  }

  public void setExplicitThreshold(int explicitThreshold) {
    this.explicitThreshold = explicitThreshold;
  }

  public boolean isSparseOn() {
    return sparseOn;
  }

  public void setSparseOn(boolean sparseOn) {
    this.sparseOn = sparseOn;
  }

  public HLLType getStartingHllType() {
    return startingHllType;
  }

  public void setStartingHllType(HLLType startingHllType) {
    this.startingHllType = startingHllType;
  }
}
