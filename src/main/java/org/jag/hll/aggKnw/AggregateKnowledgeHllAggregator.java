package org.jag.hll.aggKnw;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import net.agkn.hll.HLL;
import net.agkn.hll.HLLType;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Aggregator;

/**
 * User defined function that returns a HLL structure implementation using <a
 * href="https://github.com/aggregateknowledge/java-hll">...</a> <a
 */
public class AggregateKnowledgeHllAggregator extends Aggregator<String, byte[], byte[]>
    implements Serializable {
  private static final long serialVersionUID = 7736442229150756647L;
  private HashFunction hashFunction;

  // postgres defaults = hll(log2m=11, regwidth=5, expthresh=-1, sparseon=1)
  // from https://github.com/citusdata/postgresql-hll

  private int log2m = 13;
  private int regWidth = 4;
  private int explicitThreshold = -1;

  private boolean sparseOn = true;
  private HLLType startingHllType = HLLType.EMPTY;

  /**
   * default value inspired from GOOD_FAST_HASH_SEED in {@link Hashing} For hll union to produce
   * correct values, seed has to be same across runs! so please set seed number for the runs. from
   * docs: https://github.com/aggregateknowledge/java-hll The seed to the hash call must remain
   * constant for all inputs to a given HLL. Similarly, if one plans to compute the union of two
   * HLLs, the input values must have been hashed using the same seed.
   */
  private int seed = (int) today.toInstant().toEpochMilli();

  private static final ZonedDateTime today;

  static {
    today = LocalDate.now(ZoneId.of("UTC")).atStartOfDay(ZoneOffset.UTC);
  }

  public AggregateKnowledgeHllAggregator() {
    this.hashFunction = Hashing.murmur3_128(seed);
  }

  @Override
  public byte[] zero() {
    HLL hll = new HLL(log2m, regWidth, explicitThreshold, sparseOn, startingHllType);
    return hll.toBytes();
  }

  @Override
  public byte[] reduce(byte[] b, String elem) {
    if (StringUtils.isBlank(elem)) {
      elem = "";
    }
    final long hashCode = this.hashFunction.hashString(elem, Charset.defaultCharset()).asLong();

    HLL hll = HLL.fromBytes(b);
    hll.addRaw(hashCode);
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

  public int getSeed() {
    return seed;
  }

  public void setSeed(int seed) {
    this.seed = seed;
    this.hashFunction = Hashing.murmur3_128(seed);
  }

  public static class EmptyHllUdf implements UDF0<byte[]> {

    private static final long serialVersionUID = -8226129112052886126L;
    private final byte[] empty;

    public EmptyHllUdf() {
      this(13, 4, -1);
    }

    public EmptyHllUdf(int log2m, int regWidth, int explicitThreshold) {
      HLL emptyHLL = new HLL(log2m, regWidth, explicitThreshold, true, HLLType.EMPTY);
      empty = emptyHLL.toBytes();
    }

    @Override
    public byte[] call() throws Exception {
      return empty;
    }
  }

  public static class EstimateHllUdf implements UDF1<byte[], Long> {

    private static final long serialVersionUID = 863977446597473997L;

    public EstimateHllUdf() {}

    @Override
    public Long call(byte[] hll) throws Exception {
      return HLL.fromBytes(hll).cardinality();
    }
  }

  public static class GetHllBytesUdf implements UDF1<String, byte[]> {

    private static final long serialVersionUID = 863977446597473997L;

    private HashFunction hf = Hashing.murmur3_128(32);

    public GetHllBytesUdf() {}

    @Override
    public byte[] call(String inputElem) throws Exception {
      HLL h = new HLL(13, 4, -1, true, HLLType.EMPTY);
      h.addRaw(hf.hashString(inputElem, StandardCharsets.UTF_8).asLong());
      return h.toBytes();
    }
  }
}
