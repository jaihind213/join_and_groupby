package org.jag;

import static org.jag.SetConstants.NOMINAL_ENTRIES;

import java.io.Serializable;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.*;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

/** Aggregator implementing a union of two SET sketches. */
public class SetUnionAggregator extends Aggregator<byte[], byte[], byte[]> implements Serializable {
  private static final long serialVersionUID = -4678596214332624874L;
  private final int nominal;

  public static final String SET_UNION_UDF_FUNCTION_NAME = "union_of_sets";

  public SetUnionAggregator(int nominalEntries) {
    if (nominalEntries < 0) {
      nominalEntries = 4096;
    }
    this.nominal = nominalEntries;
  }

  public SetUnionAggregator() {
    this(NOMINAL_ENTRIES);
  }

  @Override
  public byte[] zero() {
    UpdateSketch zero = Sketches.updateSketchBuilder().setNominalEntries(nominal).build();
    return zero.compact().toByteArray();
  }

  @Override
  public byte[] reduce(byte[] b, byte[] elem) {
    return merge(b, elem);
  }

  @Override
  public byte[] merge(byte[] b1, byte[] b2) {
    CompactSketch s1 = Sketches.heapifyCompactSketch(Memory.wrap(b1));
    CompactSketch s2 = Sketches.heapifyCompactSketch(Memory.wrap(b2));
    Union union = Sketches.setOperationBuilder().setNominalEntries(nominal).buildUnion();
    return union.union(s1, s2).toByteArray();
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
}
