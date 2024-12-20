package org.jag;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Schema {

  public static StructType peopleSchema = new StructType().add("pid", DataTypes.StringType, false);

  public static StructType personLocationsSchema =
      new StructType()
          .add("pid", DataTypes.StringType, false)
          .add("locations", DataTypes.createArrayType(DataTypes.StringType), false);
  public static StructType personInterestSchema =
      new StructType()
          .add("pid", DataTypes.StringType, false)
          .add("interest", DataTypes.StringType, false);
}
