package io.dakotaoss.delta.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.dakotaoss.delta.schema.SchemaEvolution.Policy;
import io.dakotaoss.delta.schema.SchemaEvolution.Result;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Additive schema diff: which new columns to ALTER ADD, and what counts as a breaking change. */
class SchemaEvolutionTest {

  private static StructType current() {
    return new StructType()
        .add("id", IntegerType.INTEGER, false)
        .add("name", StringType.STRING, true);
  }

  @Test
  void policyParsing() {
    assertEquals(Policy.NONE, Policy.from("none"));
    assertEquals(Policy.ADD, Policy.from("add"));
    assertEquals(Policy.NONE, Policy.from("bogus"));
  }

  @Test
  void identicalSchemaAddsNothingAndIsNotBreaking() {
    StructType incoming =
        new StructType().add("id", IntegerType.INTEGER, false).add("name", StringType.STRING, true);
    Result r = SchemaEvolution.diff(current(), incoming);
    assertFalse(r.breaking);
    assertFalse(r.changed());
    assertTrue(r.addedColumns.isEmpty());
  }

  @Test
  void newNullableColumnsAreReportedInOrder() {
    StructType incoming =
        new StructType()
            .add("id", IntegerType.INTEGER, false)
            .add("name", StringType.STRING, true)
            .add("email", StringType.STRING, true)
            .add("phone", StringType.STRING, true);
    Result r = SchemaEvolution.diff(current(), incoming);
    assertFalse(r.breaking);
    assertTrue(r.changed());
    assertEquals(List.of("email", "phone"), r.addedColumns);
  }

  @Test
  void newNonNullableColumnIsBreaking() {
    StructType incoming =
        new StructType()
            .add("id", IntegerType.INTEGER, false)
            .add("name", StringType.STRING, true)
            .add("email", StringType.STRING, false); // can't back-fill a NOT NULL column
    assertTrue(SchemaEvolution.diff(current(), incoming).breaking);
  }

  @Test
  void existingColumnTypeChangeIsBreaking() {
    StructType incoming =
        new StructType().add("id", LongType.LONG, false).add("name", StringType.STRING, true);
    assertTrue(SchemaEvolution.diff(current(), incoming).breaking);
  }

  @Test
  void droppingAnExistingColumnIsBreaking() {
    StructType incoming = new StructType().add("id", IntegerType.INTEGER, false);
    assertTrue(SchemaEvolution.diff(current(), incoming).breaking);
  }
}
