package io.dakotaoss.delta.uc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

class UcColumnMapperTest {

  @Test
  void ddlForLeafTypes() {
    Schema s =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("ts", Schema.INT64_SCHEMA)
            .build();
    assertEquals(
        "`id` INT NOT NULL, `name` STRING, `ts` BIGINT NOT NULL", UcColumnMapper.ddlColumnDefs(s));
  }

  @Test
  void ddlForNestedStructAndDecimal() {
    Schema src =
        SchemaBuilder.struct().field("lsn", Schema.INT64_SCHEMA).field("db", Schema.STRING_SCHEMA).build();
    Schema s =
        SchemaBuilder.struct().field("amount", Decimal.schema(2)).field("source", src).build();
    assertEquals(
        "`amount` DECIMAL(38,2) NOT NULL, `source` STRUCT<lsn:BIGINT NOT NULL,db:STRING NOT NULL> NOT NULL",
        UcColumnMapper.ddlColumnDefs(s));
  }

  @Test
  void rejectsArrayColumnUntilSupported() {
    Schema s =
        SchemaBuilder.struct()
            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .build();
    assertThrows(UnsupportedOperationException.class, () -> UcColumnMapper.ddlColumnDefs(s));
  }
}
