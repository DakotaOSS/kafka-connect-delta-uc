package io.dakotaoss.delta.verify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dakotaoss.delta.data.Iters;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test helper. Reads a Delta table back from a local path: parse {@code _delta_log} commit JSON for
 * live AddFile entries, then read those Parquet files via the Kernel {@link Engine} ParquetHandler.
 * Local filesystem only; the production write path is abfss.
 */
public final class DeltaTableReader {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private DeltaTableReader() {}

  /** Read all live rows, returned as Object[] in {@code readSchema} column order. */
  public static List<Object[]> readRows(Engine engine, String localTablePath, StructType readSchema)
      throws IOException {
    List<FileStatus> dataFiles = liveDataFiles(localTablePath);
    List<Object[]> rows = new ArrayList<>();
    // Kernel 4.2 wraps each batch in a FileReadResult (batch + source path); unwrap via getData().
    CloseableIterator<io.delta.kernel.engine.FileReadResult> batches =
        engine
            .getParquetHandler()
            .readParquetFiles(Iters.closeable(dataFiles.iterator()), readSchema, Optional.empty());
    try {
      while (batches.hasNext()) {
        ColumnarBatch batch = batches.next().getData();
        int size = batch.getSize();
        int cols = readSchema.length();
        for (int r = 0; r < size; r++) {
          Object[] row = new Object[cols];
          for (int c = 0; c < cols; c++) {
            row[c] = value(batch.getColumnVector(c), readSchema.at(c).getDataType(), r);
          }
          rows.add(row);
        }
      }
    } finally {
      batches.close();
    }
    return rows;
  }

  public static long countRows(Engine engine, String localTablePath, StructType readSchema)
      throws IOException {
    return readRows(engine, localTablePath, readSchema).size();
  }

  private static List<FileStatus> liveDataFiles(String localTablePath) throws IOException {
    Path logDir = Paths.get(localTablePath, "_delta_log");
    List<FileStatus> added = new ArrayList<>();
    List<Path> commits;
    try (Stream<Path> s = Files.list(logDir)) {
      commits =
          s.filter(p -> p.getFileName().toString().endsWith(".json"))
              .sorted()
              .collect(Collectors.toList());
    }
    for (Path commit : commits) {
      for (String line : Files.readAllLines(commit)) {
        if (line.isEmpty()) {
          continue;
        }
        JsonNode node = MAPPER.readTree(line);
        JsonNode add = node.get("add");
        if (add != null) {
          String rel = URLDecoder.decode(add.get("path").asText(), StandardCharsets.UTF_8.name());
          long size = add.has("size") ? add.get("size").asLong() : 0L;
          added.add(FileStatus.of(localTablePath + "/" + rel, size, 0L));
        }
        // connector is append-only, so ignore "remove" entries
      }
    }
    return added;
  }

  private static Object value(ColumnVector v, DataType type, int r) {
    if (v.isNullAt(r)) {
      return null;
    }
    if (type instanceof IntegerType || type instanceof DateType) {
      return v.getInt(r);
    } else if (type instanceof LongType || type instanceof TimestampType) {
      return v.getLong(r);
    } else if (type instanceof StringType) {
      return v.getString(r);
    } else if (type instanceof BooleanType) {
      return v.getBoolean(r);
    } else if (type instanceof DoubleType) {
      return v.getDouble(r);
    } else if (type instanceof FloatType) {
      return v.getFloat(r);
    } else if (type instanceof ShortType) {
      return v.getShort(r);
    } else if (type instanceof ByteType) {
      return v.getByte(r);
    } else if (type instanceof DecimalType) {
      return v.getDecimal(r);
    } else if (type instanceof BinaryType) {
      return v.getBinary(r);
    }
    throw new UnsupportedOperationException("Unsupported read type: " + type);
  }
}
