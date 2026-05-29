package io.dakotaoss.delta.uc;

import io.dakotaoss.delta.model.TableTarget;

/** Resolves a Kafka topic to a concrete Delta {@link TableTarget} (path + access config). */
public interface TableResolver {
  TableTarget resolve(String topic);
}
