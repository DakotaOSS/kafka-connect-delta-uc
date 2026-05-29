package io.dakotaoss.delta;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Kafka Connect {@link SinkConnector} entry point for the DeltaTables sink. */
public final class DeltaSinkConnector extends SinkConnector {

  private Map<String, String> props;

  @Override
  public void start(Map<String, String> props) {
    this.props = props;
    // Validate eagerly so misconfiguration fails at connector start, not first record.
    new DeltaSinkConfig(props);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return DeltaSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> configs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; i++) {
      configs.add(new HashMap<>(props));
    }
    return configs;
  }

  @Override
  public void stop() {
    // no shared resources held at the connector level
  }

  @Override
  public ConfigDef config() {
    return DeltaSinkConfig.CONFIG_DEF;
  }

  @Override
  public String version() {
    return DeltaSinkTask.VERSION;
  }
}
