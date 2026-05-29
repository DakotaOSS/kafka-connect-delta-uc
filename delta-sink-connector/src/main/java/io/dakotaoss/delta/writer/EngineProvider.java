package io.dakotaoss.delta.writer;

import io.dakotaoss.delta.model.TableTarget;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import org.apache.hadoop.conf.Configuration;

/**
 * Builds a Delta Kernel {@link Engine} for a {@link TableTarget}.
 *
 * <p>Default impl layers the target's {@code hadoopConfig()} (e.g. a UC-vended ABFS SAS token or
 * OAuth settings) onto a base {@link Configuration} and creates a {@link DefaultEngine}. Tests pass
 * a {@code file:/} path with no overrides to run the real write path against the local filesystem.
 */
public interface EngineProvider {

  Engine engineFor(TableTarget target);

  /** Default Hadoop/ABFS-backed provider. */
  static EngineProvider hadoop() {
    return new EngineProvider() {
      @Override
      public Engine engineFor(TableTarget target) {
        Configuration conf = new Configuration();
        target.hadoopConfig().forEach(conf::set);
        return DefaultEngine.create(conf);
      }
    };
  }
}
