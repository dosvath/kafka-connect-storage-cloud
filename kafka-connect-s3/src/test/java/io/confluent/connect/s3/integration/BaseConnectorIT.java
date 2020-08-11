/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.s3.integration;

import static io.confluent.connect.utils.licensing.LicenseConfigUtil.CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG;
import static io.confluent.connect.utils.licensing.LicenseConfigUtil.CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;

import io.confluent.common.utils.IntegrationTest;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

  private static final String AVRO_TOOLS_JAR = "/opt/avro/avro-tools-1.7.7.jar";
  private static final String PARQUET_TOOLS_JAR = "/opt/parquet/parquet-tools-1.8.1.jar";
  private static final String AWS_REGION = "us-west-2";

  protected static final String CONNECTOR_CLASS_NAME = "S3SinkConnector";
  protected static final String CONNECTOR_NAME = "s3-sink";
  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.MINUTES.toMillis(1);
  protected static final int S3_TIMEOUT_MINS = 1;
  protected static final int MAX_TASKS = 3;

  protected EmbeddedConnectCluster connect;
  protected Map<String, String> props;

  @Before
  public void setup() throws IOException {
    startConnect();
    setupProperties();
  }

  @After
  public void close() {
    stopConnect();
  }

  private void setupProperties() {
    props = new HashMap<>();
    props.put(CONNECTOR_CLASS_CONFIG, CONNECTOR_CLASS_NAME);
    props.put(TASKS_MAX_CONFIG, Integer.toString(MAX_TASKS));
    // converters
    props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    // license properties
    props.put(CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    props.put(CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
  }

  protected void startConnect() throws IOException {
    connect = new EmbeddedConnectCluster.Builder()
        .name("s3-connect-cluster")
        .build();

    // start the clusters
    connect.start();
  }

  protected void stopConnect() {
    // stop all Connect, Kafka and Zk threads.
    connect.stop();
  }

  /**
   * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the
   * given name to start the specified number of tasks.
   *
   * @param name     the name of the connector
   * @param numTasks the minimum number of tasks that are expected
   * @return the time this method discovered the connector has started, in milliseconds past epoch
   * @throws InterruptedException if this was interrupted
   */
  protected long waitForConnectorToStart(String name, int numTasks) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
        CONNECTOR_STARTUP_DURATION_MS,
        "Connector tasks did not start in time."
    );
    return System.currentTimeMillis();
  }

  /**
   * Confirm that a connector with an exact number of tasks is running.
   *
   * @param connectorName the connector
   * @param numTasks      the minimum number of tasks
   * @return true if the connector and tasks are in RUNNING state; false otherwise
   */
  protected Optional<Boolean> assertConnectorAndTasksRunning(String connectorName, int numTasks) {
    try {
      ConnectorStateInfo info = connect.connectorStatus(connectorName);
      boolean result = info != null
          && info.tasks().size() >= numTasks
          && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
          && info.tasks().stream()
          .allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
      return Optional.of(result);
    } catch (Exception e) {
      log.warn("Could not check connector state info.");
      return Optional.empty();
    }
  }
}
