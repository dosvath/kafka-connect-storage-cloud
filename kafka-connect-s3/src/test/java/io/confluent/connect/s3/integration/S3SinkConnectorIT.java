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

import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_BUCKET_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FORMAT_CLASS_CONFIG;

import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.storage.S3Storage;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.test.IntegrationTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IntegrationTest.class)
public class S3SinkConnectorIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(S3SinkConnectorIT.class);
  private static final String TEST_BUCKET_NAME = "confluent-kafka-connect-s3-testing";

  private static final long NUM_RECORDS_INSERT = 10;

  private static final String VALUE_TOPIC_NAME = "Values";
  private static final String KEYS_HEADERS_TOPIC_NAME = "KeysHeadersValues";

  private static final List<String> KAFKA_TOPICS = Arrays.asList(VALUE_TOPIC_NAME,
      KEYS_HEADERS_TOPIC_NAME);

  private ConnectProducer producer;

  @BeforeClass
  public static void setupBucket() throws Throwable {
    //clear bucket.

  }

  @Before
  public void before() {
    //add class specific props
    props.put(SinkConnectorConfig.TOPICS_CONFIG, String.join(",", KAFKA_TOPICS));
    props.put(S3_BUCKET_CONFIG, TEST_BUCKET_NAME);
    props.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    props.put(FLUSH_SIZE_CONFIG, "3");
    props.put("storage.class", S3Storage.class.getName());
    producer = new ConnectProducer(connect);

    // create topics in Kafka
    KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));
  }

  @AfterClass
  public static void tearDownBucket() {

  }

  @Test
  public void testBasicFilesWrittenToBucket() throws Throwable {
    //add test specific props
    props.put(SinkConnectorConfig.TOPICS_CONFIG, String.join(",", VALUE_TOPIC_NAME));

    // start sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, Math.min(KAFKA_TOPICS.size(), MAX_TASKS));

    Schema valueSchema = getExampleStructSchemaFewColumns();
    Struct val = getExampleStructValFewColumns(valueSchema);
    // Send records to Kafka
    for (long i = 0; i < NUM_RECORDS_INSERT; i++) {
      val.put("ID", i);
      producer.produce(VALUE_TOPIC_NAME, null, null, valueSchema, val);
    }

    Thread.sleep(2000);
    // TODO: wait for files in S3
    // TODO: verify file contents
  }

  private Schema getExampleStructSchemaFewColumns() {
    return SchemaBuilder.struct()
        .field("ID", Schema.INT64_SCHEMA)
        .field("myString", Schema.STRING_SCHEMA)
        .field("myBool", Schema.BOOLEAN_SCHEMA)
        .field("myBytes", Schema.BYTES_SCHEMA)
        .field("myDate", org.apache.kafka.connect.data.Date.SCHEMA)
        .field("myTime", Timestamp.SCHEMA)
        .build();
  }

  private Struct getExampleStructValFewColumns(Schema structSchema) {
    Date sampleDate = new Date(1111111);
    sampleDate.setTime(0);
    return new Struct(structSchema)
        .put("ID", (long) 0)
        .put("myString", "theStringVal")
        .put("myBool", true)
        .put("myBytes", "theBytes".getBytes())
        .put("myDate", sampleDate)
        .put("myTime", new Date(33333333));
  }

}
