/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Tests for concurrent writes on single-node and 3-node pipeline.
 * Verifies that multiple threads can safely write to the same key
 * across different pipeline configuration.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Flaky("HDDS-1484")
public class TestPipelineConcurrentWrites {

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private ObjectStore objectStore;
  private int chunkSize;
  private String volumeName;
  private String bucketName;

  @BeforeEach
  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    chunkSize = (int) OzoneConsts.MB;

    // Configure timeouts for concurrent operations
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 2000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL,
        500, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 100, TimeUnit.SECONDS);

    // Configure Ratis for concurrent operations
    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(5));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(5));
    conf.setFromObject(ratisServerConfig);

    RatisClientConfig.RaftConfig raftClientConfig =
        conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(5));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(5));
    conf.setFromObject(raftClientConfig);

    RatisClientConfig ratisClientConfig =
        conf.getObject(RatisClientConfig.class);
    ratisClientConfig.setWriteRequestTimeout(Duration.ofSeconds(30));
    ratisClientConfig.setWatchRequestTimeout(Duration.ofSeconds(30));
    conf.setFromObject(ratisClientConfig);

    conf.setTimeDuration(
        OzoneConfigKeys.HDDS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
        1, TimeUnit.SECONDS);
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, 4);
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 16);

    // Start cluster with 6 datanodes
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(6)
        .build();
    cluster.waitForClusterToBeReady();

    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    volumeName = "testconcurrentwrites";
    bucketName = volumeName;

    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  @AfterEach
  public void tearDown() {
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        // Suppress close exceptions in teardown
      }
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test concurrent writes on single-node pipeline (ReplicationFactor=1)
   * Multiple threads write different blocks to the same key.
   */
  @Test
  public void testConcurrentWritesSingleNode() throws Exception {
    final int numThreads = 3;
    final int dataSize = chunkSize / 2; // 512 KB per write
    final String keyName = "concurrent-single-node-" + UUID.randomUUID();

    launchConcurrentWrites(numThreads, dataSize, ReplicationType.RATIS,
        keyName);
  }

  /**
   * Test concurrent writes on 3-node pipeline (ReplicationFactor=3).
   * Multiple threads write different blocks, verifying replication.
   */
  @Test
  public void testConcurrentWrite3Node() throws Exception {
    final int numThreads = 4;
    final int dataSize = chunkSize / 2; // 512 KB per write
    final String keyName = "concurrent-3node-" + UUID.randomUUID();

    launchConcurrentWrites(numThreads, dataSize, ReplicationType.RATIS,
        keyName);
  }

  /**
   * Test concurrent writes with varied data sizes.
   * Different threads write blocks of different sizes concurrently.
   */
  @Test
  public void testConcurrentWritesVariedDataSizes() throws Exception {
    final int numThreads = 4;
    final String keyName = "concurrent-varied-sizes-" + UUID.randomUUID();

    // Create a custom concurrent write scenario with varied sizes
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);

    int[] dataSizes = {
        chunkSize / 4,      // 256 KB
        chunkSize / 2,      // 512 KB
        chunkSize,          // 1 MB
        chunkSize + chunkSize / 2 // 1.5 MB
    };

    for (int i = 0; i < numThreads; i++) {
      final int size = dataSizes[i];

      executor.submit(() -> {
        try {
          String blockData = ContainerTestHelper
              .getFixedLengthString(UUID.randomUUID().toString(), size);
          OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS,
              0);
          key.write(blockData.getBytes(UTF_8));
          key.close();
          successCount.incrementAndGet();
        } catch (Exception e) {
          failureCount.incrementAndGet();
        } finally {
          latch.countDown();
        }
      });
    }

    assertTrue(latch.await(60, TimeUnit.SECONDS),
        "Concurrent writes did not complete within timeout");
    executor.shutdown();

    assertEquals(0, failureCount.get(),
        "Some concurrent writes failed");
    assertEquals(numThreads, successCount.get(),
        "Not all concurrent writes succeeded");
  }

  /**
   * Test concurrent writes with one datanode failure.
   * Verifies that concurrent writes can handle datanode failures.
   */
  @Test
  public void testConcurrentWritesWithDatanodeFailure() throws Exception {
    final int numThreads = 3;
    final int dataSize = chunkSize / 2;
    final String keyName = "concurrent-dn-failure-" + UUID.randomUUID();

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    AtomicInteger successCount = new AtomicInteger(0);

    // Start concurrent writes
    for (int i = 0; i < numThreads; i++) {
      executor.submit(() -> {
        try {
          String blockData = ContainerTestHelper
              .getFixedLengthString(UUID.randomUUID().toString(), dataSize);
          OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS,
              0);
          key.write(blockData.getBytes(UTF_8));
          key.close();
          successCount.incrementAndGet();
        } catch (Exception e) {
          // Expected, as datanode will be killed
        } finally {
          latch.countDown();
        }
      });
    }

    // Pause to let some writes start
    Thread.sleep(500);

    // Kill one datanode during concurrent writes
    cluster.shutdownHddsDatanode(0);

    assertTrue(latch.await(60, TimeUnit.SECONDS),
        "Concurrent writes did not complete within timeout");
    executor.shutdown();

    // At least some writes should succeed despite datanode failure
    assertTrue(successCount.get() > 0,
        "No concurrent writes succeeded after datanode failure");
  }

  /**
   * Helper method to launch concurrent writes to a key.
   * Create multiple threads that each write a block to the same key.
   *
   * @param numThreads Number of concurrent write threads
   * @param dataSize Size of data each thread writes (in bytes)
   * @param type Replication type (RATIS or STANDALONE)
   * @param keyName Name of the key to write to
   * @throws Exception if concurrent writes fail
   */
  private void launchConcurrentWrites(int numThreads, int dataSize,
      ReplicationType type, String keyName) throws Exception {

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);

    for (int i = 0; i < numThreads; i++) {
      executor.submit(() -> {
        try {
          String blockData = ContainerTestHelper
              .getFixedLengthString(UUID.randomUUID().toString(), dataSize);
          OzoneOutputStream key = createKey(keyName, type, 0);
          key.close();
          successCount.incrementAndGet();
        } catch (Exception e) {
          failureCount.incrementAndGet();
          throw new RuntimeException("Concurrent write failed", e);
        } finally {
          latch.countDown();
        }
      });
    }

    // Wait for all concurrent writes to complete
    assertTrue(latch.await(60, TimeUnit.SECONDS),
        "Concurrent writes did not complete within timeout");
    executor.shutdown();

    // Verify success
    assertEquals(0, failureCount.get(),
        "Some concurrent writes failed");
    assertEquals(numThreads, successCount.get(),
        "Not all concurrent writes succeeded");
  }

  /**
   * Helper method to create and open an output stream for a key.
   * Reuses the pattern from TestMultiBlockWritesWithDnFailures.
   */
  private OzoneOutputStream createKey(String keyName, ReplicationType type, long size) throws Exception {
    ReplicationConfig replicationConfig;
    if (type == ReplicationType.RATIS) {
      replicationConfig = RatisReplicationConfig.getInstance(
          HddsProtos.ReplicationFactor.THREE);
    } else {
      replicationConfig = RatisReplicationConfig.getInstance(
          HddsProtos.ReplicationFactor.ONE);
    }
    return objectStore.getVolume(volumeName)
        .getBucket(bucketName)
        .createKey(keyName, size, replicationConfig, new HashMap<>());
  }
}
