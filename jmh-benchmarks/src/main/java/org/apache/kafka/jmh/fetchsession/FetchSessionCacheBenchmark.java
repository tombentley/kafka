/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.jmh.fetchsession;

import kafka.server.FetchContext;
import kafka.server.FetchManager;
import kafka.server.FetchSessionCache;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class FetchSessionCacheBenchmark {

    public static final int NUM_THREADS = 40;

    private FetchManager manager;

    @Setup(Level.Trial)
    public void setUp() {
        FetchSessionCache cache = new FetchSessionCache(NUM_THREADS - 2, 100);
        manager = new FetchManager(Time.SYSTEM, cache);
    }

    @State(Scope.Thread)
    public static class Sess {
        private final Random rng;
        int sessionId;
        int epoch;
        Map<TopicPartition, FetchRequest.PartitionData> partitions;

        public Sess() {
            this.rng = new Random();
            epoch = 0;
        }

        int nextInt(int startInc, int endExcl) {
            return startInc + rng.nextInt(endExcl - startInc);
        }
    }

    @Benchmark
    @Threads(NUM_THREADS)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public Object maybeCreateSession(Sess sess) {
        // Create a new session with some probability
        if (sess.epoch == 0) {
            return newSession(sess);
        } else if (sess.epoch == 100/*sess.nextInt(0, 100) < 3*/) {
            return endSession(sess);
        } else {
            return updateSession(sess);
        }
    }

    private FetchResponse<Records> updateSession(Sess sess) {
        FetchContext fetchContext = manager.newContext(new FetchMetadata(sess.sessionId, sess.epoch), sess.partitions, Collections.emptyList(), false);
        FetchResponse<Records> recordsFetchResponse = fetchContext.updateAndGenerateResponseData(
                sess.partitions.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                            e -> new FetchResponse.PartitionData<>(Errors.NONE, 0, 0, 0, Collections.emptyList(), MemoryRecords.readableRecords(ByteBuffer.allocate(1))),
                            (x, y) -> y,
                            LinkedHashMap::new)));
        checkError(recordsFetchResponse);
        ++sess.epoch;
        sess.partitions = sess.partitions.keySet().stream().collect(Collectors.toMap(
            c -> new TopicPartition(c.topic(), c.partition()),
            c -> new FetchRequest.PartitionData(1, 1, 1, Optional.of(2))
        ));
        return recordsFetchResponse;
    }

    private FetchResponse<Records> newSession(Sess sess) {
        // With a random size
        int size = sess.nextInt(10, 31);
        HashMap<TopicPartition, FetchRequest.PartitionData> partitions = new HashMap<>();
        for (int i = 0; i < size; i++) {
            partitions.put(new TopicPartition("test", i), new FetchRequest.PartitionData(0, 0, 10, Optional.empty()));
        }
        FetchContext fetchContext = manager.newContext(new FetchMetadata(FetchMetadata.INVALID_SESSION_ID, FetchMetadata.INITIAL_EPOCH), partitions, Collections.emptyList(), false);
        FetchResponse<Records> recordsFetchResponse = fetchContext.updateAndGenerateResponseData(
                ((Map<TopicPartition, FetchRequest.PartitionData>) partitions).entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                            e -> new FetchResponse.PartitionData<>(Errors.NONE, 0, 0, 0, Collections.emptyList(), MemoryRecords.readableRecords(ByteBuffer.allocate(1))),
                            (x, y) -> y,
                            LinkedHashMap::new)));
        checkError(recordsFetchResponse);
        sess.sessionId = recordsFetchResponse.sessionId();
        sess.epoch = 1;
        sess.partitions = ((Map<TopicPartition, FetchRequest.PartitionData>) partitions).keySet().stream().collect(Collectors.toMap(
            c -> new TopicPartition(c.topic(), c.partition()),
            c -> new FetchRequest.PartitionData(1, 1, 1, Optional.of(2))
        ));
        return recordsFetchResponse;
    }

    private void checkError(FetchResponse<Records> recordsFetchResponse) {
        if (recordsFetchResponse.error() != Errors.NONE
                && recordsFetchResponse.error() != Errors.FETCH_SESSION_ID_NOT_FOUND) {
            System.err.println(recordsFetchResponse.error());
            Exit.exit(1);
        }
    }

    private FetchResponse<Records> endSession(Sess sess) {
        FetchContext fetchContext = manager.newContext(new FetchMetadata(sess.sessionId, FetchMetadata.FINAL_EPOCH), sess.partitions, Collections.emptyList(), false);
        FetchResponse<Records> recordsFetchResponse = fetchContext.updateAndGenerateResponseData(
                sess.partitions.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                            e -> new FetchResponse.PartitionData<>(Errors.NONE, 0, 0, 0, Collections.emptyList(), MemoryRecords.readableRecords(ByteBuffer.allocate(1))),
                            (x, y) -> y,
                            LinkedHashMap::new)));
        checkError(recordsFetchResponse);
        sess.sessionId = 0;
        sess.epoch = 0;
        sess.partitions = Collections.emptyMap();
        return recordsFetchResponse;
    }

    public static void main(String[] args) throws Exception {
//        org.openjdk.jmh.Main.main(new String[]{"-l", "org.apache.kafka.jmh.fetchsession.FetchSessionCacheBenchmark.*"});
        FetchSessionCacheBenchmark bmk = new FetchSessionCacheBenchmark();
        bmk.setUp();
        Sess sess1 = new Sess();
        Sess sess2 = new Sess();
        bmk.maybeCreateSession(sess1);
        bmk.maybeCreateSession(sess1);
        bmk.maybeCreateSession(sess2);
        bmk.maybeCreateSession(sess2);
        bmk.maybeCreateSession(sess1);
        bmk.maybeCreateSession(sess1);
        bmk.maybeCreateSession(sess2);
    }
}
