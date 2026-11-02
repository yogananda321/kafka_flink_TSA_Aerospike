package com.company.flink.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enterprise Native Java High-Throughput Benchmark Runner.
 * 
 * Asynchronously produces matched IMSI events to Topic A and Topic B in parallel
 * at ultra-high speeds with configurable realistic latency:
 * Default random distribution: 70% in 5-500ms, 30% in 500-5000ms.
 */
public class HighThroughputBenchmarkProducer {

    private static double getPercentile(List<Long> values, double percentile) {
        if (values == null || values.isEmpty()) return 0.0;
        int idx = (int) Math.ceil(percentile * values.size()) - 1;
        idx = Math.max(0, Math.min(idx, values.size() - 1));
        return values.get(idx);
    }

    private static long generateRandomLatency() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        if (rng.nextDouble() < 0.70) {
            // 70% probability: 5ms to 500ms (inclusive)
            return rng.nextLong(5, 501);
        } else {
            // 30% probability: 500ms to 5000ms (inclusive)
            return rng.nextLong(500, 5001);
        }
    }

    public static void main(String[] args) {
        int eps = args.length > 0 ? Integer.parseInt(args[0]) : 5000;
        int durationSec = args.length > 1 ? Integer.parseInt(args[1]) : 30;
        int eventSizeBytes = args.length > 2 ? Integer.parseInt(args[2]) : 5128;
        boolean isRandomLatency = true;
        int fixedLatencyMs = 0;
        String bootstrapServers = "localhost:9092";

        if (args.length > 3) {
            String arg3 = args[3].trim();
            if (arg3.contains(":") || arg3.equalsIgnoreCase("kafka")) {
                bootstrapServers = arg3;
                isRandomLatency = true;
                if (args.length > 4) {
                    String arg4 = args[4].trim();
                    if (arg4.equalsIgnoreCase("random") || arg4.equals("-1")) {
                        isRandomLatency = true;
                    } else {
                        try {
                            fixedLatencyMs = Integer.parseInt(arg4);
                            isRandomLatency = false;
                        } catch (NumberFormatException e) {
                            isRandomLatency = true;
                        }
                    }
                }
            } else if (arg3.equalsIgnoreCase("random") || arg3.equals("-1")) {
                isRandomLatency = true;
                if (args.length > 4) {
                    bootstrapServers = args[4].trim();
                }
            } else {
                try {
                    fixedLatencyMs = Integer.parseInt(arg3);
                    isRandomLatency = false;
                    if (args.length > 4) {
                        bootstrapServers = args[4].trim();
                    }
                } catch (NumberFormatException e) {
                    bootstrapServers = arg3;
                    isRandomLatency = true;
                }
            }
        }

        int totalMatchingPairs = eps * durationSec;
        int totalEventsPerTopic = totalMatchingPairs;

        System.out.println("======================================================================");
        System.out.println("   ENTERPRISE NATIVE JAVA HIGH-THROUGHPUT BENCHMARK PRODUCER         ");
        System.out.println("======================================================================");
        System.out.printf("  Target EPS per Producer     : %d events/sec\n", eps);
        System.out.printf("  Duration                    : %d seconds\n", durationSec);
        System.out.printf("  Payload Size per Event      : %d bytes\n", eventSizeBytes);
        if (isRandomLatency) {
            System.out.println("  Latency Between Events (A/B): Random [70%: 5-500ms, 30%: 500-5000ms]");
        } else {
            System.out.printf("  Latency Between Events (A/B): %d ms\n", fixedLatencyMs);
        }
        System.out.printf("  Matching IMSI Pairs         : %d pairs\n", totalMatchingPairs);
        System.out.printf("  Total Input Events          : %d events\n", totalEventsPerTopic * 2);
        System.out.printf("  Kafka Bootstrap Servers     : %s\n", bootstrapServers);
        System.out.println("----------------------------------------------------------------------");

        // High-Performance Producer Properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "131072"); // 128KB batching
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864"); // 64MB buffer
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        // props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // Pre-allocate payload padding once
        String baseSampleA = "Timestamp: 10:15:34.999 UE: IMSI 404450000000000 Cell: LTE_5123 Event: Measurement Report RSRP: -95 dBm RSRQ: -12 dB Neighbor Cell: LTE_5124";
        int lenA = baseSampleA.getBytes().length;
        int padLenA = Math.max(0, eventSizeBytes - lenA - 10);
        String paddingA = padLenA > 0 ? "A".repeat(padLenA) : "";

        String baseSampleB = "Timestamp: 10:15:33.999 IMSI: 404450000000000 Procedure: Attach Request MME_ID: MME_01 TAI: Chennai_125 Result: Success";
        int lenB = baseSampleB.getBytes().length;
        int padLenB = Math.max(0, eventSizeBytes - lenB - 10);
        String paddingB = padLenB > 0 ? "B".repeat(padLenB) : "";

        ConcurrentHashMap<String, Long> ingestTimestampsA = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Long> ingestTimestampsB = new ConcurrentHashMap<>();
        AtomicLong producedA = new AtomicLong(0);
        AtomicLong producedB = new AtomicLong(0);

        long sessionPrefix = (System.currentTimeMillis() / 1000) % 100000;
        CountDownLatch latchA = new CountDownLatch(1);
        CountDownLatch latchB = new CountDownLatch(1);

        int scheduledPoolSize = Math.max(8, Runtime.getRuntime().availableProcessors() * 2);
        ScheduledExecutorService scheduledBExecutor = Executors.newScheduledThreadPool(scheduledPoolSize);
        ExecutorService producerAExecutor = Executors.newSingleThreadExecutor();

        // 2. Real-Time Background Verifier Consumer Setup (Runs concurrently during test)
        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "bench-verifier-" + System.currentTimeMillis());
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5000);

        List<Long> e2eLatencies = Collections.synchronizedList(new ArrayList<>());
        List<Long> corrLatencies = Collections.synchronizedList(new ArrayList<>());
        Set<String> uniqueKeys = ConcurrentHashMap.newKeySet();
        AtomicLong receivedMerged = new AtomicLong(0);
        AtomicLong duplicateJoins = new AtomicLong(0);
        AtomicLong incorrectJoins = new AtomicLong(0);
        AtomicLong totalE2eLatencySum = new AtomicLong(0);
        AtomicLong totalCorrLatencySum = new AtomicLong(0);
        AtomicBoolean producingDone = new AtomicBoolean(false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps);
        try {
            List<PartitionInfo> partInfos = consumer.partitionsFor("merged_events");
            if (partInfos != null && !partInfos.isEmpty()) {
                List<TopicPartition> tps = new ArrayList<>();
                for (PartitionInfo pi : partInfos) {
                    tps.add(new TopicPartition("merged_events", pi.partition()));
                }
                consumer.assign(tps);
                consumer.seekToEnd(tps);
                System.out.printf("[*] Real-Time Verifier Consumer assigned to %d partitions and seeked to latest offset.\n", tps.size());
            } else {
                consumer.subscribe(Collections.singletonList("merged_events"));
            }
        } catch (Exception e) {
            consumer.subscribe(Collections.singletonList("merged_events"));
        }

        Thread consumerThread = new Thread(() -> {
            // Track idle time from when production ENDS, not from thread start.
            // lastMsgTime is reset to now when producingDone first becomes true,
            // ensuring the idle window doesn't expire prematurely during long production runs.
            long lastMsgTime = System.currentTimeMillis();
            long productionEndTime = -1L;
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    lastMsgTime = System.currentTimeMillis();
                    for (ConsumerRecord<String, String> record : records) {
                        String key = record.key();
                        if ((key == null || key.isEmpty()) && record.value() != null) {
                            String val = record.value();
                            int idx = val.indexOf("\"common_id\":\"");
                            if (idx != -1) {
                                int start = idx + 13;
                                int end = val.indexOf("\"", start);
                                if (end > start) {
                                    key = val.substring(start, end);
                                }
                            }
                        }

                        if (key != null && (ingestTimestampsA.containsKey(key) || ingestTimestampsB.containsKey(key))) {
                            receivedMerged.incrementAndGet();
                            if (!uniqueKeys.add(key)) {
                                duplicateJoins.incrementAndGet();
                            }

                            String val = record.value();
                            if (val == null || (!val.contains("payload_id_a") && !val.contains("raw_event_a") && !val.contains("payload_a"))) {
                                incorrectJoins.incrementAndGet();
                            }

                            long now = System.currentTimeMillis();
                            long tA = ingestTimestampsA.getOrDefault(key, now);
                            long tB = ingestTimestampsB.getOrDefault(key, now);
                            long e2e = now - Math.min(tA, tB);
                            long corr = now - Math.max(tA, tB);

                            if (e2e >= 0) {
                                e2eLatencies.add(e2e);
                                totalE2eLatencySum.addAndGet(e2e);
                            }
                            if (corr >= 0) {
                                corrLatencies.add(corr);
                                totalCorrLatencySum.addAndGet(corr);
                            }
                        }
                    }
                }
                if (receivedMerged.get() >= totalMatchingPairs) {
                    break;
                }
                // Capture the moment production finishes; reset lastMsgTime so the idle
                // window is measured from production-end, not from consumer thread start.
                if (producingDone.get() && productionEndTime < 0) {
                    productionEndTime = System.currentTimeMillis();
                    lastMsgTime = productionEndTime; // reset idle clock
                }

                long maxPostProductionMs = (long) durationSec * 1000L; // Exactly 60s post-production draining
                long idleTimeoutMs = 15_000L; // 15s idle silence timeout

                if (productionEndTime > 0) {
                    long elapsedPostProd = System.currentTimeMillis() - productionEndTime;
                    if (elapsedPostProd >= maxPostProductionMs) {
                        break;
                    }
                    if (System.currentTimeMillis() - lastMsgTime > idleTimeoutMs) {
                        break;
                    }
                }
            }
            try {
                consumer.close();
            } catch (Exception ignored) {}
        }, "benchmark-realtime-consumer");
        consumerThread.start();

        long startBenchTime = System.currentTimeMillis();

        KafkaProducer<String, String> producerA = new KafkaProducer<>(props);
        KafkaProducer<String, String> producerB = new KafkaProducer<>(props);

        final boolean useRandomLatency = isRandomLatency;
        final int targetFixedLatency = fixedLatencyMs;

        System.out.println("[*] High-Speed Smooth Java Producers Running for Topic A & Topic B...");

        // Producer A Worker Thread: produces Topic A and schedules Topic B with random latency
        producerAExecutor.submit(() -> {
            long start = System.currentTimeMillis();
            int paceStep = Math.max(1, Math.min(25, eps / 40));

            for (int i = 1; i <= totalEventsPerTopic; i++) {
                String imsi = String.format("404%05d%08d", sessionPrefix, i);
                long now = System.currentTimeMillis();
                ingestTimestampsA.putIfAbsent(imsi, now);

                String payloadA = paddingA.isEmpty()
                        ? String.format("Timestamp: 10:15:34.%03d UE: IMSI %s Cell: LTE_5123 Event: Measurement Report RSRP: -95 dBm RSRQ: -12 dB Neighbor Cell: LTE_5124",
                                i % 1000, imsi)
                        : String.format("Timestamp: 10:15:34.%03d UE: IMSI %s Cell: LTE_5123 Event: Measurement Report RSRP: -95 dBm RSRQ: -12 dB Neighbor Cell: LTE_5124 Padding: %s",
                                i % 1000, imsi, paddingA);

                producerA.send(new ProducerRecord<>("topic_a", imsi, payloadA));
                producedA.incrementAndGet();

                // Prepare Topic B payload
                String payloadB = paddingB.isEmpty()
                        ? String.format("Timestamp: 10:15:33.%03d IMSI: %s Procedure: Attach Request MME_ID: MME_01 TAI: Chennai_125 Result: Success",
                                i % 1000, imsi)
                        : String.format("Timestamp: 10:15:33.%03d IMSI: %s Procedure: Attach Request MME_ID: MME_01 TAI: Chennai_125 Result: Success Padding: %s",
                                i % 1000, imsi, paddingB);

                long delayMs = useRandomLatency ? generateRandomLatency() : targetFixedLatency;

                if (delayMs <= 0) {
                    ingestTimestampsB.putIfAbsent(imsi, System.currentTimeMillis());
                    producerB.send(new ProducerRecord<>("topic_b", imsi, payloadB));
                    if (producedB.incrementAndGet() == totalEventsPerTopic) {
                        latchB.countDown();
                    }
                } else {
                    scheduledBExecutor.schedule(() -> {
                        ingestTimestampsB.putIfAbsent(imsi, System.currentTimeMillis());
                        producerB.send(new ProducerRecord<>("topic_b", imsi, payloadB));
                        if (producedB.incrementAndGet() == totalEventsPerTopic) {
                            latchB.countDown();
                        }
                    }, delayMs, TimeUnit.MILLISECONDS);
                }

                // Smooth token pacing without microburst spikes
                if (i % paceStep == 0) {
                    long expectedElapsed = (long) ((double) i * 1000.0 / eps);
                    long actualElapsed = System.currentTimeMillis() - start;
                    if (expectedElapsed > actualElapsed) {
                        try {
                            Thread.sleep(expectedElapsed - actualElapsed);
                        } catch (InterruptedException ignored) {}
                    }
                }
            }
            producerA.flush();
            producerA.close();
            latchA.countDown();
        });

        try {
            latchA.await();
            // Wait for all delayed Topic B events to be sent (maximum delay is 5000ms + buffer)
            latchB.await((long) durationSec + 15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producerB.flush();
        producerB.close();
        scheduledBExecutor.shutdown();
        producerAExecutor.shutdown();

        long endProduceTime = System.currentTimeMillis();
        double produceSec = (endProduceTime - startBenchTime) / 1000.0;
        double epsA = producedA.get() / produceSec;
        double epsB = producedB.get() / produceSec;

        System.out.printf("[OK] Source A Produced: %d events in %.2fs (%.2f EPS)\n", producedA.get(), produceSec, epsA);
        System.out.printf("[OK] Source B Produced: %d events in %.2fs (%.2f EPS)\n", producedB.get(), produceSec, epsB);
        System.out.println("[*] Awaiting completion of real-time join verification on 'merged_events'...");

        // Signal verifier consumer that production has finished
        producingDone.set(true);
        long drainStart = System.currentTimeMillis();
        long maxWaitMs = (long) durationSec * 1000L; // Exactly 60s post-production draining
        long lastLogTime = drainStart;

        while (consumerThread.isAlive() && (System.currentTimeMillis() - drainStart < maxWaitMs)) {
            try {
                consumerThread.join(1000);
            } catch (InterruptedException e) {
                consumerThread.interrupt();
                break;
            }
            long now = System.currentTimeMillis();
            if (now - lastLogTime >= 5000) {
                long received = receivedMerged.get();
                double pct = totalMatchingPairs > 0 ? (double) received * 100.0 / totalMatchingPairs : 0.0;
                long remainingSec = Math.max(0, (maxWaitMs - (now - drainStart)) / 1000);
                System.out.printf("[*] Real-Time Progress: %,d / %,d verified (%.1f%%) | Draining time remaining: %ds\n",
                        received, totalMatchingPairs, pct, remainingSec);
                lastLogTime = now;
            }
            if (receivedMerged.get() >= totalMatchingPairs) {
                break;
            }
        }
        if (consumerThread.isAlive()) {
            consumerThread.interrupt();
        }

        long finalMergedCount = receivedMerged.get();
        double totalBenchSec = (System.currentTimeMillis() - startBenchTime) / 1000.0;
        double joinsPerSec = finalMergedCount / totalBenchSec;

        List<Long> sortedE2e = new ArrayList<>(e2eLatencies);
        List<Long> sortedCorr = new ArrayList<>(corrLatencies);
        Collections.sort(sortedE2e);
        Collections.sort(sortedCorr);

        double p50E2e = getPercentile(sortedE2e, 0.50);
        double p95E2e = getPercentile(sortedE2e, 0.95);
        double p99E2e = getPercentile(sortedE2e, 0.99);
        double p999E2e = getPercentile(sortedE2e, 0.999);
        double maxE2e = sortedE2e.isEmpty() ? 0 : sortedE2e.get(sortedE2e.size() - 1);
        double avgE2e = finalMergedCount > 0 ? (double) totalE2eLatencySum.get() / finalMergedCount : 0.0;

        double p50Corr = getPercentile(sortedCorr, 0.50);
        double p95Corr = getPercentile(sortedCorr, 0.95);
        double p99Corr = getPercentile(sortedCorr, 0.99);
        double p999Corr = getPercentile(sortedCorr, 0.999);
        double maxCorr = sortedCorr.isEmpty() ? 0 : sortedCorr.get(sortedCorr.size() - 1);
        double avgCorr = finalMergedCount > 0 ? (double) totalCorrLatencySum.get() / finalMergedCount : 0.0;

        long missingPairs = Math.max(0, totalMatchingPairs - uniqueKeys.size());
        double joinSuccessPct = totalMatchingPairs > 0 ? (uniqueKeys.size() * 100.0 / totalMatchingPairs) : 0.0;
        double dataLossPct = totalMatchingPairs > 0 ? (missingPairs * 100.0 / totalMatchingPairs) : 0.0;
        double duplicatePct = totalMatchingPairs > 0 ? (duplicateJoins.get() * 100.0 / totalMatchingPairs) : 0.0;
        double incorrectPct = finalMergedCount > 0 ? (incorrectJoins.get() * 100.0 / finalMergedCount) : 0.0;

        System.out.println("\n======================================================================");
        System.out.println("          BENCHMARK VERIFICATION & METRICS SUMMARY REPORT             ");
        System.out.println("======================================================================");
        System.out.printf("  Target Matching Event Pairs      : %d\n", totalMatchingPairs);
        if (isRandomLatency) {
            System.out.println("  Latency Between Events (A/B)     : Random [70%: 5-500ms, 30%: 500-5000ms]");
        } else {
            System.out.printf("  Latency Between Events (A/B)     : %d ms\n", fixedLatencyMs);
        }
        System.out.printf("  Source A Events Produced         : %d (%.2f EPS)\n", producedA.get(), epsA);
        System.out.printf("  Source B Events Produced         : %d (%.2f EPS)\n", producedB.get(), epsB);
        System.out.printf("  Joined & Merged Events Received  : %d (%.2f%% Join Success)\n", finalMergedCount, joinSuccessPct);
        System.out.printf("  Effective Joins Per Second       : %.2f joins/sec\n", joinsPerSec);
        System.out.printf("  Missing Pairs / Data Loss        : %d (%.2f%%)\n", missingPairs, dataLossPct);
        System.out.printf("  Duplicate Merged Pairs           : %d (%.2f%%)\n", duplicateJoins.get(), duplicatePct);
        System.out.printf("  Incorrect Merged Pairs           : %d (%.2f%%)\n", incorrectJoins.get(), incorrectPct);
        System.out.println("  --------------------------------------------------------------------");
        System.out.printf("  End-to-End Latency (ms)          : Avg=%.2f | P50=%.1f | P95=%.1f | P99=%.1f | P99.9=%.1f | MAX=%.1f\n",
                avgE2e, p50E2e, p95E2e, p99E2e, p999E2e, maxE2e);
        System.out.printf("  Correlation Latency (ms)         : Avg=%.2f | P50=%.1f | P95=%.1f | P99=%.1f | P99.9=%.1f | MAX=%.1f\n",
                avgCorr, p50Corr, p95Corr, p99Corr, p999Corr, maxCorr);
        System.out.println("======================================================================");
        System.out.printf("[EMPIRICAL_BENCHMARK_RESULT] target_eps=%d actual_eps_a=%.2f actual_eps_b=%.2f actual_combined_eps=%.2f merged_events=%d effective_jps=%.2f join_success_pct=%.2f missing_pairs=%d data_loss_pct=%.2f duplicate_pairs=%d duplicate_pct=%.2f incorrect_pairs=%d incorrect_pct=%.2f avg_e2e_ms=%.2f p50_ms=%.2f p95_ms=%.2f p99_ms=%.2f p99_9_ms=%.2f max_e2e_ms=%.2f avg_corr_ms=%.2f corr_p50=%.2f corr_p95=%.2f corr_p99=%.2f corr_p99_9=%.2f corr_max=%.2f\n",
                eps, epsA, epsB, (epsA + epsB), finalMergedCount, joinsPerSec, joinSuccessPct, missingPairs, dataLossPct, duplicateJoins.get(), duplicatePct, incorrectJoins.get(), incorrectPct, avgE2e, p50E2e, p95E2e, p99E2e, p999E2e, maxE2e, avgCorr, p50Corr, p95Corr, p99Corr, p999Corr, maxCorr);
        System.out.println("======================================================================");
    }
}
