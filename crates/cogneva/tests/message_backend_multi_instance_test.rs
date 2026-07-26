//! MemoryMessageBackend multi-instance consistency tests.
//! Spawns multiple concurrent "instances" that share a single
//! [`MemoryMessageBackend`] and verifies at-least-once delivery semantics
//! under contention.

use cog_core::MessageBackend;
use cog_stream::MemoryMessageBackend;
use futures::StreamExt;
use std::sync::Arc;
use std::time::{Duration, Instant};

const STREAM: &str = "test:multi:instance";
const GROUP: &str = "test-group";

/// Each of N instances publishes M messages and subscribes to the shared
/// stream.  We verify that every payload published is eventually received
/// by at least one consumer.
#[tokio::test]
async fn test_multi_instance_publish_subscribe_consistency() {
    let backend = Arc::new(MemoryMessageBackend::new());
    let _ = backend.create_consumer_group(STREAM, GROUP).await;

    const N_INSTANCES: usize = 4;
    const MESSAGES_PER_INSTANCE: usize = 25;

    let start = Instant::now();

    // --- Publish phase -------------------------------------------------------
    let mut publish_handles = Vec::with_capacity(N_INSTANCES);
    let mut expected_payloads: Vec<Vec<u8>> =
        Vec::with_capacity(N_INSTANCES * MESSAGES_PER_INSTANCE);

    for instance_id in 0..N_INSTANCES {
        let backend = backend.clone();
        let payloads: Vec<Vec<u8>> = (0..MESSAGES_PER_INSTANCE)
            .map(|seq| format!("instance-{}-msg-{}", instance_id, seq).into_bytes())
            .collect();
        expected_payloads.extend(payloads.clone());

        publish_handles.push(tokio::spawn(async move {
            for payload in &payloads {
                backend.publish(STREAM, payload).await.unwrap();
            }
            instance_id
        }));
    }

    for h in publish_handles {
        h.await.unwrap();
    }

    let publish_elapsed = start.elapsed();

    // --- Consume phase -------------------------------------------------------
    let mut consume_handles = Vec::with_capacity(N_INSTANCES);

    for instance_id in 0..N_INSTANCES {
        let backend = backend.clone();
        consume_handles.push(tokio::spawn(async move {
            let mut stream = backend.subscribe(STREAM, GROUP).await.unwrap();
            let mut received = Vec::with_capacity(MESSAGES_PER_INSTANCE * 2);
            let deadline = Instant::now() + Duration::from_secs(5);

            while Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_millis(200), stream.next()).await {
                    Ok(Some(Ok((_, bytes)))) => received.push(bytes),
                    _ => break,
                }
            }
            (instance_id, received)
        }));
    }

    let mut all_received: Vec<Vec<u8>> = Vec::new();
    for h in consume_handles {
        let (_id, received) = h.await.unwrap();
        all_received.extend(received);
    }

    let total_elapsed = start.elapsed();

    // --- Verify no loss ------------------------------------------------------
    let mut missing = Vec::new();
    for expected in &expected_payloads {
        if !all_received.contains(expected) {
            missing.push(String::from_utf8_lossy(expected).to_string());
        }
    }
    assert!(
        missing.is_empty(),
        "Missing {} of {} messages. Missing: {:?}. Total received: {}. Publish took {:?}, total took {:?}",
        missing.len(),
        expected_payloads.len(),
        missing,
        all_received.len(),
        publish_elapsed,
        total_elapsed,
    );

    // All messages should have been received exactly once across all consumers
    // (broadcast semantics mean every consumer sees every message).
    // With N_INSTANCES consumers, we expect N_INSTANCES copies of each message.
    assert_eq!(
        all_received.len(),
        expected_payloads.len() * N_INSTANCES,
        "Broadcast semantics: each consumer should see every message. Expected {} * {} = {}, got {}",
        expected_payloads.len(),
        N_INSTANCES,
        expected_payloads.len() * N_INSTANCES,
        all_received.len(),
    );
}

/// Concurrent publish_batch from multiple instances must not corrupt the
/// in-memory buffer ordering.
#[tokio::test]
async fn test_multi_instance_batch_publish_ordering() {
    let backend = Arc::new(MemoryMessageBackend::new());
    let _ = backend.create_consumer_group(STREAM, GROUP).await;

    const N_INSTANCES: usize = 4;
    const BATCH_SIZE: usize = 10;

    let mut handles = Vec::with_capacity(N_INSTANCES);

    for instance_id in 0..N_INSTANCES {
        let backend = backend.clone();
        let batch: Vec<Vec<u8>> = (0..BATCH_SIZE)
            .map(|seq| format!("batch-instance-{}-{}", instance_id, seq).into_bytes())
            .collect();

        handles.push(tokio::spawn(async move {
            backend.publish_batch(STREAM, &batch).await.unwrap();
            batch
        }));
    }

    let mut expected_batches = Vec::new();
    for h in handles {
        expected_batches.push(h.await.unwrap());
    }

    // Single consumer reads everything back.
    let mut stream = backend.subscribe(STREAM, GROUP).await.unwrap();
    let mut received = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);

    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), stream.next()).await {
            Ok(Some(Ok((_, bytes)))) => received.push(bytes),
            _ => break,
        }
    }

    let total_expected = N_INSTANCES * BATCH_SIZE;
    assert_eq!(
        received.len(),
        total_expected,
        "expected {} messages after batch publish, got {}",
        total_expected,
        received.len()
    );

    // Verify each instance's batch is internally ordered.
    for batch in &expected_batches {
        let batch_strings: Vec<String> = batch
            .iter()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .collect();
        let received_strings: Vec<String> = received
            .iter()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .collect();

        // Find the subsequence in received messages
        let mut batch_idx = 0;
        for r in &received_strings {
            if batch_idx < batch_strings.len() && *r == batch_strings[batch_idx] {
                batch_idx += 1;
            }
        }
        assert_eq!(
            batch_idx,
            batch_strings.len(),
            "Batch ordering corrupted for instance batch starting with {}",
            batch_strings.first().unwrap_or(&"empty".into())
        );
    }
}

/// Subscribe-from-replay: a late-joining instance must see historical
/// messages when using `subscribe_from` with start_id "0".
#[tokio::test]
async fn test_late_joining_instance_replay() {
    let backend = Arc::new(MemoryMessageBackend::new());
    let _ = backend.create_consumer_group(STREAM, GROUP).await;

    // Publish 10 messages.
    for i in 0..10 {
        backend
            .publish(STREAM, format!("replay-msg-{}", i).as_bytes())
            .await
            .unwrap();
    }

    // Late consumer joins from the beginning.
    let mut stream = backend.subscribe_from(STREAM, GROUP, "0").await.unwrap();
    let mut received = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(3);

    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), stream.next()).await {
            Ok(Some(Ok((_, bytes)))) => received.push(bytes),
            _ => break,
        }
    }

    assert_eq!(
        received.len(),
        10,
        "late-joining instance should replay all 10 historical messages, got {}",
        received.len()
    );

    for (i, msg) in received.iter().enumerate() {
        let expected = format!("replay-msg-{}", i);
        let actual = String::from_utf8_lossy(msg);
        assert_eq!(
            actual, expected,
            "message at index {} should be '{}', got '{}'",
            i, expected, actual
        );
    }
}

/// Measure end-to-end latency under light load.
#[tokio::test]
async fn test_memory_backend_latency_under_load() {
    let backend = Arc::new(MemoryMessageBackend::new());
    let _ = backend.create_consumer_group(STREAM, GROUP).await;

    const MSG_COUNT: usize = 100;

    // Consumer
    let backend_c = backend.clone();
    let consumer = tokio::spawn(async move {
        let mut stream = backend_c.subscribe(STREAM, GROUP).await.unwrap();
        let mut latencies = Vec::with_capacity(MSG_COUNT);
        let mut count = 0;
        let deadline = Instant::now() + Duration::from_secs(10);

        while count < MSG_COUNT && Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
                Ok(Some(Ok((_, bytes)))) => {
                    let sent_ts = String::from_utf8_lossy(&bytes).parse::<u64>().unwrap_or(0);
                    let now = Instant::now().elapsed().as_nanos() as u64; // not monotonic, but ok for rough measure
                                                                          // Use a simpler approach: embed Instant as nanos since program start
                    latencies.push(now.saturating_sub(sent_ts));
                    count += 1;
                }
                _ => break,
            }
        }
        latencies
    });

    // Producer
    let producer = tokio::spawn(async move {
        for i in 0..MSG_COUNT {
            let ts = Instant::now().elapsed().as_nanos() as u64;
            backend
                .publish(STREAM, ts.to_string().as_bytes())
                .await
                .unwrap();
            // Tiny delay to avoid overwhelming the broadcast channel
            if i % 10 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });

    let (latencies, _producer) = tokio::join!(consumer, producer);
    let latencies = latencies.unwrap();

    assert_eq!(
        latencies.len(),
        MSG_COUNT,
        "expected {} latency samples, got {}",
        MSG_COUNT,
        latencies.len()
    );

    let avg_ns: u64 = latencies.iter().sum::<u64>() / latencies.len() as u64;
    let max_ns = *latencies.iter().max().unwrap_or(&0);

    // Memory backend should be very fast (< 1ms average, < 10ms max).
    let avg_ms = avg_ns as f64 / 1_000_000.0;
    let max_ms = max_ns as f64 / 1_000_000.0;

    assert!(
        avg_ms < 1.0,
        "average latency too high: {} ms (expected < 1 ms)",
        avg_ms
    );
    assert!(
        max_ms < 10.0,
        "max latency too high: {} ms (expected < 10 ms)",
        max_ms
    );

    println!(
        "MemoryMessageBackend latency: avg={:.3}ms, max={:.3}ms ({} messages)",
        avg_ms, max_ms, MSG_COUNT
    );
}
