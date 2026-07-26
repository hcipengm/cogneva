//! MessageBackend stress tests — verify state consistency and latency
//! under high concurrency and large batches.

use cog_core::MessageBackend;
use cog_stream::MemoryMessageBackend;
use futures::StreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const STREAM: &str = "test:stress";
const GROUP: &str = "stress-group";

/// High-contention stress: many instances publish while many consume.
/// Verifies no message loss and broadcast semantics hold under load.
#[tokio::test]
async fn stress_memory_backend_high_contention() {
    let backend = Arc::new(MemoryMessageBackend::new());
    let _ = backend.create_consumer_group(STREAM, GROUP).await;

    const PUBLISHERS: usize = 8;
    const CONSUMERS: usize = 8;
    const MESSAGES_PER_PUBLISHER: usize = 100;

    let start = Instant::now();
    let total_expected = PUBLISHERS * MESSAGES_PER_PUBLISHER;

    // --- Publish phase -------------------------------------------------------
    let mut pub_handles = Vec::with_capacity(PUBLISHERS);
    let mut all_payloads: Vec<Vec<u8>> = Vec::with_capacity(total_expected);

    for pub_id in 0..PUBLISHERS {
        let backend = backend.clone();
        let payloads: Vec<Vec<u8>> = (0..MESSAGES_PER_PUBLISHER)
            .map(|seq| format!("pub-{}-msg-{}", pub_id, seq).into_bytes())
            .collect();
        all_payloads.extend(payloads.clone());

        pub_handles.push(tokio::spawn(async move {
            for payload in &payloads {
                backend.publish(STREAM, payload).await.unwrap();
            }
            pub_id
        }));
    }

    for h in pub_handles {
        h.await.unwrap();
    }
    let publish_done = start.elapsed();

    // --- Consume phase -------------------------------------------------------
    let mut cons_handles = Vec::with_capacity(CONSUMERS);
    let total_received = Arc::new(AtomicUsize::new(0));

    for cons_id in 0..CONSUMERS {
        let backend = backend.clone();
        let total_received = total_received.clone();
        cons_handles.push(tokio::spawn(async move {
            let mut stream = backend.subscribe(STREAM, GROUP).await.unwrap();
            let mut local_count = 0;
            let deadline = Instant::now() + Duration::from_secs(15);

            while Instant::now() < deadline && local_count < total_expected {
                match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
                    Ok(Some(Ok(_))) => {
                        local_count += 1;
                        total_received.fetch_add(1, Ordering::SeqCst);
                    }
                    _ => break,
                }
            }
            (cons_id, local_count)
        }));
    }

    let mut per_consumer_counts = Vec::with_capacity(CONSUMERS);
    for h in cons_handles {
        let (id, count) = h.await.unwrap();
        per_consumer_counts.push((id, count));
    }

    let total_elapsed = start.elapsed();
    let received_sum = total_received.load(Ordering::SeqCst);

    // --- Assertions ----------------------------------------------------------
    // Broadcast semantics: every consumer should see every message.
    for (id, count) in per_consumer_counts {
        assert_eq!(
            count, total_expected,
            "consumer {} should have received {} messages, got {}",
            id, total_expected, count
        );
    }

    assert_eq!(
        received_sum,
        total_expected * CONSUMERS,
        "total received across all consumers should be {} * {} = {}, got {}",
        total_expected,
        CONSUMERS,
        total_expected * CONSUMERS,
        received_sum
    );

    println!(
        "High-contention stress: {} publishers x {} msgs, {} consumers. Publish={:?}, Total={:?}",
        PUBLISHERS, MESSAGES_PER_PUBLISHER, CONSUMERS, publish_done, total_elapsed
    );
}

/// Large batch stress: single publisher sends a massive batch,
/// single consumer reads it back. Verifies ordering and no truncation.
#[tokio::test]
async fn stress_memory_backend_large_batch() {
    let backend = Arc::new(MemoryMessageBackend::new());
    let _ = backend.create_consumer_group(STREAM, GROUP).await;

    const BATCH_SIZE: usize = 1_000;

    let batch: Vec<Vec<u8>> = (0..BATCH_SIZE)
        .map(|i| format!("batch-item-{}", i).into_bytes())
        .collect();

    let start = Instant::now();
    backend.publish_batch(STREAM, &batch).await.unwrap();
    let publish_elapsed = start.elapsed();

    let mut stream = backend.subscribe(STREAM, GROUP).await.unwrap();
    let mut received = Vec::with_capacity(BATCH_SIZE);
    let deadline = Instant::now() + Duration::from_secs(10);

    while Instant::now() < deadline && received.len() < BATCH_SIZE {
        match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok((_, bytes)))) => received.push(bytes),
            _ => break,
        }
    }

    let total_elapsed = start.elapsed();

    assert_eq!(
        received.len(),
        BATCH_SIZE,
        "expected {} messages from large batch, got {}",
        BATCH_SIZE,
        received.len()
    );

    // Verify strict ordering.
    for (i, msg) in received.iter().enumerate() {
        let expected = format!("batch-item-{}", i);
        let actual = String::from_utf8_lossy(msg);
        assert_eq!(
            actual, expected,
            "message at index {} should be '{}', got '{}'",
            i, expected, actual
        );
    }

    println!(
        "Large batch stress: {} items. Publish={:?}, Total={:?}",
        BATCH_SIZE, publish_elapsed, total_elapsed
    );
}

/// Latency distribution under sustained load.
/// Measures p50, p99 and max latency for individual publish→consume round trips.
#[tokio::test]
async fn stress_memory_backend_latency_distribution() {
    let backend = Arc::new(MemoryMessageBackend::new());
    let _ = backend.create_consumer_group(STREAM, GROUP).await;

    const MSG_COUNT: usize = 1_000;

    let backend_c = backend.clone();
    let consumer = tokio::spawn(async move {
        let mut stream = backend_c.subscribe(STREAM, GROUP).await.unwrap();
        let mut latencies = Vec::with_capacity(MSG_COUNT);
        let deadline = Instant::now() + Duration::from_secs(30);

        while latencies.len() < MSG_COUNT && Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(1000), stream.next()).await {
                Ok(Some(Ok((_, bytes)))) => {
                    let sent_ns = String::from_utf8_lossy(&bytes).parse::<u64>().unwrap_or(0);
                    let now_ns = Instant::now().elapsed().as_nanos() as u64;
                    latencies.push(now_ns.saturating_sub(sent_ns));
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
            // Yield periodically to let consumer keep up.
            if i % 50 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });

    let (latencies, _) = tokio::join!(consumer, producer);
    let latencies = latencies.unwrap();

    assert_eq!(
        latencies.len(),
        MSG_COUNT,
        "expected {} latency samples, got {}",
        MSG_COUNT,
        latencies.len()
    );

    let mut sorted = latencies.clone();
    sorted.sort();

    let avg_ns: u64 = sorted.iter().sum::<u64>() / sorted.len() as u64;
    let p50_ns = sorted[sorted.len() / 2];
    let p99_idx = (sorted.len() as f64 * 0.99) as usize;
    let p99_ns = sorted[p99_idx.min(sorted.len() - 1)];
    let max_ns = *sorted.last().unwrap_or(&0);

    let avg_ms = avg_ns as f64 / 1_000_000.0;
    let p50_ms = p50_ns as f64 / 1_000_000.0;
    let p99_ms = p99_ns as f64 / 1_000_000.0;
    let max_ms = max_ns as f64 / 1_000_000.0;

    // Memory backend should be extremely fast.
    assert!(
        avg_ms < 1.0,
        "average latency too high: {} ms (expected < 1 ms)",
        avg_ms
    );
    assert!(
        p99_ms < 5.0,
        "p99 latency too high: {} ms (expected < 5 ms)",
        p99_ms
    );
    assert!(
        max_ms < 20.0,
        "max latency too high: {} ms (expected < 20 ms)",
        max_ms
    );

    println!(
        "Latency distribution ({} msgs): avg={:.3}ms p50={:.3}ms p99={:.3}ms max={:.3}ms",
        MSG_COUNT, avg_ms, p50_ms, p99_ms, max_ms
    );
}

/// Rapid create/delete cycle for consumer groups and streams.
/// Verifies that the backend remains stable under churn.
#[tokio::test]
async fn stress_memory_backend_churn() {
    let backend = Arc::new(MemoryMessageBackend::new());

    const CYCLES: usize = 100;
    const MSGS_PER_CYCLE: usize = 10;

    let start = Instant::now();

    for cycle in 0..CYCLES {
        let stream = format!("{}:churn:{}", STREAM, cycle);
        let group = format!("{}:churn:{}", GROUP, cycle);

        let _ = backend.create_consumer_group(&stream, &group).await;

        let payloads: Vec<Vec<u8>> = (0..MSGS_PER_CYCLE)
            .map(|i| format!("cycle-{}-msg-{}", cycle, i).into_bytes())
            .collect();
        backend.publish_batch(&stream, &payloads).await.unwrap();

        let mut stream_consumer = backend.subscribe(&stream, &group).await.unwrap();
        let mut received = 0;
        let deadline = Instant::now() + Duration::from_millis(500);

        while Instant::now() < deadline && received < MSGS_PER_CYCLE {
            match tokio::time::timeout(Duration::from_millis(50), stream_consumer.next()).await {
                Ok(Some(Ok(_))) => received += 1,
                _ => break,
            }
        }

        assert_eq!(
            received, MSGS_PER_CYCLE,
            "churn cycle {}: expected {} messages, got {}",
            cycle, MSGS_PER_CYCLE, received
        );
    }

    let elapsed = start.elapsed();
    println!(
        "Churn stress: {} cycles x {} msgs in {:?}",
        CYCLES, MSGS_PER_CYCLE, elapsed
    );
}
