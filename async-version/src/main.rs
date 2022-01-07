use std::{collections::VecDeque, mem, sync::Arc, thread, time::Duration};

use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender, TryRecvError};
use glommio::{LocalExecutorBuilder, LocalExecutorPoolBuilder, Placement};
use raft::eraftpb::Entry;
use raft_engine::{Config, Engine, LogBatch, MessageExt, ReadableSize};

#[derive(Clone)]
pub struct MessageExtTyped;

impl MessageExt for MessageExtTyped {
    type Entry = Entry;

    fn index(e: &Self::Entry) -> u64 {
        e.index
    }
}

struct Region {
    id: u64,
    last_index: u64,
    entries: Vec<Entry>,
}

fn main() {
    let config = Config {
        dir: "/data2/async-version".to_owned(),
        purge_threshold: ReadableSize::gb(5),
        batch_compression_threshold: ReadableSize::kb(0),
        ..Default::default()
    };
    let engine = Arc::new(Engine::open(config).expect("Open raft engine"));
    let (producer_tx, producer_rx) = crossbeam_channel::unbounded();
    // let (consumer_tx, consumer_rx) = crossbeam_channel::unbounded();
    let (consumer_tx, consumer_rx) = async_channel::unbounded();
    for region_id in 1..=4096 {
        producer_tx
            .send(Region {
                id: region_id,
                last_index: 0,
                entries: Vec::new(),
            })
            .unwrap();
    }
    let engine_ = engine.clone();
    thread::spawn(move || loop {
        engine_.purge_expired_files().ok();
        thread::sleep(Duration::from_secs(5));
    });
    let handles: Vec<_> = (0..4)
        .map(move |i| {
            let engine = engine.clone();
            let producer_tx = producer_tx.clone();
            let consumer_rx = consumer_rx.clone();
            LocalExecutorBuilder::new(Placement::Fixed(i + 1))
                // .record_io_latencies(true)
                .spawn(move || run_worker(engine, consumer_rx, producer_tx))
                .unwrap()
        })
        .collect();

    loop {
        let mut region = producer_rx.recv().unwrap();
        for _ in 0..1 {
            let mut entry = Entry::default();
            entry.index = region.last_index + 1;
            region.last_index += 1;
            entry.set_data(Bytes::from_static(&[b'x'; 777]));
            region.entries.push(entry);
        }
        consumer_tx.try_send(region).unwrap();
    }
    // drop(consumer_tx);
    // handles
    //     .into_iter()
    //     .for_each(|handle| handle.join().unwrap());
}

async fn run_worker(
    engine: Arc<Engine>,
    consumer_rx: async_channel::Receiver<Region>,
    producer_tx: Sender<Region>,
) {
    // glommio::spawn_local(async {
    //     loop {
    //         let ring = &glommio::executor().io_stats().all_rings();
    //         let latency = ring.io_latency_us();
    //         println!(
    //             "io latency {:?}us",
    //             latency.sum().map(|sum| sum / latency.count() as f64)
    //         );
    //         // println!("{:?}", glommio::executor().io_stats());
    //         glommio::timer::sleep(Duration::from_secs(2)).await;
    //     }
    // })
    // .detach();
    loop {
        let mut batch = LogBatch::with_capacity(256);
        let mut regions = Vec::new();
        let mut compacts = Vec::new();
        match consumer_rx.recv().await {
            Ok(mut region) => {
                if region.entries[0].index % 10 == 9 {
                    compacts.push((region.id, region.entries[0].index - 5));
                }
                batch
                    .add_entries::<MessageExtTyped>(region.id, &region.entries)
                    .unwrap();
                region.entries.clear();
                regions.push(region);
            }
            Err(_) => break,
        }
        while let Ok(mut region) = consumer_rx.try_recv() {
            if region.entries[0].index % 10 == 9 {
                compacts.push((region.id, region.entries[0].index - 5));
            }
            batch
                .add_entries::<MessageExtTyped>(region.id, &region.entries)
                .unwrap();
            region.entries.clear();
            regions.push(region);
            if regions.len() >= 8 {
                break;
            }
        }
        if !regions.is_empty() {
            for (region_id, index) in compacts {
                let engine = engine.clone();
                glommio::spawn_local(async move {
                    engine.compact_to_async(region_id, index).await;
                })
                .detach();
            }
            let engine = engine.clone();
            let producer_tx = producer_tx.clone();
            glommio::spawn_local(async move {
                engine.write_async(&mut batch).await.unwrap();
                for region in regions.drain(..) {
                    producer_tx.send(region).unwrap();
                }
            })
            .detach();
        }
    }
}
