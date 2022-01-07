use std::{collections::VecDeque, mem, sync::Arc, thread};

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
        purge_threshold: ReadableSize::gb(10),
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
    let handles: Vec<_> = (0..2)
        .map(move |i| {
            let engine = engine.clone();
            let producer_tx = producer_tx.clone();
            let consumer_rx = consumer_rx.clone();
            LocalExecutorBuilder::new(Placement::Fixed(i + 1))
                .spawn(move || run_worker(engine, consumer_rx, producer_tx))
                .unwrap()
        })
        .collect();
    for _ in 0..10000000 {
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
    drop(consumer_tx);
    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}

async fn run_worker(
    engine: Arc<Engine>,
    consumer_rx: async_channel::Receiver<Region>,
    producer_tx: Sender<Region>,
) {
    loop {
        let mut batch = LogBatch::with_capacity(256);
        let mut regions = Vec::new();
        match consumer_rx.recv().await {
            Ok(mut region) => {
                batch
                    .add_entries::<MessageExtTyped>(region.id, &region.entries)
                    .unwrap();
                region.entries.clear();
                regions.push(region);
            }
            Err(_) => break,
        }
        while let Ok(mut region) = consumer_rx.try_recv() {
            batch
                .add_entries::<MessageExtTyped>(region.id, &region.entries)
                .unwrap();
            region.entries.clear();
            regions.push(region);
            if regions.len() >= 64 {
                break;
            }
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
