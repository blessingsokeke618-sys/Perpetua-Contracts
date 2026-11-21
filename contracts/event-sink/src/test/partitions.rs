//! Deterministic ingress-level simulations for disconnected and faulty emitters.
//!
//! Soroban executes one contract instance and cannot model consensus between
//! replicas. These scenarios model partitioned emitters buffering submissions
//! until connectivity returns, and verify the sink's counter and validation
//! behavior when those submissions arrive.

use soroban_sdk::testutils::{Address as _, Events as _};
use soroban_sdk::{Bytes, Symbol, Vec};

use super::common::*;
use crate::{SinkEntry, SinkError};

fn entry_from(h: &Harness, source: &soroban_sdk::Address, sequence: u64) -> SinkEntry {
    SinkEntry {
        source_contract: source.clone(),
        topic: Symbol::new(&h.env, "partition_event"),
        sequence,
        payload: Bytes::from_slice(&h.env, &sequence.to_be_bytes()),
    }
}

#[test]
fn buffered_partition_delivery_keeps_global_ids_contiguous() {
    let h = Harness::new();
    let partitioned_emitter = soroban_sdk::Address::generate(&h.env);
    h.client.register_emitter(&partitioned_emitter);

    // The online emitter continues while the other partition buffers work.
    let online_first = h.client.emit(&h.emitter, &h.entry(1));
    let mut buffered = Vec::new(&h.env);
    buffered.push_back(entry_from(&h, &partitioned_emitter, 1));
    buffered.push_back(entry_from(&h, &partitioned_emitter, 2));
    let online_second = h.client.emit(&h.emitter, &h.entry(2));

    // Healing the partition delivers its queued entries as a single batch.
    h.advance(30);
    let buffered_first = h.client.batch_emit(&partitioned_emitter, &buffered);

    assert_eq!(online_first, 0);
    assert_eq!(online_second, 1);
    assert_eq!(buffered_first, 2);
    assert_eq!(h.client.entry_count(), 4);
}

#[test]
fn malformed_byzantine_batch_is_atomic_and_other_emitters_can_continue() {
    let h = Harness::new();
    let second_emitter = soroban_sdk::Address::generate(&h.env);
    h.client.register_emitter(&second_emitter);

    let mut faulty_batch = Vec::new(&h.env);
    faulty_batch.push_back(entry_from(&h, &second_emitter, 1));
    faulty_batch.push_back(entry_from(&h, &second_emitter, 0));

    let err = h
        .client
        .try_batch_emit(&second_emitter, &faulty_batch)
        .unwrap_err()
        .unwrap();
    assert_eq!(err, SinkError::InvalidSequence);
    assert!(
        h.env
            .events()
            .all()
            .filter_by_contract(&h.contract_id)
            .events()
            .is_empty(),
        "a rejected batch must not publish partial entries"
    );
    assert_eq!(h.client.entry_count(), 0);

    let healthy_id = h.client.emit(&h.emitter, &h.entry(1));
    assert_eq!(healthy_id, 0);

    let recovery_id = h
        .client
        .emit(&second_emitter, &entry_from(&h, &second_emitter, 1));
    assert_eq!(recovery_id, 1);
    assert_eq!(h.client.entry_count(), 2);
}

#[test]
fn unregistered_partition_cannot_inject_events_or_consume_ids() {
    let h = Harness::new();
    let err = h
        .client
        .try_emit(&h.other, &entry_from(&h, &h.other, 1))
        .unwrap_err()
        .unwrap();

    assert_eq!(err, SinkError::CallerNotRegisteredEmitter);
    assert_eq!(h.client.entry_count(), 0);
    assert_eq!(h.client.emit(&h.emitter, &h.entry(1)), 0);
    assert_eq!(h.client.entry_count(), 1);
}
