//! Issue #460 — strict invariant assertions: re-entrancy protection and
//! state-desync detection in the event-sink.
//!
//! # What is tested
//!
//! 1. **Re-entrancy guard** — after `emit` or `batch_emit` completes normally,
//!    the `Executing` instance flag is cleared, so a subsequent call succeeds.
//!    (True re-entrant calls cannot be constructed in the Soroban test harness,
//!    but we verify the flag lifecycle by inspecting storage directly.)
//!
//! 2. **Counter monotonicity invariant** — after every successful `emit` and
//!    `batch_emit` call, `entry_count()` equals the last assigned `entry_id + 1`.
//!
//! 3. **Guard cleared on validation failure** — when `emit` returns an error
//!    (e.g. `InvalidSequence`), the `Executing` flag must not be left set,
//!    so the next valid call succeeds.
//!
//! 4. **New error variants carry correct messages and groups.**

use soroban_sdk::{Bytes, Symbol, Vec};

use super::common::*;
use crate::{DataKey, SinkEntry, SinkError};

// ---------------------------------------------------------------------------
// §1  Executing flag is cleared after successful emit
// ---------------------------------------------------------------------------

#[test]
fn executing_flag_is_clear_after_successful_emit() {
    let h = Harness::new();
    let _id = h.client.emit(&h.emitter, &h.entry(1));

    // Inspect the instance storage directly: the flag must be absent.
    let flag: Option<bool> = h.env.as_contract(&h.contract_id, || {
        h.env.storage().instance().get(&DataKey::Executing)
    });
    assert!(
        flag.is_none() || flag == Some(false),
        "Executing flag must be cleared after successful emit, got {:?}",
        flag
    );
}

#[test]
fn executing_flag_is_clear_after_successful_batch_emit() {
    let h = Harness::new();
    let mut entries = Vec::new(&h.env);
    entries.push_back(h.entry(1));
    entries.push_back(h.entry(2));
    let _first = h.client.batch_emit(&h.emitter, &entries);

    let flag: Option<bool> = h.env.as_contract(&h.contract_id, || {
        h.env.storage().instance().get(&DataKey::Executing)
    });
    assert!(
        flag.is_none() || flag == Some(false),
        "Executing flag must be cleared after successful batch_emit, got {:?}",
        flag
    );
}

// ---------------------------------------------------------------------------
// §2  Counter monotonicity invariant
// ---------------------------------------------------------------------------

#[test]
fn entry_count_equals_last_id_plus_one_after_emit() {
    let h = Harness::new();
    for seq in 1u64..=5 {
        let id = h.client.emit(&h.emitter, &h.entry(seq));
        let count = h.client.entry_count();
        assert_eq!(
            count,
            id + 1,
            "entry_count() must equal last entry_id + 1 after emit #{seq}: \
             got count={count} id={id}"
        );
    }
}

#[test]
fn entry_count_advances_by_batch_size_after_batch_emit() {
    let h = Harness::new();
    let batch_size = 4u32;
    let mut entries = Vec::new(&h.env);
    for seq in 1..=batch_size {
        entries.push_back(h.entry(seq as u64));
    }

    let count_before = h.client.entry_count();
    let first_id = h.client.batch_emit(&h.emitter, &entries);
    let count_after = h.client.entry_count();

    assert_eq!(
        count_after,
        count_before + batch_size as u64,
        "entry_count must advance by {batch_size} after batch_emit"
    );
    assert_eq!(
        count_after,
        first_id + batch_size as u64,
        "entry_count must equal first_id + batch_size"
    );
}

#[test]
fn interleaved_emit_and_batch_emit_maintain_counter_invariant() {
    let h = Harness::new();

    let id0 = h.client.emit(&h.emitter, &h.entry(1));
    assert_eq!(h.client.entry_count(), id0 + 1);

    let mut batch = Vec::new(&h.env);
    batch.push_back(h.entry(2));
    batch.push_back(h.entry(3));
    let first_batch = h.client.batch_emit(&h.emitter, &batch);
    assert_eq!(h.client.entry_count(), first_batch + 2);

    let id3 = h.client.emit(&h.emitter, &h.entry(4));
    assert_eq!(h.client.entry_count(), id3 + 1);
    assert_eq!(id3, 3, "fourth entry must receive id 3");
}

// ---------------------------------------------------------------------------
// §3  Executing flag cleared on validation failure
// ---------------------------------------------------------------------------

#[test]
fn executing_flag_cleared_after_failed_emit_invalid_sequence() {
    let h = Harness::new();
    let bad_entry = SinkEntry {
        source_contract: h.emitter.clone(),
        topic: Symbol::new(&h.env, "evt"),
        sequence: 0, // invalid
        payload: Bytes::new(&h.env),
    };
    let err = h
        .client
        .try_emit(&h.emitter, &bad_entry)
        .unwrap_err()
        .unwrap();
    assert_eq!(err, SinkError::InvalidSequence);

    // The guard must be cleared so the next valid call succeeds.
    let flag: Option<bool> = h.env.as_contract(&h.contract_id, || {
        h.env.storage().instance().get(&DataKey::Executing)
    });
    assert!(
        flag.is_none() || flag == Some(false),
        "Executing flag must be cleared after failed emit, got {:?}",
        flag
    );

    // Confirm: a valid emit still works.
    let id = h.client.emit(&h.emitter, &h.entry(1));
    assert_eq!(id, 0);
}

#[test]
fn executing_flag_cleared_after_failed_batch_emit_validation_error() {
    let h = Harness::new();
    let bad_entry = SinkEntry {
        source_contract: h.emitter.clone(),
        topic: Symbol::new(&h.env, "evt"),
        sequence: 0, // invalid
        payload: Bytes::new(&h.env),
    };
    let mut entries = Vec::new(&h.env);
    entries.push_back(h.entry(1));
    entries.push_back(bad_entry);

    let err = h
        .client
        .try_batch_emit(&h.emitter, &entries)
        .unwrap_err()
        .unwrap();
    assert_eq!(err, SinkError::InvalidSequence);

    // Guard cleared.
    let flag: Option<bool> = h.env.as_contract(&h.contract_id, || {
        h.env.storage().instance().get(&DataKey::Executing)
    });
    assert!(
        flag.is_none() || flag == Some(false),
        "Executing flag must be cleared after failed batch_emit, got {:?}",
        flag
    );

    // Next valid call works.
    let id = h.client.emit(&h.emitter, &h.entry(1));
    assert_eq!(id, 0);
}

// ---------------------------------------------------------------------------
// §4  New error variants carry correct messages and groups
// ---------------------------------------------------------------------------

#[test]
fn reentrancy_guard_error_message_and_group() {
    assert_eq!(
        SinkError::ReentrancyGuard.message(),
        "re-entrant call detected; another emit is already in progress"
    );
    assert_eq!(SinkError::ReentrancyGuard.group(), 'E');
}

#[test]
fn invariant_violation_error_message_and_group() {
    assert_eq!(
        SinkError::InvariantViolation.message(),
        "post-condition invariant check failed; state desync detected"
    );
    assert_eq!(SinkError::InvariantViolation.group(), 'E');
}
