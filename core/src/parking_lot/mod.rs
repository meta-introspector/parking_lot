// Copyright 2016 Amanieu d'Antras
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
use crate::thread_parker::{ThreadParker, ThreadParkerT, UnparkHandleT};
use crate::util::UncheckedOptionExt;
use crate::word_lock::WordLock;
use core::{
    cell::{Cell, UnsafeCell},
    ptr,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};
use smallvec::SmallVec;
use std::time::{Duration, Instant};

// Don't use Instant on wasm32-unknown-unknown, it just panics.
cfg_if::cfg_if! {
    if #[cfg(all(
        target_family = "wasm",
        target_os = "unknown",
        target_vendor = "unknown"
    ))] {
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        pub struct TimeoutInstant;
        impl TimeoutInstant {
            pub fn now() -> TimeoutInstant {
                TimeoutInstant
            }
        }
        impl core::ops::Add<Duration> for TimeoutInstant {
            type Output = Self;
            fn add(self, _rhs: Duration) -> Self::Output {
                TimeoutInstant
            }
        }
    } else {
        pub use std::time::Instant as TimeoutInstant;
    }
}

pub mod thread_data;
pub use thread_data::ThreadData;

pub mod get_hashtable;
pub use get_hashtable::get_hashtable;

pub mod create_hashtable;
pub use create_hashtable::create_hashtable;

pub mod grow_hashtable;
pub use grow_hashtable::grow_hashtable;

pub mod fair_timeout;
pub use fair_timeout::*;
pub mod bucket;
pub use bucket::*;

pub mod rehash_bucket_into;
pub use rehash_bucket_into::rehash_bucket_into;

pub mod hash_table;
pub use hash_table::*;

pub mod hash;
pub use hash::hash;

pub mod lock_bucket;
pub use lock_bucket::lock_bucket;

pub mod lock_bucket_checked;
pub use lock_bucket_checked::lock_bucket_checked;

pub mod lock_bucket_pair;
pub use lock_bucket_pair::lock_bucket_pair;

pub mod unlock_bucket_pair;
pub use unlock_bucket_pair::unlock_bucket_pair;

pub mod park_result;
pub use park_result::ParkResult;

pub mod unpark_result;
pub use unpark_result::UnparkResult;

pub mod requeue_op;
pub use requeue_op::RequeueOp;

pub mod filter_op;
pub use filter_op::FilterOp;
pub mod unpark_token;
pub use unpark_token::UnparkToken;

pub mod park_token;
pub use park_token::ParkToken;

pub mod default_unpark_token;
pub use default_unpark_token::DEFAULT_UNPARK_TOKEN;

pub mod default_park_token;
pub use default_park_token::DEFAULT_PARK_TOKEN;

pub mod park;
pub use park::park;

pub mod unpark_one;
pub use unpark_one::unpark_one;

pub mod unpark_all;
pub use unpark_all::unpark_all;

pub mod unpark_requeue;
pub use unpark_requeue::unpark_requeue;

pub mod unpark_filter;
pub use unpark_filter::unpark_filter;

pub mod with_thread_data;
pub use with_thread_data::with_thread_data;
pub mod deadlock;

#[cfg(feature = "deadlock_detection")]
pub mod deadlock_impl;

#[cfg(test)]
pub mod tests;
