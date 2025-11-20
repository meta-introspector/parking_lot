use crate::parking_lot::{get_hashtable, lock_bucket, with_thread_data, ThreadData, NUM_THREADS};
use core::sync::atomic::Ordering;
use crate::thread_parker::{ThreadParkerT, UnparkHandleT};
use crate::word_lock::WordLock;
use backtrace::Backtrace;
use petgraph;
use petgraph::graphmap::DiGraphMap;

#[cfg(feature = "deadlock_detection")]
pub mod deadlock_impl {
use crate::parking_lot::{get_hashtable, lock_bucket, with_thread_data, ThreadData};
    use crate::parking_lot::thread_data::NUM_THREADS;
    use crate::thread_parker::{ThreadParkerT, UnparkHandleT};
    use crate::word_lock::WordLock;
    use ::backtrace::Backtrace;
    use ::petgraph;
    use ::petgraph::graphmap::DiGraphMap;
    use std::cell::{Cell, UnsafeCell};
    use std::collections::HashSet;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::thread::ThreadId;

    /// Representation of a deadlocked thread
    pub struct DeadlockedThread {
        pub thread_id: ThreadId,
        pub backtrace: Backtrace,
    }

    impl DeadlockedThread {
        /// The system thread id
        pub fn thread_id(&self) -> ThreadId {
            self.thread_id
        }

        /// The thread backtrace
        pub fn backtrace(&self) -> &Backtrace {
            &self.backtrace
        }
    }



    pub(super) unsafe fn on_unpark(td: &ThreadData) {
        if td.deadlock_data.deadlocked.get() {
            let sender = td.deadlock_data.backtrace_sender.get().take().unwrap();
            sender
                .send(DeadlockedThread {
                    thread_id: td.deadlock_data.thread_id,
                    backtrace: Backtrace::new(),
                })
                .unwrap();
            // make sure to close this sender
            drop(sender);

            // park until the end of the time
            td.parker.prepare_park();
            td.parker.park();
            unreachable!("unparked deadlocked thread!");
        }
    }

    pub unsafe fn acquire_resource(key: usize) {
        with_thread_data(|thread_data| {
            thread_data.deadlock_data.resources.get().push(key);
        });
    }

    pub unsafe fn release_resource(key: usize) {
        with_thread_data(|thread_data| {
            let resources = &mut *thread_data.deadlock_data.resources.get();

            // There is only one situation where we can fail to find the
            // resource: we are currently running TLS destructors and our
            // ThreadData has already been freed. There isn't much we can do
            // about it at this point, so just ignore it.
            if let Some(p) = resources.iter().rposition(|x| *x == key) {
                resources.swap_remove(p);
            }
        });
    }

    pub fn check_deadlock() -> Vec<Vec<DeadlockedThread>> {
        unsafe {
            // fast pass
            if check_wait_graph_fast() {
                // double check
                check_wait_graph_slow()
            } else {
                Vec::new()
            }
        }
    }

    // Simple algorithm that builds a wait graph f the threads and the resources,
    // then checks for the presence of cycles (deadlocks).
    // This variant isn't precise as it doesn't lock the entire table before checking
    pub unsafe fn check_wait_graph_fast() -> bool {
        let table = get_hashtable();
        let thread_count = NUM_THREADS.load(Ordering::Relaxed);
        let mut graph = DiGraphMap::<usize, ()>::with_capacity(thread_count * 2, thread_count * 2);

        for b in &(*table).entries[..] {
            b.mutex.lock();
            let mut current = b.queue_head.get();
            while !current.is_null() {
                if !(*current).parked_with_timeout.get()
                    && !(*current).deadlock_data.deadlocked.get()
                {
                    // .resources are waiting for their owner
                    for &resource in &(*current).deadlock_data.resources.get() {
                        graph.add_edge(Resource(resource), Thread(current), ());
                    }
                    // owner waits for resource .key
                    graph.add_edge(current as usize, (*current).key.load(Ordering::Relaxed), ());
                }
                current = (*current).next_in_queue.get();
            }
            // SAFETY: We hold the lock here, as required
            b.mutex.unlock();
        }

        petgraph::algo::is_cyclic_directed(&graph)
    }

    #[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
    pub enum WaitGraphNode {
        Thread(*const ThreadData),
        Resource(usize),
    }

    use self::WaitGraphNode::*;

    // Contrary to the _fast variant this locks the entries table before looking for cycles.
    // Returns all detected thread wait cycles.
    // Note that once a cycle is reported it's never reported again.
    pub unsafe fn check_wait_graph_slow() -> Vec<Vec<DeadlockedThread>> {
        static DEADLOCK_DETECTION_LOCK: WordLock = WordLock::new();
        DEADLOCK_DETECTION_LOCK.lock();

        let mut table = get_hashtable();
        loop {
            // Lock all buckets in the old table
            for b in &table.entries[..] {
                b.mutex.lock();
            }

            // Now check if our table is still the latest one. Another thread could
            // have grown the hash table between us getting and locking the hash table.
            let new_table = get_hashtable();
            if new_table as *const _ == table as *const _ {
                break;
            }

            // Unlock buckets and try again
            for b in &table.entries[..] {
                // SAFETY: We hold the lock here, as required
                b.mutex.unlock();
            }

            table = new_table;
        }

        let thread_count = NUM_THREADS.load(Ordering::Relaxed);
        let mut graph =
            DiGraphMap::<WaitGraphNode, ()>::with_capacity(thread_count * 2, thread_count * 2);

        for b in &table.entries[..] {
            let mut current = b.queue_head.get();
            while !current.is_null() {
                if !(*current).parked_with_timeout.get()
                    && !(*current).deadlock_data.deadlocked.get()
                {
                    // .resources are waiting for their owner
                    for &resource in &(*current).deadlock_data.resources.get() {
                        graph.add_edge(Resource(resource), Thread(current), ());
                    }
                    // owner waits for resource .key
                    graph.add_edge(
                        Thread(current),
                        Resource((*current).key.load(Ordering::Relaxed)),
                        (),
                    );
                }
                current = (*current).next_in_queue.get();
            }
        }

        for b in &table.entries[..] {
            // SAFETY: We hold the lock here, as required
            b.mutex.unlock();
        }

        // find cycles
        let cycles = graph_cycles(&graph);

        let mut results = Vec::with_capacity(cycles.len());

        for cycle in cycles {
            let (sender, receiver) = mpsc::channel();
            for td in cycle {
                let bucket = lock_bucket((*td).key.load(Ordering::Relaxed));
                td.deadlock_data.deadlocked.set(true);
                *td.deadlock_data.backtrace_sender.get() = Some(sender.clone());
                let handle = (*td).parker.unpark_lock();
                // SAFETY: We hold the lock here, as required
                bucket.mutex.unlock();
                // unpark the deadlocked thread!
                // on unpark it'll notice the deadlocked flag and report back
                handle.unpark();
            }
            // make sure to drop our sender before collecting results
            drop(sender);
            results.push(receiver.iter().collect());
        }

        DEADLOCK_DETECTION_LOCK.unlock();

        results
    }

    // normalize a cycle to start with the "smallest" node
    pub fn normalize_cycle<T: Ord + Copy + Clone>(input: &[T]) -> Vec<T> {
        let min_pos = input
            .iter()
            .enumerate()
            .min_by_key(|&(_, &t)| t)
            .map(|(p, _)| p)
            .unwrap_or(0);
        input
            .iter()
            .cycle()
            .skip(min_pos)
            .take(input.len())
            .cloned()
            .collect()
    }

    // returns all thread cycles in the wait graph
    pub fn graph_cycles(g: &DiGraphMap<WaitGraphNode, ()>) -> Vec<Vec<*const ThreadData>> {
        use petgraph::visit::depth_first_search;
        use petgraph::visit::DfsEvent;
        use petgraph::visit::NodeIndexable;

        let mut cycles = HashSet::new();
        let mut path = Vec::with_capacity(g.node_bound());
        // start from threads to get the correct threads cycle
        let threads = g
            .nodes()
            .filter(|n| if let &Thread(_) = n { true } else { false });

        depth_first_search(g, threads, |e| match e {
            DfsEvent::Discover(Thread(n), _) => path.push(n),
            DfsEvent::Finish(Thread(_), _) => {
                path.pop();
            }
            DfsEvent::BackEdge(_, Thread(n)) => {
                let from = path.iter().rposition(|&i| i == n).unwrap();
                cycles.insert(normalize_cycle(&path[from..]));
            }
            _ => (),
        });

        cycles.iter().cloned().collect()
    }
}