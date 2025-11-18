use crate::parking_lot::ThreadData;

// Invokes the given closure with a reference to the current thread `ThreadData`.
#[inline(always)]
pub fn with_thread_data<T>(f: impl FnOnce(&ThreadData) -> T) -> T {
    // Unlike word_lock::ThreadData, parking_lot::ThreadData is always expensive
    // to construct. Try to use a thread-local version if possible. Otherwise just
    // create a ThreadData on the stack
    let mut thread_data_storage = None;
    thread_local!(static THREAD_DATA: ThreadData = ThreadData::new());
    let thread_data_ptr = THREAD_DATA
        .try_with(|x| x as *const ThreadData)
        .unwrap_or_else(|_| thread_data_storage.get_or_insert_with(ThreadData::new));

    f(unsafe { &*thread_data_ptr })
}