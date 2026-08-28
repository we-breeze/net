use std::{
    cell::UnsafeCell,
    fmt,
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    ptr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use atomic_waker::AtomicWaker;

pub const MAX_IN_FLIGHT: usize = 256;
const WORD_BITS: usize = u64::BITS as usize;
const FREE_WORDS: usize = MAX_IN_FLIGHT / WORD_BITS;

const OCCUPIED: usize = 1 << 0;
const DRIVER_CLAIMED: usize = 1 << 1;
const READY: usize = 1 << 2;
const RECEIVER_DONE: usize = 1 << 3;
const VALUE_TAKEN: usize = 1 << 4;
const DRIVER_DONE: usize = 1 << 5;

/// Opaque per-connection request identifier.
///
/// The low eight bits select one of 256 response slots. The remaining bits are
/// a generation that prevents a late response from completing a reused slot.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestToken(u64);

impl RequestToken {
    const INDEX_MASK: u64 = (MAX_IN_FLIGHT - 1) as u64;

    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    #[inline]
    pub const fn index(self) -> usize {
        (self.0 & Self::INDEX_MASK) as usize
    }

    #[inline]
    const fn generation(self) -> u64 {
        self.0 >> 8
    }
}

impl fmt::Debug for RequestToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestToken")
            .field("id", &self.0)
            .field("slot", &self.index())
            .finish()
    }
}

pub(crate) struct CompletionTable<T> {
    slots: Box<[ResponseSlot<T>; MAX_IN_FLIGHT]>,
    free: [AtomicU64; FREE_WORDS],
    cursor: AtomicUsize,
    active: AtomicUsize,
}

struct ResponseSlot<T> {
    generation: AtomicU64,
    state: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
    waker: AtomicWaker,
}

// State transitions guarantee one driver writer and one Future reader.
unsafe impl<T: Send> Sync for ResponseSlot<T> {}

impl<T> CompletionTable<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            slots: Box::new(std::array::from_fn(|_| ResponseSlot {
                generation: AtomicU64::new(0),
                state: AtomicUsize::new(0),
                value: UnsafeCell::new(MaybeUninit::uninit()),
                waker: AtomicWaker::new(),
            })),
            free: std::array::from_fn(|_| AtomicU64::new(u64::MAX)),
            cursor: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
        })
    }

    pub(crate) fn reserve(self: &Arc<Self>) -> Option<(RequestToken, ResponseFuture<T>)> {
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % FREE_WORDS;
        for offset in 0..FREE_WORDS {
            let word_index = (start + offset) % FREE_WORDS;
            let word = &self.free[word_index];
            let mut available = word.load(Ordering::Relaxed);
            while available != 0 {
                let bit = available.trailing_zeros() as usize;
                let mask = 1_u64 << bit;
                match word.compare_exchange_weak(
                    available,
                    available & !mask,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let index = word_index * WORD_BITS + bit;
                        let slot = &self.slots[index];
                        let generation = slot
                            .generation
                            .fetch_add(1, Ordering::Relaxed)
                            .wrapping_add(1);
                        let token = RequestToken((generation << 8) | index as u64);
                        debug_assert_eq!(slot.state.load(Ordering::Relaxed), 0);
                        slot.state.store(OCCUPIED, Ordering::Release);
                        self.active.fetch_add(1, Ordering::Relaxed);
                        return Some((
                            token,
                            ResponseFuture {
                                table: self.clone(),
                                token,
                                completed: false,
                            },
                        ));
                    }
                    Err(actual) => available = actual,
                }
            }
        }
        None
    }

    /// Releases a reservation that was never published to the connection task.
    pub(crate) fn abort(&self, token: RequestToken) {
        let slot = &self.slots[token.index()];
        debug_assert_eq!(slot.generation.load(Ordering::Relaxed), token.generation());
        debug_assert_eq!(slot.state.load(Ordering::Acquire), OCCUPIED);
        self.release(token);
    }

    /// Complete one request. Returns false for stale or duplicate responses.
    pub(crate) fn complete(&self, token: RequestToken, value: T) -> bool {
        let slot = &self.slots[token.index()];
        loop {
            if slot.generation.load(Ordering::Acquire) != token.generation() {
                return false;
            }
            let mut state = slot.state.load(Ordering::Acquire);
            if state & OCCUPIED == 0 {
                return false;
            }
            if state & DRIVER_CLAIMED != 0 {
                if state & READY != 0 {
                    return false;
                }
                std::hint::spin_loop();
                continue;
            }
            match slot.state.compare_exchange_weak(
                state,
                state | DRIVER_CLAIMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // A stale response may observe the old generation just
                    // before a free slot is reserved again. Recheck after
                    // claiming the writer bit so it can never write into the
                    // new generation. The new writer will wait for this bit.
                    if slot.generation.load(Ordering::Acquire) != token.generation() {
                        slot.state.fetch_and(!DRIVER_CLAIMED, Ordering::Release);
                        return false;
                    }
                    break;
                }
                Err(actual) => state = actual,
            }
        }

        // SAFETY: DRIVER_CLAIMED gives the connection task exclusive write
        // access. The receiver cannot read until READY is published.
        unsafe { (*slot.value.get()).write(value) };
        slot.state.fetch_or(READY, Ordering::Release);
        slot.waker.wake();

        let previous = slot.state.fetch_or(DRIVER_DONE, Ordering::AcqRel);
        if previous & RECEIVER_DONE != 0 {
            if previous & VALUE_TAKEN == 0 {
                // SAFETY: READY is set and the receiver declared it will never
                // consume the value.
                unsafe { ptr::drop_in_place((*slot.value.get()).as_mut_ptr()) };
            }
            self.release(token);
        }
        true
    }

    pub(crate) fn is_driver_pending(&self, token: RequestToken) -> bool {
        let slot = &self.slots[token.index()];
        slot.generation.load(Ordering::Acquire) == token.generation()
            && slot.state.load(Ordering::Acquire) & (OCCUPIED | DRIVER_DONE) == OCCUPIED
    }

    pub(crate) fn active(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }

    fn receive(&self, token: RequestToken) -> Option<T> {
        let slot = &self.slots[token.index()];
        debug_assert_eq!(slot.generation.load(Ordering::Acquire), token.generation());
        let state = slot.state.load(Ordering::Acquire);
        if state & READY == 0 {
            return None;
        }

        // SAFETY: READY publishes the initialized value and this Future is the
        // only receiver. VALUE_TAKEN is set immediately after the move.
        let value = unsafe { (*slot.value.get()).assume_init_read() };
        let previous = slot
            .state
            .fetch_or(RECEIVER_DONE | VALUE_TAKEN, Ordering::AcqRel);
        debug_assert_eq!(previous & RECEIVER_DONE, 0);
        if previous & DRIVER_DONE != 0 {
            self.release(token);
        }
        Some(value)
    }

    fn receiver_dropped(&self, token: RequestToken) {
        let slot = &self.slots[token.index()];
        if slot.generation.load(Ordering::Acquire) != token.generation() {
            return;
        }
        let previous = slot.state.fetch_or(RECEIVER_DONE, Ordering::AcqRel);
        debug_assert_eq!(previous & RECEIVER_DONE, 0);
        if previous & DRIVER_DONE != 0 {
            if previous & READY != 0 && previous & VALUE_TAKEN == 0 {
                // SAFETY: the driver is finished and this is the only receiver.
                unsafe { ptr::drop_in_place((*slot.value.get()).as_mut_ptr()) };
            }
            self.release(token);
        }
    }

    fn release(&self, token: RequestToken) {
        let index = token.index();
        let slot = &self.slots[index];
        debug_assert_eq!(slot.generation.load(Ordering::Relaxed), token.generation());
        let _ = slot.waker.take();
        slot.state.store(0, Ordering::Release);
        self.active.fetch_sub(1, Ordering::Relaxed);
        let word = index / WORD_BITS;
        let bit = index % WORD_BITS;
        let previous = self.free[word].fetch_or(1_u64 << bit, Ordering::Release);
        debug_assert_eq!(previous & (1_u64 << bit), 0);
    }
}

/// Allocation-free response future backed by one reusable session slot.
pub struct ResponseFuture<T> {
    table: Arc<CompletionTable<T>>,
    token: RequestToken,
    completed: bool,
}

impl<T> ResponseFuture<T> {
    #[inline]
    pub fn request_token(&self) -> RequestToken {
        self.token
    }

    pub(crate) fn abort(mut self) {
        self.table.abort(self.token);
        self.completed = true;
    }
}

impl<T> Unpin for ResponseFuture<T> {}

impl<T> fmt::Debug for ResponseFuture<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResponseFuture")
            .field("token", &self.token)
            .field("completed", &self.completed)
            .finish()
    }
}

impl<T> Future for ResponseFuture<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.completed, "ResponseFuture polled after completion");
        let slot = &self.table.slots[self.token.index()];
        slot.waker.register(context.waker());
        if let Some(value) = self.table.receive(self.token) {
            self.completed = true;
            Poll::Ready(value)
        } else {
            Poll::Pending
        }
    }
}

impl<T> Drop for ResponseFuture<T> {
    fn drop(&mut self) {
        if !self.completed {
            self.table.receiver_dropped(self.token);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn response_can_arrive_before_the_future_is_polled() {
        let table = CompletionTable::new();
        let (token, future) = table.reserve().unwrap();
        assert!(table.complete(token, 42));
        assert_eq!(future.await, 42);
        assert_eq!(table.active(), 0);
    }

    #[test]
    fn all_slots_are_fixed_and_reuse_changes_the_generation() {
        let table = CompletionTable::<usize>::new();
        let mut reservations = Vec::new();
        for _ in 0..MAX_IN_FLIGHT {
            reservations.push(table.reserve().unwrap());
        }
        assert!(table.reserve().is_none());

        let (old, old_future) = reservations.pop().unwrap();
        drop(old_future);
        assert!(table.complete(old, 1));
        let (new, new_future) = table.reserve().unwrap();
        assert_eq!(old.index(), new.index());
        assert_ne!(old, new);
        assert!(!table.complete(old, 2));
        drop(new_future);
        assert!(table.complete(new, 3));

        for (token, future) in reservations {
            drop(future);
            assert!(table.complete(token, 0));
        }
        assert_eq!(table.active(), 0);
    }

    #[test]
    fn dropping_the_receiver_releases_after_driver_completion() {
        let table = CompletionTable::new();
        let (token, future) = table.reserve().unwrap();
        drop(future);
        assert_eq!(table.active(), 1);
        assert!(table.complete(token, String::from("unused")));
        assert_eq!(table.active(), 0);
    }
}
