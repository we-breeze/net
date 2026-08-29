use std::{cell::UnsafeCell, fmt, mem::MaybeUninit, ops::Range, slice, sync::Arc};

#[cfg(all(loom, test))]
use loom::sync::atomic::{AtomicUsize, Ordering};
#[cfg(not(all(loom, test)))]
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::{BufMut, Bytes, buf::UninitSlice};

/// Default maximum capacity used by the connection receive ring.
pub const DEFAULT_MAX_RX_BUFFER_CAPACITY: usize = 64 * 1024 * 1024;

// The connection driver calls `shrink` once a minute. Match ghbreeze's
// wall-clock policy (20/64 checks at 30 seconds) while halving maintenance
// wakeups for large endpoint sets.
const EMPTY_SHRINK_CYCLES: u8 = 10;
const NON_EMPTY_SHRINK_CYCLES: u8 = 32;

/// A receive buffer cannot satisfy a protocol's reservation without exceeding
/// its configured upper bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("receive buffer requires {required} bytes, above its {maximum}-byte limit")]
pub struct RxCapacityError {
    pub required: usize,
    pub maximum: usize,
}

/// Connection-local dynamically resized ring buffer.
///
/// `read <= taken <= write`: bytes before `read` are reusable, bytes between
/// `read` and `taken` belong to response guards, and bytes between `taken` and
/// `write` are available to the protocol decoder. Resizing copies only the
/// unparsed suffix. Existing response guards keep the old allocation alive,
/// so a slow caller never prevents the connection from receiving more data.
pub struct RxBuffer {
    backing: Option<Arc<Backing>>,
    minimum: usize,
    maximum: usize,
    read: usize,
    taken: usize,
    write: usize,
    taken_frames: usize,
    releases: Arc<Releases>,
    shrink: ShrinkPolicy,
}

#[derive(Debug, Default)]
struct ShrinkPolicy {
    peak_len: usize,
    low_usage_cycles: u8,
}

impl ShrinkPolicy {
    #[inline]
    fn record(&mut self, len: usize) {
        self.peak_len = self.peak_len.max(len);
    }

    #[inline]
    fn reset(&mut self, current_len: usize) {
        self.peak_len = current_len;
        self.low_usage_cycles = 0;
    }

    fn target(&mut self, current_len: usize, capacity: usize, minimum: usize) -> Option<usize> {
        self.record(current_len);
        if capacity <= minimum {
            self.reset(current_len);
            return None;
        }

        // A peak above 25% means the current allocation is still useful.
        // Start a fresh observation window after that burst has passed.
        if self.peak_len > capacity / 4 {
            self.reset(current_len);
            return None;
        }

        self.low_usage_cycles = self.low_usage_cycles.saturating_add(1);
        let required_cycles = if current_len == 0 {
            EMPTY_SHRINK_CYCLES
        } else {
            NON_EMPTY_SHRINK_CYCLES
        };
        if self.low_usage_cycles < required_cycles {
            return None;
        }

        let target = self
            .peak_len
            .saturating_mul(2)
            .max(current_len)
            .max(minimum)
            .next_power_of_two();
        self.reset(current_len);
        (target < capacity).then_some(target)
    }
}

#[cfg_attr(loom, allow(dead_code))]
impl RxBuffer {
    pub(crate) fn new(minimum: usize, maximum: usize) -> Self {
        debug_assert!(minimum > 0 && minimum.is_power_of_two());
        debug_assert!(maximum >= minimum && maximum.is_power_of_two());
        Self {
            backing: None,
            minimum,
            maximum,
            read: 0,
            taken: 0,
            write: 0,
            taken_frames: 0,
            releases: Arc::new(Releases::default()),
            shrink: ShrinkPolicy::default(),
        }
    }

    /// Construct an in-memory receive buffer for protocol tests and adapters.
    pub fn with_capacity(capacity: usize) -> Self {
        let minimum = capacity.max(1).next_power_of_two();
        let maximum = DEFAULT_MAX_RX_BUFFER_CAPACITY.max(minimum);
        Self::new(minimum, maximum)
    }

    /// Append bytes as though they had been read from a socket.
    ///
    /// The connection driver writes directly into the ring; this convenience
    /// method is primarily useful to exercise protocol decoders in isolation.
    pub fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), RxCapacityError> {
        self.reserve(bytes.len())?;
        self.put_slice(bytes);
        Ok(())
    }

    /// Number of unparsed bytes currently visible to the protocol.
    #[inline]
    pub fn len(&self) -> usize {
        self.write - self.taken
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.backing.as_ref().map_or(0, |backing| backing.len())
    }

    #[inline]
    pub fn maximum_capacity(&self) -> usize {
        self.maximum
    }

    /// Return one byte relative to the protocol-visible cursor.
    #[inline]
    pub fn byte(&self, offset: usize) -> Option<u8> {
        if offset >= self.len() {
            return None;
        }
        let backing = self.backing.as_ref()?;
        let index = ring_index(self.taken + offset, backing.len());
        // SAFETY: `[taken, write)` is initialized and the connection driver
        // never writes into that interval while the decoder borrows `self`.
        Some(unsafe { *backing.ptr().add(index) })
    }

    /// Find the first CRLF pair at or after `offset`.
    pub fn find_crlf(&self, offset: usize) -> Option<usize> {
        if offset >= self.len() {
            return None;
        }
        let end = self.len().saturating_sub(1);
        (offset..end)
            .find(|&index| self.byte(index) == Some(b'\r') && self.byte(index + 1) == Some(b'\n'))
    }

    /// Compare a logical range without requiring it to be physically linear.
    pub fn range_eq(&self, range: Range<usize>, expected: &[u8]) -> bool {
        if range.end > self.len() || range.len() != expected.len() {
            return false;
        }
        expected
            .iter()
            .enumerate()
            .all(|(index, byte)| self.byte(range.start + index) == Some(*byte))
    }

    /// Reserve writable capacity. Protocols should call this after decoding a
    /// length prefix, allowing a large frame to grow the ring only once.
    pub fn reserve(&mut self, additional: usize) -> Result<(), RxCapacityError> {
        self.gc();
        if additional <= self.available() {
            return Ok(());
        }

        let required = self.len().checked_add(additional).ok_or(RxCapacityError {
            required: usize::MAX,
            maximum: self.maximum,
        })?;
        if required > self.maximum {
            return Err(RxCapacityError {
                required,
                maximum: self.maximum,
            });
        }

        let capacity = required
            .max(self.minimum)
            .next_power_of_two()
            .min(self.maximum);
        self.resize(capacity);
        Ok(())
    }

    /// Move one complete response out of the decoder-visible interval.
    pub fn take(&mut self, length: usize) -> RxFrame {
        assert!(length > 0 && length <= self.len());
        let backing = self
            .backing
            .as_ref()
            .expect("a non-empty receive buffer has backing storage")
            .clone();
        let start = ring_index(self.taken, backing.len());
        self.taken += length;
        self.taken_frames = self
            .taken_frames
            .checked_add(1)
            .expect("receive frame counter overflow");
        RxFrame {
            backing,
            start,
            length,
            releases: self.releases.clone(),
        }
    }

    /// Consume bytes that do not have to escape the decoder.
    #[inline]
    pub fn advance(&mut self, length: usize) {
        drop(self.take(length));
        self.gc();
    }

    pub(crate) fn reset(&mut self) {
        self.backing = None;
        self.read = 0;
        self.taken = 0;
        self.write = 0;
        self.taken_frames = 0;
        self.releases = Arc::new(Releases::default());
        self.shrink.reset(0);
    }

    /// Periodic control-plane maintenance. It is intentionally not called by
    /// request admission or response lookup paths.
    pub(crate) fn shrink(&mut self) {
        self.gc();
        let current_len = self.len();
        if let Some(target) = self
            .shrink
            .target(current_len, self.capacity(), self.minimum)
        {
            self.resize(target);
        }
    }

    pub(crate) fn is_full(&mut self) -> bool {
        self.gc();
        self.capacity() > 0 && self.available() == 0
    }

    pub(crate) fn prepare_read(&mut self) -> Result<(), RxCapacityError> {
        self.gc();
        if self.backing.is_none() {
            self.resize(self.minimum);
        } else if self.available() == 0 && self.is_empty() {
            // All storage belongs to guards. Rotate to a fresh allocation of
            // the smallest useful size; guards retain the old backing.
            self.resize(self.minimum);
        }
        Ok(())
    }

    fn available(&self) -> usize {
        self.capacity() - (self.write - self.read)
    }

    fn gc(&mut self) {
        if self.taken_frames == 0 {
            return;
        }
        if self.releases.count.load(Ordering::Acquire) < self.taken_frames {
            return;
        }
        self.read = self.taken;
        self.taken_frames = 0;
        self.releases = Arc::new(Releases::default());
        if self.read == self.write {
            // Logical counters need not grow forever on an idle connection.
            self.read = 0;
            self.taken = 0;
            self.write = 0;
        }
    }

    fn resize(&mut self, capacity: usize) {
        debug_assert!(capacity >= self.minimum);
        debug_assert!(capacity <= self.maximum);
        debug_assert!(capacity.is_power_of_two());
        debug_assert!(capacity >= self.len());

        let unread = self.len();
        let replacement = Arc::new(Backing::new(capacity));
        if unread > 0 {
            let source = self
                .backing
                .as_ref()
                .expect("unread receive bytes have backing storage");
            let (first, second) = source.segments(ring_index(self.taken, source.len()), unread);
            // SAFETY: replacement is new and uniquely owned here.
            unsafe {
                std::ptr::copy_nonoverlapping(first.as_ptr(), replacement.ptr(), first.len());
                std::ptr::copy_nonoverlapping(
                    second.as_ptr(),
                    replacement.ptr().add(first.len()),
                    second.len(),
                );
            }
        }

        self.backing = Some(replacement);
        self.read = 0;
        self.taken = 0;
        self.write = unread;
        self.taken_frames = 0;
        self.releases = Arc::new(Releases::default());
        self.shrink.reset(unread);
    }
}

// SAFETY: `chunk_mut` exposes only the free ring interval and `advance_mut`
// publishes exactly the initialized prefix written by the caller. Guarded and
// decoder-visible intervals never overlap that free interval.
unsafe impl BufMut for RxBuffer {
    #[inline]
    fn remaining_mut(&self) -> usize {
        self.available()
    }

    #[inline]
    unsafe fn advance_mut(&mut self, length: usize) {
        assert!(length <= self.available());
        self.write += length;
        self.shrink.record(self.write - self.taken);
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        self.gc();
        let available = self.available();
        if available == 0 {
            return UninitSlice::uninit(&mut []);
        }
        let backing = self
            .backing
            .as_ref()
            .expect("prepare_read installs receive storage");
        let capacity = backing.len();
        let offset = ring_index(self.write, capacity);
        let length = available.min(capacity - offset);
        // SAFETY: this is the writable interval outside `[read, write)` and
        // the allocation remains alive for the returned mutable borrow.
        unsafe { UninitSlice::from_raw_parts_mut(backing.ptr().add(offset), length) }
    }
}

impl fmt::Debug for RxBuffer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RxBuffer")
            .field("capacity", &self.capacity())
            .field("read", &self.read)
            .field("taken", &self.taken)
            .field("write", &self.write)
            .field("taken_frames", &self.taken_frames)
            .field("peak_len", &self.shrink.peak_len)
            .field("low_usage_cycles", &self.shrink.low_usage_cycles)
            .finish()
    }
}

/// One complete response retained from an [`RxBuffer`].
pub struct RxFrame {
    backing: Arc<Backing>,
    start: usize,
    length: usize,
    releases: Arc<Releases>,
}

impl RxFrame {
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    #[inline]
    pub fn is_contiguous(&self) -> bool {
        self.start + self.length <= self.backing.len()
    }

    pub fn byte(&self, offset: usize) -> Option<u8> {
        if offset >= self.length {
            return None;
        }
        let index = ring_index(self.start + offset, self.backing.len());
        // SAFETY: the frame's guard keeps its immutable backing alive.
        Some(unsafe { *self.backing.ptr().add(index) })
    }

    pub fn copy_range(&self, range: Range<usize>) -> Bytes {
        assert!(range.end <= self.length);
        let mut output = Vec::with_capacity(range.len());
        let start = ring_index(self.start + range.start, self.backing.len());
        let (first, second) = self.backing.segments(start, range.len());
        output.extend_from_slice(first);
        output.extend_from_slice(second);
        Bytes::from(output)
    }

    /// Convert a physically contiguous response to an owner suitable for
    /// `Bytes::from_owner`. A wrapped response is returned unchanged.
    pub fn into_contiguous(self) -> Result<ContiguousRxFrame, Self> {
        if self.is_contiguous() {
            Ok(ContiguousRxFrame(self))
        } else {
            Err(self)
        }
    }
}

impl fmt::Debug for RxFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RxFrame")
            .field("start", &self.start)
            .field("length", &self.length)
            .field("capacity", &self.backing.len())
            .finish()
    }
}

impl Drop for RxFrame {
    fn drop(&mut self) {
        self.releases.count.fetch_add(1, Ordering::Release);
    }
}

/// A response whose bytes occupy one physical ring segment.
pub struct ContiguousRxFrame(RxFrame);

impl AsRef<[u8]> for ContiguousRxFrame {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: construction verifies that the whole range is contiguous;
        // the contained guard keeps it immutable and allocated.
        unsafe { slice::from_raw_parts(self.0.backing.ptr().add(self.0.start), self.0.length) }
    }
}

impl fmt::Debug for ContiguousRxFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Default)]
struct Releases {
    count: AtomicUsize,
}

struct Backing {
    data: UnsafeCell<Box<[MaybeUninit<u8>]>>,
}

impl Backing {
    fn new(capacity: usize) -> Self {
        Self {
            data: UnsafeCell::new(Box::<[u8]>::new_uninit_slice(capacity)),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        // SAFETY: the boxed allocation is never replaced or resized.
        unsafe { (&*self.data.get()).len() }
    }

    #[inline]
    fn ptr(&self) -> *mut u8 {
        // SAFETY: callers enforce disjoint initialized/readable and writable
        // ring intervals; the boxed allocation itself never moves.
        unsafe { (&*self.data.get()).as_ptr().cast_mut().cast::<u8>() }
    }

    fn segments(&self, start: usize, length: usize) -> (&[u8], &[u8]) {
        debug_assert!(start < self.len() || length == 0);
        debug_assert!(length <= self.len());
        let first_len = length.min(self.len() - start);
        let second_len = length - first_len;
        // SAFETY: callers only request initialized ring intervals and backing
        // allocations remain alive for the returned borrow.
        unsafe {
            (
                slice::from_raw_parts(self.ptr().add(start), first_len),
                slice::from_raw_parts(self.ptr(), second_len),
            )
        }
    }
}

// Access is synchronized by the single connection task. Response owners only
// read ranges removed from the writable interval.
unsafe impl Send for Backing {}
unsafe impl Sync for Backing {}

#[inline]
fn ring_index(position: usize, capacity: usize) -> usize {
    position & (capacity - 1)
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::RxBuffer;

    fn write(buffer: &mut RxBuffer, bytes: &[u8]) {
        buffer.extend_from_slice(bytes).unwrap();
    }

    #[test]
    fn wrapped_frame_preserves_logical_order() {
        let mut buffer = RxBuffer::new(8, 64);
        write(&mut buffer, b"12345678");
        drop(buffer.take(6));
        buffer.gc();
        write(&mut buffer, b"abcd");

        let frame = buffer.take(6);
        assert!(!frame.is_contiguous());
        assert_eq!(frame.copy_range(0..frame.len()), b"78abcd".as_slice());
    }

    #[test]
    fn resize_copies_only_unparsed_bytes() {
        let mut buffer = RxBuffer::new(8, 64);
        write(&mut buffer, b"abcdefgh");
        let retained = buffer.take(4);
        buffer.reserve(12).unwrap();

        assert_eq!(buffer.capacity(), 16);
        assert_eq!(buffer.len(), 4);
        assert_eq!(buffer.byte(0), Some(b'e'));
        assert_eq!(retained.copy_range(0..4), b"abcd".as_slice());
    }

    #[test]
    fn held_frame_does_not_block_fresh_receive_storage() {
        let mut buffer = RxBuffer::new(8, 64);
        write(&mut buffer, b"response");
        let retained = buffer.take(8);
        buffer.prepare_read().unwrap();
        write(&mut buffer, b"next");

        assert_eq!(retained.copy_range(0..8), b"response".as_slice());
        assert_eq!(buffer.len(), 4);
        assert_eq!(buffer.capacity(), 8);
    }

    #[test]
    fn maintenance_waits_for_sustained_low_usage_before_shrinking() {
        let mut buffer = RxBuffer::new(8, 64);
        write(&mut buffer, b"0123456789abcdef0123456789abcdef");
        drop(buffer.take(32));

        // The first check observes the burst and starts a fresh window.
        buffer.shrink();
        assert_eq!(buffer.capacity(), 32);

        for _ in 0..9 {
            buffer.shrink();
            assert_eq!(buffer.capacity(), 32);
        }
        buffer.shrink();

        assert!(buffer.is_empty());
        assert_eq!(buffer.capacity(), 8);
    }

    #[test]
    fn recurring_usage_above_one_quarter_prevents_shrinking() {
        let mut buffer = RxBuffer::new(8, 64);
        write(&mut buffer, b"0123456789abcdef0123456789abcdef");
        drop(buffer.take(32));
        buffer.shrink();

        for _ in 0..20 {
            write(&mut buffer, b"012345678");
            drop(buffer.take(9));
            buffer.shrink();
        }

        assert_eq!(buffer.capacity(), 32);
    }

    #[test]
    fn maintenance_keeps_twice_the_recent_peak() {
        let mut buffer = RxBuffer::new(8, 64);
        write(&mut buffer, b"0123456789abcdef0123456789abcdef01234567");
        drop(buffer.take(40));
        buffer.shrink();

        for _ in 0..10 {
            write(&mut buffer, b"0123456789ab");
            drop(buffer.take(12));
            buffer.shrink();
        }

        assert_eq!(buffer.capacity(), 32);
    }

    #[test]
    fn non_empty_buffer_waits_longer_and_preserves_data() {
        let mut buffer = RxBuffer::new(8, 64);
        write(&mut buffer, b"0123456789abcdef0123456789abcdef01234567");
        drop(buffer.take(40));
        buffer.shrink();
        write(&mut buffer, b"abcdefgh");

        for _ in 0..31 {
            buffer.shrink();
            assert_eq!(buffer.capacity(), 64);
        }
        buffer.shrink();

        assert_eq!(buffer.capacity(), 16);
        assert_eq!(buffer.len(), 8);
        assert!(buffer.range_eq(0..8, b"abcdefgh"));
    }

    #[test]
    fn shrinking_does_not_invalidate_a_retained_frame() {
        let mut buffer = RxBuffer::new(8, 64);
        write(&mut buffer, b"0123456789abcdef0123456789abcdef01234567");
        let retained = buffer.take(40);
        buffer.shrink();

        for _ in 0..10 {
            buffer.shrink();
        }

        assert_eq!(buffer.capacity(), 8);
        assert_eq!(
            retained.copy_range(0..retained.len()),
            b"0123456789abcdef0123456789abcdef01234567".as_slice()
        );
    }

    #[test]
    fn reservation_enforces_the_configured_upper_bound() {
        let mut buffer = RxBuffer::new(8, 16);
        write(&mut buffer, b"12345678");
        let error = buffer.reserve(9).unwrap_err();

        assert_eq!(error.required, 17);
        assert_eq!(error.maximum, 16);
        assert_eq!(buffer.capacity(), 8);
        assert_eq!(buffer.len(), 8);
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use loom::{
        model::Builder,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
    };

    use super::{Releases, RxBuffer};

    fn model(body: impl Fn() + Send + Sync + 'static) {
        let mut builder = Builder::new();
        builder.max_branches = 100_000;
        builder.preemption_bound = Some(3);
        builder.check(move || body());
    }

    #[test]
    fn loom_all_guards_must_release_before_reuse() {
        model(|| {
            let releases = Arc::new(Releases::default());
            let published = Arc::new(AtomicUsize::new(0));

            let first_releases = releases.clone();
            let first_published = published.clone();
            let first = thread::spawn(move || {
                first_published.fetch_or(1, Ordering::Relaxed);
                first_releases.count.fetch_add(1, Ordering::Release);
            });

            let second_releases = releases.clone();
            let second_published = published.clone();
            let second = thread::spawn(move || {
                second_published.fetch_or(2, Ordering::Relaxed);
                second_releases.count.fetch_add(1, Ordering::Release);
            });

            let observed = releases.count.load(Ordering::Acquire);
            assert!(observed <= 2);
            if observed == 2 {
                assert_eq!(published.load(Ordering::Relaxed), 3);
            }

            first.join().unwrap();
            second.join().unwrap();
            assert_eq!(releases.count.load(Ordering::Acquire), 2);
            assert_eq!(published.load(Ordering::Relaxed), 3);
        });
    }

    #[test]
    fn loom_old_generation_release_cannot_release_new_frames() {
        model(|| {
            let old = Arc::new(Releases::default());
            let new = Arc::new(Releases::default());
            let thread_old = old.clone();
            let release = thread::spawn(move || {
                thread_old.count.fetch_add(1, Ordering::Release);
            });

            assert_eq!(new.count.load(Ordering::Acquire), 0);
            release.join().unwrap();
            assert_eq!(old.count.load(Ordering::Acquire), 1);
            assert_eq!(new.count.load(Ordering::Acquire), 0);
        });
    }

    #[test]
    fn loom_ring_never_reuses_backing_before_every_guard_drops() {
        model(|| {
            let mut buffer = RxBuffer::new(8, 64);
            buffer.extend_from_slice(b"abcdefgh").unwrap();
            let old_backing = buffer.backing.as_ref().unwrap().ptr() as usize;
            let old_releases = buffer.releases.clone();
            let first = buffer.take(4);
            let second = buffer.take(4);

            let drop_first = thread::spawn(move || drop(first));
            let drop_second = thread::spawn(move || drop(second));

            buffer.prepare_read().unwrap();
            let current_backing = buffer.backing.as_ref().unwrap().ptr() as usize;
            if current_backing == old_backing {
                assert_eq!(old_releases.count.load(Ordering::Acquire), 2);
            }

            drop_first.join().unwrap();
            drop_second.join().unwrap();
            assert_eq!(old_releases.count.load(Ordering::Acquire), 2);
        });
    }
}
