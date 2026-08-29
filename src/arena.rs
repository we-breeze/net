//! Process-wide storage for short-lived encoded request frames.

use std::sync::OnceLock;

pub use brz_ds::{EphemeralBytes, EphemeralBytesArena, EphemeralBytesMut};

use crate::{NetError, Result};

/// Compile-time default capacity of each of the two request arena chunks.
///
/// The default process-wide reserved storage is therefore 128 MiB.
pub const DEFAULT_REQUEST_ARENA_CHUNK_SIZE: usize = 64 * 1024 * 1024;

static GLOBAL_REQUEST_ARENA: OnceLock<EphemeralBytesArena> = OnceLock::new();

/// Initialize the process-wide request arena during application startup.
///
/// `chunk_size` applies to each of the arena's two chunks. Initialization must
/// happen before any SDK client calls [`global_request_arena`]. Repeating the
/// same value is allowed; changing an initialized arena is rejected because
/// runtime resize is intentionally unsupported.
pub fn init_global_request_arena(chunk_size: usize) -> Result<&'static EphemeralBytesArena> {
    if chunk_size == 0 || chunk_size >= u32::MAX as usize {
        return Err(NetError::InvalidConfig(format!(
            "global request arena chunk size must be in 1..{}, got {chunk_size}",
            u32::MAX
        )));
    }

    let arena = GLOBAL_REQUEST_ARENA.get_or_init(|| EphemeralBytesArena::new(chunk_size));
    let configured = arena.chunk_capacity();
    if configured != chunk_size {
        return Err(NetError::RequestArenaAlreadyInitialized {
            configured,
            requested: chunk_size,
        });
    }
    Ok(arena)
}

/// Return the process-wide request arena.
///
/// The first call lazily initializes it with
/// [`DEFAULT_REQUEST_ARENA_CHUNK_SIZE`]. Applications that need another size
/// must call [`init_global_request_arena`] before constructing SDK clients.
pub fn global_request_arena() -> &'static EphemeralBytesArena {
    GLOBAL_REQUEST_ARENA.get_or_init(|| EphemeralBytesArena::new(DEFAULT_REQUEST_ARENA_CHUNK_SIZE))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_chunk_size_does_not_initialize_the_global_arena() {
        assert!(matches!(
            init_global_request_arena(0),
            Err(NetError::InvalidConfig(_))
        ));
    }

    #[test]
    fn global_arena_is_shared_and_cannot_be_resized() {
        let first = global_request_arena();
        assert_eq!(first.chunk_capacity(), DEFAULT_REQUEST_ARENA_CHUNK_SIZE);

        let second = init_global_request_arena(DEFAULT_REQUEST_ARENA_CHUNK_SIZE).unwrap();
        assert!(std::ptr::eq(first, second));

        assert!(matches!(
            init_global_request_arena(DEFAULT_REQUEST_ARENA_CHUNK_SIZE + 1),
            Err(NetError::RequestArenaAlreadyInitialized {
                configured: DEFAULT_REQUEST_ARENA_CHUNK_SIZE,
                requested,
            }) if requested == DEFAULT_REQUEST_ARENA_CHUNK_SIZE + 1
        ));
    }
}
