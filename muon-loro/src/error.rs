use kernel::OwnedPath;
use loro::{LoroEncodeError, LoroError, UpdateTimeoutError};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Loro(#[from] LoroError),
    #[error(transparent)]
    TextUpdate(#[from] UpdateTimeoutError),
    #[error(transparent)]
    Encode(#[from] LoroEncodeError),
    #[error("the Loro root container cannot be replaced")]
    EmptyPath,
    #[error("the Loro model root must be attached to a document")]
    DetachedRoot,
    #[error("the Loro document already has {0} pending operations")]
    PendingTransaction(usize),
    #[error("the model root is missing from the forked Loro document")]
    MissingForkRoot,
    #[error("Loro path does not exist: {0}")]
    MissingPath(OwnedPath),
    #[error("expected a container while traversing {0}")]
    ExpectedContainer(OwnedPath),
    #[error("expected a text container at {0}")]
    ExpectedText(OwnedPath),
    #[error("expected a counter container at {0}")]
    ExpectedCounter(OwnedPath),
    #[error("expected a list container at {0}")]
    ExpectedList(OwnedPath),
    #[error("expected a map container at {0}")]
    ExpectedMap(OwnedPath),
    #[error("expected a movable-list container at {0}")]
    ExpectedMovableList(OwnedPath),
    #[error("path segment is incompatible with its parent container at {0}")]
    InvalidSegment(OwnedPath),
    #[error("identity path segments are not supported at {0}")]
    UnsupportedIdentity(OwnedPath),
    #[error("index is out of bounds at {0}")]
    IndexOutOfBounds(OwnedPath),
    #[error("value is outside Loro's integer range")]
    IntegerOutOfRange,
    #[error("cannot serialize the replacement value: {0}")]
    Serialization(String),
    #[error("cannot deserialize the Loro model")]
    Deserialization(#[from] serde_json::Error),
}

impl serde::ser::Error for Error {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        Self::Serialization(message.to_string())
    }
}
