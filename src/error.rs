// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Errors.

/// Result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Everything an object store can report.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The object does not exist (ranged reads only; `get`/`head` return `None`).
    #[error("object not found")]
    NotFound,
    /// A [`Cond::CreateOnly`](crate::Cond::CreateOnly) write found an existing object.
    #[error("object already exists")]
    Conflict,
    /// A [`Cond::IfGeneration`](crate::Cond::IfGeneration) write found another
    /// generation (or no object).
    #[error("generation precondition failed")]
    PreconditionFailed,
    /// The key or prefix is not valid (see [`Key::parse`](crate::Key::parse)).
    #[error("invalid key: {reason}")]
    InvalidKey {
        /// Why.
        reason: &'static str,
    },
    /// The store could not be reached or refused for a transient reason.
    #[error("store unavailable: {message}")]
    Unavailable {
        /// What happened (never contains credentials).
        message: String,
        /// Worth retrying.
        retriable: bool,
    },
    /// Authentication or authorization failed.
    #[error("not authorized: {message}")]
    Auth {
        /// What happened (never contains credentials).
        message: String,
    },
    /// The body exceeds a configured limit.
    #[error("object larger than the {limit}-byte limit")]
    TooLarge {
        /// The limit.
        limit: u64,
    },
    /// The store answered with something unexpected.
    #[error("protocol error: {message}")]
    Protocol {
        /// What was wrong.
        message: String,
    },
    /// A fenced publish lost to another writer (see [`crate::fenced`]).
    /// Stop writing; re-read before publishing again.
    #[error("fenced: {reason}")]
    Fenced {
        /// Why.
        reason: &'static str,
    },
    /// Published data failed verification against the record naming it.
    #[error("corrupt: {message}")]
    Corrupt {
        /// What was wrong.
        message: String,
    },
    /// Local I/O.
    #[error("i/o: {source}")]
    Io {
        /// The underlying error.
        #[from]
        source: std::io::Error,
    },
    // New variants go last: a variant's position is its discriminant.
    /// A ranged read of an object stored with a content encoding: a byte
    /// range of an encoded (compressed) stream is meaningless. Read it whole
    /// with `get`, which decodes it.
    #[error("ranged read of a {encoding}-encoded object: read it whole")]
    EncodedRange {
        /// The object's `Content-Encoding`.
        encoding: String,
    },
    /// A content encoding this build cannot write or decode (an unknown
    /// token, or `gzip` without feature `gzip`).
    #[error("unsupported content encoding {encoding:?}")]
    UnsupportedEncoding {
        /// The encoding named (at most 64 characters).
        encoding: String,
    },
    /// The object was replaced or deleted while a read that had to resume
    /// was in flight. Nothing from either generation is returned (two
    /// generations are never spliced); read it again.
    #[error("object replaced during a resumed read")]
    Changed,
    /// A request refused by the transport's request policy before any byte
    /// was sent (see `http::Client::with_policy`).
    #[error("refused by policy: {0}")]
    Policy(#[from] PolicyError),
    /// Credential discovery found no credential it is allowed to use (for
    /// example under `token::MetadataPolicy::Forbid` with no user
    /// credentials), or an empty token was supplied. Nothing was contacted.
    #[error("no credentials: {reason}")]
    NoCredentials {
        /// Why.
        reason: &'static str,
    },
}

/// Why a request policy refused a request. Never carries the URL's query
/// string, a body or a token.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PolicyError {
    /// The method, scheme, host, port or path is not one the policy allows.
    #[error("endpoint not allowed: {reason}")]
    Endpoint {
        /// Which rule refused it.
        reason: &'static str,
    },
    /// The SQL text of a `BigQuery` query job was refused.
    #[error("query not allowed: {reason}")]
    Query {
        /// Why (the policy's own wording).
        reason: std::borrow::Cow<'static, str>,
    },
    /// A request to a `BigQuery` query endpoint whose SQL could not be
    /// isolated for checking (unreadable body, non-query or legacy-SQL job,
    /// or a field that changes what the SQL reads). Refused under every
    /// policy, so no query reaches `BigQuery` unchecked.
    #[error("query body not checkable: {reason}")]
    UncheckableQuery {
        /// What was wrong.
        reason: &'static str,
    },
}

impl Error {
    /// Worth retrying (transient unavailability; never conflicts or preconditions).
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        matches!(self, Error::Unavailable { retriable: true, .. })
    }

    #[cfg_attr(not(any(feature = "gcs", test)), allow(dead_code))]
    pub(crate) fn unavailable(message: impl Into<String>, retriable: bool) -> Self {
        Error::Unavailable { message: message.into(), retriable }
    }

    pub(crate) fn corrupt(message: impl Into<String>) -> Self {
        Error::Corrupt { message: message.into() }
    }

    #[cfg_attr(not(feature = "gcs"), allow(dead_code))]
    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Error::Protocol { message: message.into() }
    }
}
