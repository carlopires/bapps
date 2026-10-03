use std::{error::Error, fmt, time::Duration};

use bapps_trio::{NurseryError, StartError};

use crate::ExitReason;

/// Why a supervisor, and with it its application, stopped abnormally.
#[derive(Debug)]
#[non_exhaustive]
pub enum OtpError {
    /// A child exited instead of reporting readiness.
    ChildStartFailed {
        /// Its path.
        child: String,
        /// How it exited.
        reason: ExitReason,
    },
    /// The readiness handshake broke: the parent stopped waiting for this
    /// child's readiness.
    ChildStartProtocol {
        /// Its path.
        child: String,
        /// What went wrong.
        detail: String,
    },
    /// Children failed more often than the supervisor's restart intensity
    /// allows; the supervisor gave up and escalated.
    RestartIntensityExceeded {
        /// The supervisor's path.
        supervisor: String,
        /// Restarts within the window.
        restarts: usize,
        /// The allowed maximum.
        max_restarts: usize,
        /// The window.
        within: Duration,
    },
    /// The supervisor's own task group failed.
    Nursery(String),
    /// An internal supervisor wait ended unexpectedly. Indicates a bug in this
    /// crate; please report it.
    Application(String),
}

impl fmt::Display for OtpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChildStartFailed { child, reason } => {
                write!(f, "child {child} failed before readiness: {reason}")
            }
            Self::ChildStartProtocol { child, detail } => {
                write!(f, "child {child} startup protocol failed: {detail}")
            }
            Self::RestartIntensityExceeded {
                supervisor,
                restarts,
                max_restarts,
                within,
            } => write!(
                f,
                "supervisor {supervisor} exceeded restart intensity: {restarts} restarts (max {max_restarts}) within {within:?}"
            ),
            Self::Nursery(detail) => write!(f, "structured-concurrency failure: {detail}"),
            Self::Application(detail) => write!(f, "application failure: {detail}"),
        }
    }
}

impl Error for OtpError {}

pub(crate) fn map_start<E: fmt::Debug>(child: &str, error: StartError<E>) -> OtpError {
    OtpError::ChildStartProtocol {
        child: child.to_owned(),
        detail: format!("start failed: {error:?}"),
    }
}

pub(crate) fn map_nursery<E: fmt::Debug>(error: NurseryError<E>) -> OtpError {
    OtpError::Nursery(format!("{error:?}"))
}
