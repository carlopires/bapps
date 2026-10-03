use std::{error::Error, fmt, time::Duration};

use bapps_trio::{NurseryError, StartError};

use crate::ExitReason;

#[derive(Debug)]
#[non_exhaustive]
pub enum OtpError {
    ChildStartFailed {
        child: String,
        reason: ExitReason,
    },
    ChildStartProtocol {
        child: String,
        detail: String,
    },
    RestartIntensityExceeded {
        supervisor: String,
        restarts: usize,
        max_restarts: usize,
        within: Duration,
    },
    Nursery(String),
    Registry(String),
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
            Self::Registry(detail) => write!(f, "registry failure: {detail}"),
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
