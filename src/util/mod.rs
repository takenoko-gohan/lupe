#[cfg(windows)]
pub(crate) mod named_pipe;
#[cfg(unix)]
pub(crate) mod uds;
