use std::path::PathBuf;

pub(crate) fn get_sock_path() -> PathBuf {
    std::env::temp_dir().join("alb-logview.sock")
}
