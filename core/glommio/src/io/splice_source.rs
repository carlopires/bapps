// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::io::{BufferedFile, DmaFile};
use std::os::unix::io::{AsRawFd, RawFd};

mod sealed {
    pub trait Sealed {}
    impl Sealed for crate::io::BufferedFile {}
    impl Sealed for crate::io::DmaFile {}
}

/// A file whose contents can be sent straight to a socket, without the bytes
/// passing through this process.
///
/// Implemented for [`BufferedFile`] and [`DmaFile`]. It is sealed: the two
/// methods below are an internal contract with
/// [`TcpStream::send_file`](crate::net::TcpStream::send_file), not an
/// extension point.
pub trait SpliceSource: sealed::Sealed {
    /// The descriptor to splice from.
    fn splice_fd(&self) -> RawFd;

    /// The alignment a splice offset must satisfy.
    ///
    /// `1` for a page-cache file, which accepts any offset. A file opened
    /// `O_DIRECT` requires its offset to be a multiple of the device's
    /// logical block size, and splicing from a misaligned offset fails with
    /// `EINVAL`.
    fn splice_offset_alignment(&self) -> u64;
}

impl SpliceSource for BufferedFile {
    fn splice_fd(&self) -> RawFd {
        self.as_raw_fd()
    }

    fn splice_offset_alignment(&self) -> u64 {
        1
    }
}

impl SpliceSource for DmaFile {
    fn splice_fd(&self) -> RawFd {
        self.as_raw_fd()
    }

    fn splice_offset_alignment(&self) -> u64 {
        self.alignment()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{BufferedFile, DmaFile};
    use crate::LocalExecutor;
    use std::os::unix::io::AsRawFd;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("glommio-splice-{}-{}", std::process::id(), name));
        p
    }

    #[test]
    fn buffered_file_needs_no_alignment() {
        LocalExecutor::default().run(async {
            let path = tmp_path("buffered-align");
            let file = BufferedFile::create(&path).await.unwrap();
            assert_eq!(
                file.splice_offset_alignment(),
                1,
                "a page-cache file can be spliced from any offset"
            );
            assert_eq!(file.splice_fd(), file.as_raw_fd());
            file.close().await.unwrap();
            std::fs::remove_file(&path).ok();
        });
    }

    #[test]
    fn dma_file_reports_its_o_direct_alignment() {
        LocalExecutor::default().run(async {
            let path = tmp_path("dma-align");
            let file = DmaFile::create(&path).await.unwrap();
            let alignment = file.splice_offset_alignment();
            assert!(
                alignment.is_power_of_two(),
                "alignment must be a power of two, got {alignment}"
            );
            assert_eq!(
                alignment,
                file.align_up(1),
                "the trait must report the same alignment the file already enforces"
            );
            assert_eq!(file.splice_fd(), file.as_raw_fd());
            file.close().await.unwrap();
            std::fs::remove_file(&path).ok();
        });
    }
}
