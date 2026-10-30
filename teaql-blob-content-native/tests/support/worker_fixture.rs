//! Explicit test-only ELF. Production worker accepts no fault/debug commands.
use std::{
    io::{Read, Write},
    process::ExitCode,
};
fn main() -> ExitCode {
    if std::env::args().nth(1).as_deref() == Some("--probe-v1") {
        return teaql_blob_content_native::worker_main();
    }
    // SAFETY: fixed operation, no shared pointer; suppress core collection in fault tests.
    unsafe {
        libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0);
    }
    let mut first = [0];
    // Staging leaves the inherited open description at EOF. Reopen independently,
    // exactly as the real worker does, instead of changing the parent's offset.
    std::fs::File::open("/proc/self/fd/0")
        .unwrap()
        .read_exact(&mut first)
        .unwrap();
    match first[0] {
        b'H' => loop {
            std::thread::sleep(std::time::Duration::from_millis(50));
        },
        b'C' => {
            let mut n = 1u64;
            loop {
                n = std::hint::black_box(n.wrapping_mul(6364136223846793005).wrapping_add(1));
            }
        }
        b'M' => {
            let mut blocks = Vec::new();
            loop {
                blocks.push(vec![0xff; 8388608]);
                std::hint::black_box(&blocks);
            }
        }
        b'O' => loop {
            let _ = std::io::stdout().write_all(&[b'x'; 1024]);
        },
        b'J' => {
            print!(
                "{{\"schema\":1,\"version\":\"{}\",\"body\":{{\"kind\":\"Verified\",\"content_type\":\"application/pdf\",\"size_bytes\":1,\"sha256\":\"{}\",\"unknown\":true}}}}",
                env!("CARGO_PKG_VERSION"),
                "a".repeat(64)
            );
            ExitCode::SUCCESS
        }
        b'B' => {
            print!(
                "{{\"schema\":1,\"version\":\"{}\",\"body\":{{\"kind\":\"Verified\",\"content_type\":\"application/pdf\",\"size_bytes\":1,\"sha256\":\"{}\"}}}}",
                env!("CARGO_PKG_VERSION"),
                "a".repeat(64)
            );
            ExitCode::SUCCESS
        }
        b'V' => {
            print!(
                "{{\"schema\":1,\"version\":\"wrong\",\"body\":{{\"kind\":\"Failure\",\"code\":\"BLOB_FORMAT_NOT_ALLOWED\"}}}}"
            );
            ExitCode::from(2)
        }
        b'E' => {
            // SAFETY: query only; no descriptor or process state is changed.
            if unsafe { libc::fcntl(0, libc::F_GETFL) } & libc::O_ACCMODE != libc::O_RDONLY {
                return ExitCode::from(17);
            }
            let descriptors: Vec<i32> = std::fs::read_dir("/proc/self/fd")
                .unwrap()
                .map(|entry| {
                    entry
                        .unwrap()
                        .file_name()
                        .to_str()
                        .unwrap()
                        .parse()
                        .unwrap()
                })
                .collect();
            for fd in descriptors.into_iter().filter(|fd| *fd > 2) {
                // SAFETY: F_GETFD only queries a number; the directory iterator
                // has already closed its own temporary descriptor.
                if unsafe { libc::fcntl(fd, libc::F_GETFD) } != -1 {
                    return ExitCode::from(17);
                }
            }
            let environment = std::env::vars().collect::<std::collections::BTreeMap<_, _>>();
            let expected = std::collections::BTreeMap::from([
                ("LANG".to_owned(), "C".to_owned()),
                ("LC_ALL".to_owned(), "C".to_owned()),
                ("TZ".to_owned(), "UTC".to_owned()),
            ]);
            if environment != expected
                || std::env::current_dir().unwrap() != std::path::Path::new("/")
            {
                return ExitCode::from(17);
            }
            print!(
                "{{\"schema\":1,\"version\":\"{}\",\"body\":{{\"kind\":\"Failure\",\"code\":\"BLOB_SIZE_EXCEEDED\"}}}}",
                env!("CARGO_PKG_VERSION")
            );
            ExitCode::from(2)
        }
        b'D' => {
            print!(
                "{{\"schema\":1,\"schema\":1,\"version\":\"{}\",\"body\":{{\"kind\":\"Failure\",\"code\":\"BLOB_SIZE_EXCEEDED\"}}}}",
                env!("CARGO_PKG_VERSION")
            );
            ExitCode::from(2)
        }
        b'T' => {
            print!(
                "{{\"schema\":1,\"version\":\"{}\",\"body\":{{\"kind\":\"Failure\",\"code\":\"BLOB_SIZE_EXCEEDED\"}}}} trailing",
                env!("CARGO_PKG_VERSION")
            );
            ExitCode::from(2)
        }
        b'X' => ExitCode::from(17),
        _ => teaql_blob_content_native::worker_main(),
    }
}
