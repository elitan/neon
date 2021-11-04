//!
//! WAL redo. This service runs PostgreSQL in a special wal_redo mode
//! to apply given WAL records over an old page image and return new
//! page image.
//!
//! We rely on Postgres to perform WAL redo for us. We launch a
//! postgres process in special "wal redo" mode that's similar to
//! single-user mode. We then pass the previous page image, if any,
//! and all the WAL records we want to apply, to the postgres
//! process. Then we get the page image back. Communication with the
//! postgres process happens via stdin/stdout
//!
//! See src/backend/tcop/zenith_wal_redo.c for the other side of
//! this communication.
//!
//! The Postgres process is assumed to be secure against malicious WAL
//! records. It achieves it by dropping privileges before replaying
//! any WAL records, so that even if an attacker hijacks the Postgres
//! process, he cannot escape out of it.
//!
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use lazy_static::lazy_static;
use log::*;
use nix::poll::*;
use serde::Serialize;
use std::fs;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::{Error, ErrorKind};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::process::Stdio;
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use zenith_metrics::{register_histogram, register_int_counter, Histogram, IntCounter};
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;
use zenith_utils::nonblock::set_nonblock;
use zenith_utils::zid::ZTenantId;

use crate::relish::*;
use crate::repository::WALRecord;
use crate::waldecoder::XlMultiXactCreate;
use crate::waldecoder::XlXactParsedRecord;
use crate::PageServerConf;
use postgres_ffi::nonrelfile_utils::mx_offset_to_flags_bitshift;
use postgres_ffi::nonrelfile_utils::mx_offset_to_flags_offset;
use postgres_ffi::nonrelfile_utils::mx_offset_to_member_offset;
use postgres_ffi::nonrelfile_utils::transaction_id_set_status;
use postgres_ffi::pg_constants;
use postgres_ffi::XLogRecord;

///
/// `RelTag` + block number (`blknum`) gives us a unique id of the page in the cluster.
///
/// In Postgres `BufferTag` structure is used for exactly the same purpose.
/// [See more related comments here](https://github.com/postgres/postgres/blob/99c5852e20a0987eca1c38ba0c09329d4076b6a0/src/include/storage/buf_internals.h#L91).
///
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize)]
pub struct BufferTag {
    pub rel: RelTag,
    pub blknum: u32,
}

///
/// WAL Redo Manager is responsible for replaying WAL records.
///
/// Callers use the WAL redo manager through this abstract interface,
/// which makes it easy to mock it in tests.
pub trait WalRedoManager: Send + Sync {
    /// Apply some WAL records.
    ///
    /// The caller passes an old page image, and WAL records that should be
    /// applied over it. The return value is a new page image, after applying
    /// the reords.
    fn request_redo(
        &self,
        rel: RelishTag,
        blknum: u32,
        lsn: Lsn,
        base_img: Option<Bytes>,
        records: Vec<(Lsn, WALRecord)>,
    ) -> Result<Bytes, WalRedoError>;
}

///
/// A dummy WAL Redo Manager implementation that doesn't allow replaying
/// anything. Currently used during bootstrapping (zenith init), to create
/// a Repository object without launching the real WAL redo process.
///
pub struct DummyRedoManager {}
impl crate::walredo::WalRedoManager for DummyRedoManager {
    fn request_redo(
        &self,
        _rel: RelishTag,
        _blknum: u32,
        _lsn: Lsn,
        _base_img: Option<Bytes>,
        _records: Vec<(Lsn, WALRecord)>,
    ) -> Result<Bytes, WalRedoError> {
        Err(WalRedoError::InvalidState)
    }
}

static TIMEOUT: Duration = Duration::from_secs(20);

// Metrics collected on WAL redo operations
//
// We collect the time spent in actual WAL redo ('redo'), and time waiting
// for access to the postgres process ('wait') since there is only one for
// each tenant.
lazy_static! {
    static ref WAL_REDO_TIME: Histogram =
        register_histogram!("pageserver_wal_redo_time", "Time spent on WAL redo")
            .expect("failed to define a metric");
    static ref WAL_REDO_WAIT_TIME: Histogram = register_histogram!(
        "pageserver_wal_redo_wait_time",
        "Time spent waiting for access to the WAL redo process"
    )
    .expect("failed to define a metric");
    static ref WAL_REDO_RECORD_COUNTER: IntCounter = register_int_counter!(
        "pageserver_wal_records_replayed",
        "Number of WAL records replayed"
    )
    .unwrap();
}

///
/// This is the real implementation that uses a Postgres process to
/// perform WAL replay. Only one thread can use the processs at a time,
/// that is controlled by the Mutex. In the future, we might want to
/// launch a pool of processes to allow concurrent replay of multiple
/// records.
///
pub struct PostgresRedoManager {
    tenantid: ZTenantId,
    conf: &'static PageServerConf,

    process: Mutex<Option<PostgresRedoProcess>>,
}

#[derive(Debug)]
struct WalRedoRequest {
    rel: RelishTag,
    blknum: u32,
    lsn: Lsn,

    base_img: Option<Bytes>,
    records: Vec<(Lsn, WALRecord)>,
}

impl WalRedoRequest {
    // Can this request be served by zenith redo funcitons
    // or we need to pass it to wal-redo postgres process?
    fn can_apply_in_zenith(&self) -> bool {
        !matches!(self.rel, RelishTag::Relation(_))
    }
}
/// An error happened in WAL redo
#[derive(Debug, thiserror::Error)]
pub enum WalRedoError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("cannot perform WAL redo now")]
    InvalidState,
    #[error("cannot perform WAL redo for this request")]
    InvalidRequest,
}

///
/// Public interface of WAL redo manager
///
impl WalRedoManager for PostgresRedoManager {
    ///
    /// Request the WAL redo manager to apply some WAL records
    ///
    /// The WAL redo is handled by a separate thread, so this just sends a request
    /// to the thread and waits for response.
    ///
    fn request_redo(
        &self,
        rel: RelishTag,
        blknum: u32,
        lsn: Lsn,
        base_img: Option<Bytes>,
        records: Vec<(Lsn, WALRecord)>,
    ) -> Result<Bytes, WalRedoError> {
        let start_time;
        let end_time;

        let request = WalRedoRequest {
            rel,
            blknum,
            lsn,
            base_img,
            records,
        };

        start_time = Instant::now();
        let result;

        if request.can_apply_in_zenith() {
            result = self.handle_apply_request_zenith(&request);

            end_time = Instant::now();
            WAL_REDO_TIME.observe(end_time.duration_since(start_time).as_secs_f64());
        } else {
            let mut process_guard = self.process.lock().unwrap();
            let lock_time = Instant::now();

            // launch the WAL redo process on first use
            if process_guard.is_none() {
                let p = PostgresRedoProcess::launch(self.conf, &self.tenantid)?;
                *process_guard = Some(p);
            }
            let process = process_guard.as_mut().unwrap();

            result = self.handle_apply_request_postgres(process, &request);

            WAL_REDO_WAIT_TIME.observe(lock_time.duration_since(start_time).as_secs_f64());
            end_time = Instant::now();
            WAL_REDO_TIME.observe(end_time.duration_since(lock_time).as_secs_f64());

            // If something went wrong, don't try to reuse the process. Kill it, and
            // next request will launch a new one.
            if result.is_err() {
                let process = process_guard.take().unwrap();
                process.kill();
            }
        }

        result
    }
}

impl PostgresRedoManager {
    ///
    /// Create a new PostgresRedoManager.
    ///
    pub fn new(conf: &'static PageServerConf, tenantid: ZTenantId) -> PostgresRedoManager {
        // The actual process is launched lazily, on first request.
        PostgresRedoManager {
            tenantid,
            conf,
            process: Mutex::new(None),
        }
    }

    ///
    /// Process one request for WAL redo using wal-redo postgres
    ///
    fn handle_apply_request_postgres(
        &self,
        process: &mut PostgresRedoProcess,
        request: &WalRedoRequest,
    ) -> Result<Bytes, WalRedoError> {
        let blknum = request.blknum;
        let lsn = request.lsn;
        let base_img = request.base_img.clone();
        let records = &request.records;
        let nrecords = records.len();

        let start = Instant::now();

        let apply_result: Result<Bytes, Error>;

        if let RelishTag::Relation(rel) = request.rel {
            // Relational WAL records are applied using wal-redo-postgres
            let buf_tag = BufferTag { rel, blknum };
            apply_result = process.apply_wal_records(buf_tag, base_img, records);

            let duration = start.elapsed();

            debug!(
                "postgres applied {} WAL records in {} us to reconstruct page image at LSN {}",
                nrecords,
                duration.as_micros(),
                lsn
            );

            apply_result.map_err(WalRedoError::IoError)
        } else {
            Err(WalRedoError::InvalidRequest)
        }
    }

    ///
    /// Process one request for WAL redo using custom zenith code
    ///
    fn handle_apply_request_zenith(&self, request: &WalRedoRequest) -> Result<Bytes, WalRedoError> {
        let rel = request.rel;
        let blknum = request.blknum;
        let lsn = request.lsn;
        let base_img = request.base_img.clone();
        let records = &request.records;

        let nrecords = records.len();

        let start = Instant::now();

        let apply_result: Result<Bytes, Error>;

        // Non-relational WAL records are handled here, with custom code that has the
        // same effects as the corresponding Postgres WAL redo function.
        const ZERO_PAGE: [u8; 8192] = [0u8; 8192];
        let mut page = BytesMut::new();
        if let Some(fpi) = base_img {
            // If full-page image is provided, then use it...
            page.extend_from_slice(&fpi[..]);
        } else {
            // otherwise initialize page with zeros
            page.extend_from_slice(&ZERO_PAGE);
        }
        // Apply all collected WAL records
        for (_lsn, record) in records {
            let mut buf = Bytes::from(record.rec().to_vec());

            WAL_REDO_RECORD_COUNTER.inc();

            // 1. Parse XLogRecord struct
            // FIXME: refactor to avoid code duplication.
            let xlogrec = XLogRecord::from_bytes(&mut buf);

            //move to main data
            // TODO probably, we should store some records in our special format
            // to avoid this weird parsing on replay
            let skip = (record.main_data_offset() - pg_constants::SIZEOF_XLOGRECORD) as usize;
            if buf.remaining() > skip {
                buf.advance(skip);
            }

            if xlogrec.xl_rmid == pg_constants::RM_XACT_ID {
                // Transaction manager stuff
                let rec_segno = match rel {
                    RelishTag::Slru { slru, segno } => {
                        assert!(
                            slru == SlruKind::Clog,
                            "Not valid XACT relish tag {:?}",
                            rel
                        );
                        segno
                    }
                    _ => panic!("Not valid XACT relish tag {:?}", rel),
                };
                let parsed_xact =
                    XlXactParsedRecord::decode(&mut buf, xlogrec.xl_xid, xlogrec.xl_info);
                if parsed_xact.info == pg_constants::XLOG_XACT_COMMIT
                    || parsed_xact.info == pg_constants::XLOG_XACT_COMMIT_PREPARED
                {
                    transaction_id_set_status(
                        parsed_xact.xid,
                        pg_constants::TRANSACTION_STATUS_COMMITTED,
                        &mut page,
                    );
                    for subxact in &parsed_xact.subxacts {
                        let pageno = *subxact as u32 / pg_constants::CLOG_XACTS_PER_PAGE;
                        let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                        let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                        // only update xids on the requested page
                        if rec_segno == segno && blknum == rpageno {
                            transaction_id_set_status(
                                *subxact,
                                pg_constants::TRANSACTION_STATUS_COMMITTED,
                                &mut page,
                            );
                        }
                    }
                } else if parsed_xact.info == pg_constants::XLOG_XACT_ABORT
                    || parsed_xact.info == pg_constants::XLOG_XACT_ABORT_PREPARED
                {
                    transaction_id_set_status(
                        parsed_xact.xid,
                        pg_constants::TRANSACTION_STATUS_ABORTED,
                        &mut page,
                    );
                    for subxact in &parsed_xact.subxacts {
                        let pageno = *subxact as u32 / pg_constants::CLOG_XACTS_PER_PAGE;
                        let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                        let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                        // only update xids on the requested page
                        if rec_segno == segno && blknum == rpageno {
                            transaction_id_set_status(
                                *subxact,
                                pg_constants::TRANSACTION_STATUS_ABORTED,
                                &mut page,
                            );
                        }
                    }
                }
            } else if xlogrec.xl_rmid == pg_constants::RM_MULTIXACT_ID {
                // Multixact operations
                let info = xlogrec.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
                if info == pg_constants::XLOG_MULTIXACT_CREATE_ID {
                    let xlrec = XlMultiXactCreate::decode(&mut buf);
                    if let RelishTag::Slru {
                        slru,
                        segno: rec_segno,
                    } = rel
                    {
                        if slru == SlruKind::MultiXactMembers {
                            for i in 0..xlrec.nmembers {
                                let pageno = i / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;
                                let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                                let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                                if segno == rec_segno && rpageno == blknum {
                                    // update only target block
                                    let offset = xlrec.moff + i;
                                    let memberoff = mx_offset_to_member_offset(offset);
                                    let flagsoff = mx_offset_to_flags_offset(offset);
                                    let bshift = mx_offset_to_flags_bitshift(offset);
                                    let mut flagsval =
                                        LittleEndian::read_u32(&page[flagsoff..flagsoff + 4]);
                                    flagsval &=
                                        !(((1 << pg_constants::MXACT_MEMBER_BITS_PER_XACT) - 1)
                                            << bshift);
                                    flagsval |= xlrec.members[i as usize].status << bshift;
                                    LittleEndian::write_u32(
                                        &mut page[flagsoff..flagsoff + 4],
                                        flagsval,
                                    );
                                    LittleEndian::write_u32(
                                        &mut page[memberoff..memberoff + 4],
                                        xlrec.members[i as usize].xid,
                                    );
                                }
                            }
                        } else {
                            // Multixact offsets SLRU
                            let offs = (xlrec.mid % pg_constants::MULTIXACT_OFFSETS_PER_PAGE as u32
                                * 4) as usize;
                            LittleEndian::write_u32(&mut page[offs..offs + 4], xlrec.moff);
                        }
                    } else {
                        panic!();
                    }
                } else {
                    panic!();
                }
            }
        }

        apply_result = Ok::<Bytes, Error>(page.freeze());

        let duration = start.elapsed();

        debug!(
            "zenith applied {} WAL records in {} ms to reconstruct page image at LSN {}",
            nrecords,
            duration.as_millis(),
            lsn
        );

        apply_result.map_err(WalRedoError::IoError)
    }
}

///
/// Handle to the Postgres WAL redo process
///
struct PostgresRedoProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    stderr: ChildStderr,
}

impl PostgresRedoProcess {
    //
    // Start postgres binary in special WAL redo mode.
    //
    fn launch(conf: &PageServerConf, tenantid: &ZTenantId) -> Result<PostgresRedoProcess, Error> {
        // FIXME: We need a dummy Postgres cluster to run the process in. Currently, we
        // just create one with constant name. That fails if you try to launch more than
        // one WAL redo manager concurrently.
        let datadir = conf.tenant_path(tenantid).join("wal-redo-datadir");

        // Create empty data directory for wal-redo postgres, deleting old one first.
        if datadir.exists() {
            info!("directory {:?} exists, removing", &datadir);
            if let Err(e) = fs::remove_dir_all(&datadir) {
                error!("could not remove old wal-redo-datadir: {:#}", e);
            }
        }
        info!("running initdb in {:?}", datadir.display());
        let initdb = Command::new(conf.pg_bin_dir().join("initdb"))
            .args(&["-D", datadir.to_str().unwrap()])
            .arg("-N")
            .env_clear()
            .env("LD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
            .env("DYLD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
            .output()
            .expect("failed to execute initdb");

        if !initdb.status.success() {
            panic!(
                "initdb failed: {}\nstderr:\n{}",
                std::str::from_utf8(&initdb.stdout).unwrap(),
                std::str::from_utf8(&initdb.stderr).unwrap()
            );
        } else {
            // Limit shared cache for wal-redo-postres
            let mut config = OpenOptions::new()
                .append(true)
                .open(PathBuf::from(&datadir).join("postgresql.conf"))?;
            config.write_all(b"shared_buffers=128kB\n")?;
            config.write_all(b"fsync=off\n")?;
            config.write_all(b"shared_preload_libraries=zenith\n")?;
            config.write_all(b"zenith.wal_redo=on\n")?;
        }
        // Start postgres itself
        let mut child = Command::new(conf.pg_bin_dir().join("postgres"))
            .arg("--wal-redo")
            .stdin(Stdio::piped())
            .stderr(Stdio::piped())
            .stdout(Stdio::piped())
            .env_clear()
            .env("LD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
            .env("DYLD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
            .env("PGDATA", &datadir)
            .spawn()
            .expect("postgres --wal-redo command failed to start");

        info!(
            "launched WAL redo postgres process on {:?}",
            datadir.display()
        );

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        set_nonblock(stdin.as_raw_fd())?;
        set_nonblock(stdout.as_raw_fd())?;
        set_nonblock(stderr.as_raw_fd())?;

        Ok(PostgresRedoProcess {
            child,
            stdin,
            stdout,
            stderr,
        })
    }

    fn kill(mut self) {
        let _ = self.child.kill();
        if let Ok(exit_status) = self.child.wait() {
            error!("wal-redo-postgres exited with code {}", exit_status);
        }
        drop(self);
    }

    //
    // Apply given WAL records ('records') over an old page image. Returns
    // new page image.
    //
    fn apply_wal_records(
        &mut self,
        tag: BufferTag,
        base_img: Option<Bytes>,
        records: &[(Lsn, WALRecord)],
    ) -> Result<Bytes, std::io::Error> {
        // Serialize all the messages to send the WAL redo process first.
        //
        // This could be problematic if there are millions of records to replay,
        // but in practice the number of records is usually so small that it doesn't
        // matter, and it's better to keep this code simple.
        let mut writebuf: Vec<u8> = Vec::new();
        build_begin_redo_for_block_msg(tag, &mut writebuf);
        if let Some(img) = base_img {
            build_push_page_msg(tag, &img, &mut writebuf);
        }
        for (lsn, rec) in records.iter() {
            build_apply_record_msg(*lsn, rec.rec(), &mut writebuf);
        }
        build_get_page_msg(tag, &mut writebuf);
        WAL_REDO_RECORD_COUNTER.inc_by(records.len() as u64);

        // The input is now in 'writebuf'. Do a blind write first, writing as much as
        // we can, before calling poll(). That skips one call to poll() if the stdin is
        // already available for writing, which it almost certainly is because the
        // process is idle.
        let mut nwrite = self.stdin.write(&writebuf)?;

        // We expect the WAL redo process to respond with an 8k page image. We read it
        // into this buffer.
        let mut resultbuf = vec![0; pg_constants::BLCKSZ.into()];
        let mut nresult: usize = 0; // # of bytes read into 'resultbuf' so far

        // Prepare for calling poll()
        let mut pollfds = [
            PollFd::new(self.stdout.as_raw_fd(), PollFlags::POLLIN),
            PollFd::new(self.stderr.as_raw_fd(), PollFlags::POLLIN),
            PollFd::new(self.stdin.as_raw_fd(), PollFlags::POLLOUT),
        ];

        // We do three things simultaneously: send the old base image and WAL records to
        // the child process's stdin, read the result from child's stdout, and forward any logging
        // information that the child writes to its stderr to the page server's log.
        while nresult < pg_constants::BLCKSZ.into() {
            // If we have more data to write, wake up if 'stdin' becomes writeable or
            // we have data to read. Otherwise only wake up if there's data to read.
            let nfds = if nwrite < writebuf.len() { 3 } else { 2 };
            let n = nix::poll::poll(&mut pollfds[0..nfds], TIMEOUT.as_millis() as i32)?;

            if n == 0 {
                return Err(Error::new(ErrorKind::Other, "WAL redo timed out"));
            }

            // If we have some messages in stderr, forward them to the log.
            let err_revents = pollfds[1].revents().unwrap();
            if err_revents & (PollFlags::POLLERR | PollFlags::POLLIN) != PollFlags::empty() {
                let mut errbuf: [u8; 16384] = [0; 16384];
                let n = self.stderr.read(&mut errbuf)?;

                // The message might not be split correctly into lines here. But this is
                // good enough, the important thing is to get the message to the log.
                if n > 0 {
                    error!(
                        "wal-redo-postgres: {}",
                        String::from_utf8_lossy(&errbuf[0..n])
                    );

                    // To make sure we capture all log from the process if it fails, keep
                    // reading from the stderr, before checking the stdout.
                    continue;
                }
            } else if err_revents.contains(PollFlags::POLLHUP) {
                return Err(Error::new(
                    ErrorKind::BrokenPipe,
                    "WAL redo process closed its stderr unexpectedly",
                ));
            }

            // If we have more data to write and 'stdin' is writeable, do write.
            if nwrite < writebuf.len() {
                let in_revents = pollfds[2].revents().unwrap();
                if in_revents & (PollFlags::POLLERR | PollFlags::POLLOUT) != PollFlags::empty() {
                    nwrite += self.stdin.write(&writebuf[nwrite..])?;
                } else if in_revents.contains(PollFlags::POLLHUP) {
                    // We still have more data to write, but the process closed the pipe.
                    return Err(Error::new(
                        ErrorKind::BrokenPipe,
                        "WAL redo process closed its stdin unexpectedly",
                    ));
                }
            }

            // If we have some data in stdout, read it to the result buffer.
            let out_revents = pollfds[0].revents().unwrap();
            if out_revents & (PollFlags::POLLERR | PollFlags::POLLIN) != PollFlags::empty() {
                nresult += self.stdout.read(&mut resultbuf[nresult..])?;
            } else if out_revents.contains(PollFlags::POLLHUP) {
                return Err(Error::new(
                    ErrorKind::BrokenPipe,
                    "WAL redo process closed its stdout unexpectedly",
                ));
            }
        }

        Ok(Bytes::from(resultbuf))
    }
}

// Functions for constructing messages to send to the postgres WAL redo
// process. See vendor/postgres/src/backend/tcop/zenith_wal_redo.c for
// explanation of the protocol.

fn build_begin_redo_for_block_msg(tag: BufferTag, buf: &mut Vec<u8>) {
    let len = 4 + 1 + 4 * 4;

    buf.put_u8(b'B');
    buf.put_u32(len as u32);

    tag.ser_into(buf)
        .expect("serialize BufferTag should always succeed");
}

fn build_push_page_msg(tag: BufferTag, base_img: &[u8], buf: &mut Vec<u8>) {
    assert!(base_img.len() == 8192);

    let len = 4 + 1 + 4 * 4 + base_img.len();

    buf.put_u8(b'P');
    buf.put_u32(len as u32);
    tag.ser_into(buf)
        .expect("serialize BufferTag should always succeed");
    buf.put(base_img);
}

fn build_apply_record_msg(endlsn: Lsn, rec: &[u8], buf: &mut Vec<u8>) {
    let len = 4 + 8 + rec.len();

    buf.put_u8(b'A');
    buf.put_u32(len as u32);
    buf.put_u64(endlsn.0);
    buf.put(rec);
}

fn build_get_page_msg(tag: BufferTag, buf: &mut Vec<u8>) {
    let len = 4 + 1 + 4 * 4;

    buf.put_u8(b'G');
    buf.put_u32(len as u32);
    tag.ser_into(buf)
        .expect("serialize BufferTag should always succeed");
}
