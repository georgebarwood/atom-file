//! [`AtomicFile`] provides buffered concurrent access to files with async atomic commit.
//!
//! [`BasicAtomicFile`] is a non-async alternative.

#![deny(missing_docs)]

use rustc_hash::FxHashMap as HashMap;
use std::cmp::min;
use std::sync::{Arc, Mutex, RwLock};
use std::cell::RefCell;

#[cfg(feature = "pstd")]
use pstd::collections::BTreeMap;
#[cfg(not(feature = "pstd"))]
use std::collections::BTreeMap;

/// ```Arc<Vec<u8>>```
pub type Data = Arc<Vec<u8>>;

/// Based on [BasicAtomicFile] which makes sure that updates are all-or-nothing.
/// Performs commit asyncronously.
///
/// #Example
///
/// ```
/// use atom_file::{AtomicFile,DummyFile,MemFile,BasicStorage};
/// let mut af = AtomicFile::new(MemFile::new(), DummyFile::new());
/// af.write( 0, &[1,2,3,4] );
/// af.commit(4);
/// af.wait_complete();
/// ```
///
/// Atomic file has two maps of writes. On commit, the latest batch of writes are sent to be written to underlying
/// storage, and are also applied to the second map in the "CommitFile". The CommitFile map is reset when all
/// the updates to underlying storage have been applied.
pub struct AtomicFile {
    /// New updates are written here.
    map: WMap,
    /// Underlying file, with previous updates mapped.
    cf: Arc<RwLock<CommitFile>>,
    /// File size.
    size: u64,
    /// For sending update maps to be saved.
    tx: std::sync::mpsc::Sender<(u64, WMap)>,
    /// Held by update process while it is active.
    busy: Arc<Mutex<()>>,
    /// Limit on size of CommitFile map.
    map_lim: usize,
}

impl AtomicFile {
    /// Construct AtomicFile with default limits. stg is the main underlying storage, upd is temporary storage for updates during commit.
    pub fn new(stg: Box<dyn Storage>, upd: Box<dyn BasicStorage>) -> Box<Self> {
        Self::new_with_limits(stg, upd, &Limits::default())
    }

    /// Construct Atomic file with specified limits.
    pub fn new_with_limits(
        stg: Box<dyn Storage>,
        upd: Box<dyn BasicStorage>,
        lim: &Limits,
    ) -> Box<Self> {
        let size = stg.size();
        let mut baf = BasicAtomicFile::new(stg.clone(), upd, lim);

        let (tx, rx) = std::sync::mpsc::channel::<(u64, WMap)>();
        let cf = Arc::new(RwLock::new(CommitFile::new(stg, lim.rbuf_mem)));
        let busy = Arc::new(Mutex::new(())); // Lock held while async save thread is active.

        // Start the thread which does save asyncronously.
        let (cf1, busy1) = (cf.clone(), busy.clone());

        std::thread::spawn(move || {
            // Loop that recieves a map of updates and applies it to BasicAtomicFile.
            while let Ok((size, map)) = rx.recv() {
                let _lock = busy1.lock();
                baf.map = map;
                baf.commit(size);
                cf1.write().unwrap().done_one();
            }
        });
        Box::new(Self {
            map: WMap::default(),
            cf,
            size,
            tx,
            busy,
            map_lim: lim.map_lim,
        })
    }
}

impl Storage for AtomicFile
{
   fn clone(&self) -> Box<dyn Storage>
   {
       panic!()
   }
}

impl BasicStorage for AtomicFile {
    fn commit(&mut self, size: u64) {
        self.size = size;
        if self.map.is_empty() {
            return;
        }
        if self.cf.read().unwrap().map.len() > self.map_lim {
            self.wait_complete();
        }
        let map = std::mem::take(&mut self.map);
        let cf = &mut *self.cf.write().unwrap();
        cf.todo += 1;
        // Apply map of updates to CommitFile.
        map.to_storage(cf);
        // Send map of updates to thread to be written to underlying storage.
        self.tx.send((size, map)).unwrap();
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn read(&self, start: u64, data: &mut [u8]) {
        self.map.read(start, data, &*self.cf.read().unwrap());
    }

    fn write_data(&mut self, start: u64, data: Data, off: usize, len: usize) {
        self.map.write(start, data, off, len);
    }

    fn write(&mut self, start: u64, data: &[u8]) {
        let len = data.len();
        let d = Arc::new(data.to_vec());
        self.write_data(start, d, 0, len);
    }

    fn wait_complete(&self) {
        while self.cf.read().unwrap().todo != 0 {
            let _x = self.busy.lock();
        }
    }
}

struct CommitFile {
    /// Buffered underlying storage.
    stg: ReadBufStg<256>,
    /// Map of committed updates.
    map: WMap,
    /// Number of outstanding unsaved commits.
    todo: usize,
}

impl CommitFile {
    fn new(stg: Box<dyn Storage>, buf_mem: usize) -> Self {
        Self {
            stg: ReadBufStg::<256>::new(stg, 50, buf_mem / 256),
            map: WMap::default(),
            todo: 0,
        }
    }

    fn done_one(&mut self) {
        self.todo -= 1;
        if self.todo == 0 {
            self.map = WMap::default();
            self.stg.reset();
        }
    }
}

impl BasicStorage for CommitFile {
    fn commit(&mut self, _size: u64) {
        panic!()
    }

    fn size(&self) -> u64 {
        panic!()
    }

    fn read(&self, start: u64, data: &mut [u8]) {
        self.map.read(start, data, &self.stg);
    }

    fn write_data(&mut self, start: u64, data: Data, off: usize, len: usize) {
        self.map.write(start, data, off, len);
    }

    fn write(&mut self, _start: u64, _data: &[u8]) {
        panic!()
    }
}

/// Storage interface - BasicStorage is some kind of "file" storage.
///
/// read and write methods take a start which is a byte offset in the underlying file.
pub trait BasicStorage: Send {
    /// Get the size of the underlying storage.
    /// Note : this is valid initially and after a commit but is not defined after write is called.
    fn size(&self) -> u64;

    /// Read data.
    fn read(&self, start: u64, data: &mut [u8]);

    /// Write byte slice to storage.
    fn write(&mut self, start: u64, data: &[u8]);

    /// Write byte Vec.
    fn write_vec(&mut self, start: u64, data: Vec<u8>) {
        let len = data.len();
        let d = Arc::new(data);
        self.write_data(start, d, 0, len);
    }

    /// Write Data slice.
    fn write_data(&mut self, start: u64, data: Data, off: usize, len: usize) {
        self.write(start, &data[off..off + len]);
    }

    /// Finish write transaction, size is new size of underlying storage.
    fn commit(&mut self, size: u64);

    /// Write u64.
    fn write_u64(&mut self, start: u64, value: u64) {
        self.write(start, &value.to_le_bytes());
    }

    /// Read u64.
    fn read_u64(&self, start: u64) -> u64 {
        let mut bytes = [0; 8];
        self.read(start, &mut bytes);
        u64::from_le_bytes(bytes)
    }

    /// Wait until current writes are complete.
    fn wait_complete(&self) {}
}

/// BasicStorage with Sync and clone.
pub trait Storage: BasicStorage + Sync {
    /// Clone.
    fn clone(&self) -> Box<dyn Storage>;
}

/// Simple implementation of [Storage] using `Arc<Mutex<Vec<u8>>`.
#[derive(Default)]
pub struct MemFile {
    v: Arc<Mutex<Vec<u8>>>,
}

impl MemFile {
    /// Get a new (boxed) MemFile.
    pub fn new() -> Box<Self> {
        Box::default()
    }
}

impl Storage for MemFile {
    fn clone(&self) -> Box<dyn Storage> {
        Box::new(Self { v: self.v.clone() })
    }
}

impl BasicStorage for MemFile {
    fn size(&self) -> u64 {
        let v = self.v.lock().unwrap();
        v.len() as u64
    }

    fn read(&self, off: u64, bytes: &mut [u8]) {
        let off = off as usize;
        let len = bytes.len();
        let mut v = self.v.lock().unwrap();
        if off + len > v.len() {
            v.resize(off + len, 0);
        }
        bytes.copy_from_slice(&v[off..off + len]);
    }

    fn write(&mut self, off: u64, bytes: &[u8]) {
        let off = off as usize;
        let len = bytes.len();
        let mut v = self.v.lock().unwrap();
        if off + len > v.len() {
            v.resize(off + len, 0);
        }
        v[off..off + len].copy_from_slice(bytes);
    }

    fn commit(&mut self, size: u64) {
        let mut v = self.v.lock().unwrap();
        v.resize(size as usize, 0);
    }
}

use std::{fs, fs::OpenOptions, io::Read, io::Seek, io::SeekFrom, io::Write};

struct FileInner {
    f: fs::File,
}

impl FileInner {
    /// Construct from filename.
    pub fn new(filename: &str) -> Self {
        Self {
            f: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(filename)
                .unwrap(),
        }
    }

    fn size(&mut self) -> u64 {
        self.f.seek(SeekFrom::End(0)).unwrap()
    }

    fn read(&mut self, off: u64, bytes: &mut [u8]) {
        self.f.seek(SeekFrom::Start(off)).unwrap();
        let _ = self.f.read(bytes).unwrap();
    }

    fn write(&mut self, off: u64, bytes: &[u8]) {
        // The list of operating systems which auto-zero is likely more than this...research is todo.
        #[cfg(not(any(target_os = "windows", target_os = "linux")))]
        {
            let size = self.f.seek(SeekFrom::End(0)).unwrap();
            if off > size {
                self.f.set_len(off).unwrap();
            }
        }
        self.f.seek(SeekFrom::Start(off)).unwrap();
        let _ = self.f.write(bytes).unwrap();
    }

    fn commit(&mut self, size: u64) {
        self.f.set_len(size).unwrap();
        self.f.sync_all().unwrap();
    }
}

/// Can be used for atomic upd file ( does not implement Sync ).
pub struct FastFileStorage {
    file: RefCell<FileInner>,
}

impl FastFileStorage {
    /// Construct from filename.
    pub fn new(filename: &str) -> Box<Self> {
        Box::new(Self {
            file: RefCell::new(FileInner::new(filename)),
        })
    }
}

impl BasicStorage for FastFileStorage {
    fn size(&self) -> u64 {
        self.file.borrow_mut().size()
    }
    fn read(&self, off: u64, bytes: &mut [u8]) {
        self.file.borrow_mut().read(off, bytes);
    }

    fn write(&mut self, off: u64, bytes: &[u8]) {
        self.file.borrow_mut().write(off, bytes);
    }

    fn commit(&mut self, size: u64) {
        self.file.borrow_mut().commit(size);
    }
}

/// Simple implementation of [Storage] using [`std::fs::File`].
pub struct SimpleFileStorage {
    file: Arc<Mutex<FileInner>>,
}

impl SimpleFileStorage {
    /// Construct from filename.
    pub fn new(filename: &str) -> Box<Self> {
        Box::new(Self {
            file: Arc::new(Mutex::new(FileInner::new(filename))),
        })
    }
}

impl Storage for SimpleFileStorage {
    fn clone(&self) -> Box<dyn Storage> {
        Box::new(Self {
            file: self.file.clone(),
        })
    }
}

impl BasicStorage for SimpleFileStorage {
    fn size(&self) -> u64 {
        self.file.lock().unwrap().size()
    }

    fn read(&self, off: u64, bytes: &mut [u8]) {
        self.file.lock().unwrap().read(off, bytes);
    }

    fn write(&mut self, off: u64, bytes: &[u8]) {
        self.file.lock().unwrap().write(off, bytes);
    }

    fn commit(&mut self, size: u64) {
        self.file.lock().unwrap().commit(size);
    }
}

/// Alternative to SimpleFileStorage that uses multiple [SimpleFileStorage]s to allow parallel reads by different threads.
pub struct AnyFileStorage {
    filename: String,
    files: Arc<Mutex<Vec<FileInner>>>,
}

impl AnyFileStorage {
    /// Create new.
    pub fn new(filename: &str) -> Box<Self> {
        Box::new(Self {
            filename: filename.to_owned(),
            files: Arc::new(Mutex::new(Vec::new())),
        })
    }

    fn get_file(&self) -> FileInner {
        match self.files.lock().unwrap().pop() {
            Some(f) => f,
            _ => FileInner::new(&self.filename),
        }
    }

    fn put_file(&self, f: FileInner) {
        self.files.lock().unwrap().push(f);
    }
}

impl Storage for AnyFileStorage {
     fn clone(&self) -> Box<dyn Storage> {
        Box::new(Self {
            filename: self.filename.clone(),
            files: self.files.clone(),
        })
    }
}

impl BasicStorage for AnyFileStorage {
    fn size(&self) -> u64 {
        let mut f = self.get_file();
        let result = f.size();
        self.put_file(f);
        result
    }

    fn read(&self, off: u64, bytes: &mut [u8]) {
        let mut f = self.get_file();
        f.read(off, bytes);
        self.put_file(f);
    }

    fn write(&mut self, off: u64, bytes: &[u8]) {
        let mut f = self.get_file();
        f.write(off, bytes);
        self.put_file(f);
    }

    fn commit(&mut self, size: u64) {
        let mut f = self.get_file();
        f.commit(size);
        self.put_file(f);
    }
}

/// Dummy Stg that can be used for Atomic upd file if "reliable" atomic commits are not required.
pub struct DummyFile {}
impl DummyFile {
    /// Construct.
    pub fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

impl Storage for DummyFile {
    fn clone(&self) -> Box<dyn Storage> {
        Self::new()
    }
}

impl BasicStorage for DummyFile {
    fn size(&self) -> u64 {
        0
    }

    fn read(&self, _off: u64, _bytes: &mut [u8]) {}

    fn write(&mut self, _off: u64, _bytes: &[u8]) {}

    fn commit(&mut self, _size: u64) {}
}

/// Memory configuration limits.
#[non_exhaustive]
pub struct Limits {
    /// Limit on size of commit write map, default is 5000.
    pub map_lim: usize,
    /// Memory for buffering small reads, default is 0x200000 ( 2MB ).
    pub rbuf_mem: usize,
    /// Memory for buffering writes to main storage, default is 0x100000 (1MB).
    pub swbuf: usize,
    /// Memory for buffering writes to temporary storage, default is 0x100000 (1MB).
    pub uwbuf: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            map_lim: 5000,
            rbuf_mem: 0x200000,
            swbuf: 0x100000,
            uwbuf: 0x100000,
        }
    }
}

/// Write Buffer.
struct WriteBuffer {
    /// Current write index into buf.
    ix: usize,
    /// Current file position.
    pos: u64,
    /// Underlying storage.
    pub stg: Box<dyn BasicStorage>,
    /// Buffer.
    buf: Vec<u8>,
}

impl WriteBuffer {
    /// Construct.
    pub fn new(stg: Box<dyn BasicStorage>, buf_size: usize) -> Self {
        Self {
            ix: 0,
            pos: u64::MAX,
            stg,
            buf: vec![0; buf_size],
        }
    }

    /// Write data to specified offset,
    pub fn write(&mut self, off: u64, data: &[u8]) {
        if self.pos + self.ix as u64 != off {
            self.flush(off);
        }
        let mut done: usize = 0;
        let mut todo: usize = data.len();
        while todo > 0 {
            let mut n: usize = self.buf.len() - self.ix;
            if n == 0 {
                self.flush(off + done as u64);
                n = self.buf.len();
            }
            if n > todo {
                n = todo;
            }
            self.buf[self.ix..self.ix + n].copy_from_slice(&data[done..done + n]);
            todo -= n;
            done += n;
            self.ix += n;
        }
    }

    fn flush(&mut self, new_pos: u64) {
        if self.ix > 0 {
            self.stg.write(self.pos, &self.buf[0..self.ix]);
        }
        self.ix = 0;
        self.pos = new_pos;
    }

    /// Commit.
    pub fn commit(&mut self, size: u64) {
        self.flush(u64::MAX);
        self.stg.commit(size);
    }

    /// Write u64.
    pub fn write_u64(&mut self, start: u64, value: u64) {
        self.write(start, &value.to_le_bytes());
    }
}

/// ReadBufStg buffers small (up to limit) reads to the underlying storage using multiple buffers. Only supported functions are read and reset.
///
/// See implementation of AtomicFile for how this is used in conjunction with WMap.
///
/// N is buffer size.
struct ReadBufStg<const N: usize> {
    /// Underlying storage.
    stg: Box<dyn Storage>,
    /// Buffers.
    buf: Mutex<ReadBuffer<N>>,
    /// Read size that is considered small.
    limit: usize,
}

impl<const N: usize> Drop for ReadBufStg<N> {
    fn drop(&mut self) {
        self.reset();
    }
}

impl<const N: usize> ReadBufStg<N> {
    /// limit is the size of a read that is considered "small", max_buf is the maximum number of buffers used.
    pub fn new(stg: Box<dyn Storage>, limit: usize, max_buf: usize) -> Self {
        Self {
            stg,
            buf: Mutex::new(ReadBuffer::<N>::new(max_buf)),
            limit,
        }
    }

    /// Clears the buffers.
    fn reset(&mut self) {
        self.buf.lock().unwrap().reset();
    }
}

impl<const N: usize> BasicStorage for ReadBufStg<N> {
    /// Read data from storage.
    fn read(&self, start: u64, data: &mut [u8]) {
        if data.len() <= self.limit {
            self.buf.lock().unwrap().read(&*self.stg, start, data);
        } else {
            self.stg.read(start, data);
        }
    }

    /// Panics.
    fn size(&self) -> u64 {
        panic!()
    }

    /// Panics.
    fn write(&mut self, _start: u64, _data: &[u8]) {
        panic!();
    }

    /// Panics.
    fn commit(&mut self, _size: u64) {
        panic!();
    }
}

struct ReadBuffer<const N: usize> {
    /// Maps sector mumbers cached buffers.
    map: HashMap<u64, Box<[u8; N]>>,
    /// Maximum number of buffers.
    max_buf: usize,
}

impl<const N: usize> ReadBuffer<N> {
    fn new(max_buf: usize) -> Self {
        Self {
            map: HashMap::default(),
            max_buf,
        }
    }

    fn reset(&mut self) {
        self.map.clear();
    }

    fn read(&mut self, stg: &dyn BasicStorage, off: u64, data: &mut [u8]) {
        let mut done = 0;
        while done < data.len() {
            let off = off + done as u64;
            let sector = off / N as u64;
            let disp = (off % N as u64) as usize;
            let amount = min(data.len() - done, N - disp);

            let p = self.map.entry(sector).or_insert_with(|| {
                let mut p: Box<[u8; N]> = vec![0; N].try_into().unwrap();
                stg.read(sector * N as u64, &mut *p);
                p
            });
            data[done..done + amount].copy_from_slice(&p[disp..disp + amount]);
            done += amount;
        }
        if self.map.len() >= self.max_buf {
            self.reset();
        }
    }
}

#[derive(Default)]
/// Slice of Data to be written to storage.
struct DataSlice {
    /// Slice data.
    pub data: Data,
    /// Start of slice.
    pub off: usize,
    /// Length of slice.
    pub len: usize,
}

impl DataSlice {
    /// Get reference to the whole slice.
    pub fn all(&self) -> &[u8] {
        &self.data[self.off..self.off + self.len]
    }
    /// Get reference to part of slice.
    pub fn part(&self, off: usize, len: usize) -> &[u8] {
        &self.data[self.off + off..self.off + off + len]
    }
    /// Trim specified amount from start of slice.
    pub fn trim(&mut self, trim: usize) {
        self.off += trim;
        self.len -= trim;
    }
    /// Take the data.
    #[allow(dead_code)]
    pub fn take(&mut self) -> Data {
        std::mem::take(&mut self.data)
    }
}

#[derive(Default)]
/// Updateable store based on some underlying storage.
struct WMap {
    /// Map of writes. Key is the end of the slice.
    map: BTreeMap<u64, DataSlice>,
}

impl WMap {
    /// Is the map empty?
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Number of key-value pairs in the map.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Take the map and convert it to a Vec.
    pub fn convert_to_vec(&mut self) -> Vec<(u64, DataSlice)> {
        let map = std::mem::take(&mut self.map);
        let mut result = Vec::with_capacity(map.len());
        for (end, v) in map {
            let start = end - v.len as u64;
            result.push((start, v));
        }
        result
    }

    /// Write the map into storage.
    pub fn to_storage(&self, stg: &mut dyn BasicStorage) {
        for (end, v) in self.map.iter() {
            let start = end - v.len as u64;
            stg.write_data(start, v.data.clone(), v.off, v.len);
        }
    }

    #[cfg(not(feature = "pstd"))]
    /// Write to storage, existing writes which overlap with new write need to be trimmed or removed.
    pub fn write(&mut self, start: u64, data: Data, off: usize, len: usize) {
        if len != 0 {
            let (mut insert, mut remove) = (Vec::new(), Vec::new());
            let end = start + len as u64;
            for (ee, v) in self.map.range_mut(start + 1..) {
                let ee = *ee;
                let es = ee - v.len as u64; // Existing write Start.
                if es >= end {
                    // Existing write starts after end of new write, nothing to do.
                    break;
                } else if start <= es {
                    if end < ee {
                        // New write starts before existing write, but doesn't subsume it. Trim existing write.
                        v.trim((end - es) as usize);
                        break;
                    }
                    // New write subsumes existing write entirely, remove existing write.
                    remove.push(ee);
                } else if end < ee {
                    // New write starts in middle of existing write, ends before end of existing write,
                    // put start of existing write in insert list, trim existing write.
                    insert.push((es, v.data.clone(), v.off, (start - es) as usize));
                    v.trim((end - es) as usize);
                    break;
                } else {
                    // New write starts in middle of existing write, ends after existing write,
                    // put start of existing write in insert list, remove existing write.
                    insert.push((es, v.take(), v.off, (start - es) as usize));
                    remove.push(ee);
                }
            }
            for end in remove {
                self.map.remove(&end);
            }
            for (start, data, off, len) in insert {
                self.map
                    .insert(start + len as u64, DataSlice { data, off, len });
            }
            self.map
                .insert(start + len as u64, DataSlice { data, off, len });
        }
    }

    #[cfg(feature = "pstd")]
    /// Write to storage, existing writes which overlap with new write need to be trimmed or removed.
    pub fn write(&mut self, start: u64, data: Data, off: usize, len: usize) {
        if len != 0 {
            let end = start + len as u64;
            let mut c = self
                .map
                .lower_bound_mut(std::ops::Bound::Excluded(&start))
                .with_mutable_key();
            while let Some((eend, v)) = c.next() {
                let ee = *eend;
                let es = ee - v.len as u64; // Existing write Start.
                if es >= end {
                    // Existing write starts after end of new write, nothing to do.
                    c.prev();
                    break;
                } else if start <= es {
                    if end < ee {
                        // New write starts before existing write, but doesn't subsume it. Trim existing write.
                        v.trim((end - es) as usize);
                        c.prev();
                        break;
                    }
                    // New write subsumes existing write entirely, remove existing write.
                    c.remove_prev();
                } else if end < ee {
                    // New write starts in middle of existing write, ends before end of existing write,
                    // trim existing write, insert start of existing write.
                    let (data, off, len) = (v.data.clone(), v.off, (start - es) as usize);
                    v.trim((end - es) as usize);
                    c.prev();
                    c.insert_before_unchecked(es + len as u64, DataSlice { data, off, len });
                    break;
                } else {
                    // New write starts in middle of existing write, ends after existing write,
                    // Trim existing write ( modifies key, but this is ok as ordering is not affected ).
                    v.len = (start - es) as usize;
                    *eend = es + v.len as u64;
                }
            }
            // Insert the new write.
            c.insert_after_unchecked(start + len as u64, DataSlice { data, off, len });
        }
    }

    /// Read from storage, taking map of existing writes into account. Unwritten ranges are read from underlying storage.
    pub fn read(&self, start: u64, data: &mut [u8], u: &dyn BasicStorage) {
        let len = data.len();
        if len != 0 {
            let mut done = 0;
            for (&end, v) in self.map.range(start + 1..) {
                let es = end - v.len as u64; // Existing write Start.
                let doff = start + done as u64;
                if es > doff {
                    // Read from underlying storage.
                    let a = min(len - done, (es - doff) as usize);
                    u.read(doff, &mut data[done..done + a]);
                    done += a;
                    if done == len {
                        return;
                    }
                }
                // Use existing write.
                let skip = (start + done as u64 - es) as usize;
                let a = min(len - done, v.len - skip);
                data[done..done + a].copy_from_slice(v.part(skip, a));
                done += a;
                if done == len {
                    return;
                }
            }
            u.read(start + done as u64, &mut data[done..]);
        }
    }
}

/// Basis for [crate::AtomicFile] ( non-async alternative ). Provides two-phase commit and buffering of writes.
pub struct BasicAtomicFile {
    /// The main underlying storage.
    stg: WriteBuffer,
    /// Temporary storage for updates during commit.
    upd: WriteBuffer,
    /// Map of writes.
    map: WMap,
    /// List of writes.
    list: Vec<(u64, DataSlice)>,
    size: u64,
}

impl BasicAtomicFile {
    /// stg is the main underlying storage, upd is temporary storage for updates during commit.
    pub fn new(stg: Box<dyn BasicStorage>, upd: Box<dyn BasicStorage>, lim: &Limits) -> Box<Self> {
        let size = stg.size();
        let mut result = Box::new(Self {
            stg: WriteBuffer::new(stg, lim.swbuf),
            upd: WriteBuffer::new(upd, lim.uwbuf),
            map: WMap::default(),
            list: Vec::new(),
            size,
        });
        result.init();
        result
    }

    /// Apply outstanding updates.
    fn init(&mut self) {
        let end = self.upd.stg.read_u64(0);
        let size = self.upd.stg.read_u64(8);
        if end == 0 {
            return;
        }
        assert!(end == self.upd.stg.size());
        let mut pos = 16;
        while pos < end {
            let start = self.upd.stg.read_u64(pos);
            pos += 8;
            let len = self.upd.stg.read_u64(pos);
            pos += 8;
            let mut buf = vec![0; len as usize];
            self.upd.stg.read(pos, &mut buf);
            pos += len;
            self.stg.write(start, &buf);
        }
        self.stg.commit(size);
        self.upd.commit(0);
    }

    /// Perform the specified phase ( 1 or 2 ) of a two-phase commit.
    pub fn commit_phase(&mut self, size: u64, phase: u8) {
        if self.map.is_empty() && self.list.is_empty() {
            return;
        }
        if phase == 1 {
            self.list = self.map.convert_to_vec();

            // Write the updates to upd.
            // First set the end position to zero.
            self.upd.write_u64(0, 0);
            self.upd.write_u64(8, size);
            self.upd.commit(16); // Not clear if this is necessary.

            // Write the update records.
            let mut stg_written = false;
            let mut pos: u64 = 16;
            for (start, v) in self.list.iter() {
                let (start, len, data) = (*start, v.len as u64, v.all());
                if start >= self.size {
                    // Writes beyond current stg size can be written directly.
                    stg_written = true;
                    self.stg.write(start, data);
                } else {
                    self.upd.write_u64(pos, start);
                    pos += 8;
                    self.upd.write_u64(pos, len);
                    pos += 8;
                    self.upd.write(pos, data);
                    pos += len;
                }
            }
            if stg_written {
                self.stg.commit(size);
            }
            self.upd.commit(pos); // Not clear if this is necessary.

            // Set the end position.
            self.upd.write_u64(0, pos);
            self.upd.write_u64(8, size);
            self.upd.commit(pos);
        } else {
            for (start, v) in self.list.iter() {
                if *start < self.size {
                    // Writes beyond current stg size have already been written.
                    self.stg.write(*start, v.all());
                }
            }
            self.list.clear();
            self.stg.commit(size);
            self.upd.commit(0);
        }
    }
}

impl BasicStorage for BasicAtomicFile {
    fn commit(&mut self, size: u64) {
        self.commit_phase(size, 1);
        self.commit_phase(size, 2);
        self.size = size;
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn read(&self, start: u64, data: &mut [u8]) {
        self.map.read(start, data, &*self.stg.stg);
    }

    fn write_data(&mut self, start: u64, data: Data, off: usize, len: usize) {
        self.map.write(start, data, off, len);
    }

    fn write(&mut self, start: u64, data: &[u8]) {
        let len = data.len();
        let d = Arc::new(data.to_vec());
        self.write_data(start, d, 0, len);
    }
}
    
/// Optimized implementation of [Storage] ( unix only ). 
#[cfg(target_family = "unix")]
pub struct UnixFileStorage
{
    size: Arc<Mutex<u64>>,
    f: fs::File,
}
#[cfg(target_family = "unix")]
impl UnixFileStorage
{
    /// Construct from filename.
    pub fn new(filename: &str) -> Box<Self>
    {
        let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(filename)
                .unwrap();
        let size = f.seek(SeekFrom::End(0)).unwrap();
        let size = Arc::new(Mutex::new(size));
        Box::new(Self{ size, f })
    }
}

#[cfg(target_family = "unix")]
impl Storage for UnixFileStorage {
    fn clone(&self) -> Box<dyn Storage> {
        Box::new(Self {
            size: self.size.clone(),
            f: self.f.try_clone().unwrap()
        })
    }
}

#[cfg(target_family = "unix")]
use std::os::unix::fs::FileExt;

#[cfg(target_family = "unix")]
impl BasicStorage for UnixFileStorage
{
    fn read(&self, start: u64, data: &mut [u8])
    {
        
        let _ = self.f.read_at(data, start );
    }

    fn write(&mut self, start: u64, data: &[u8])
    {
        
        let _ = self.f.write_at(data, start );
    }

    fn size(&self) -> u64 {
        *self.size.lock().unwrap()
    }

    fn commit(&mut self, size: u64)
    {
        *self.size.lock().unwrap() = size;
        self.f.set_len(size).unwrap();
        self.f.sync_all().unwrap();
    }
}

/// Optimized implementation of [Storage] ( windows only ). 
#[cfg(target_family = "windows")]
pub struct WindowsFileStorage
{
    size: Arc<Mutex<u64>>,
    f: fs::File,
}
#[cfg(target_family = "windows")]
impl WindowsFileStorage
{
    /// Construct from filename.
    pub fn new(filename: &str) -> Box<Self>
    {
        let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(filename)
                .unwrap();
        let size = f.seek(SeekFrom::End(0)).unwrap();
        let size = Arc::new(Mutex::new(size));
        Box::new(Self{ size, f })
    }
}

#[cfg(target_family = "windows")]
impl Storage for WindowsFileStorage {
    fn clone(&self) -> Box<dyn Storage> {
        Box::new(Self {
            size: self.size.clone(),
            f: self.f.try_clone().unwrap()
        })
    }
}

#[cfg(target_family = "windows")]
use std::os::windows::fs::FileExt;

#[cfg(target_family = "windows")]
impl BasicStorage for WindowsFileStorage
{
    fn read(&self, start: u64, data: &mut [u8])
    {
        
        let _ = self.f.seek_read(data, start );
    }

    fn write(&mut self, start: u64, data: &[u8])
    {
        
        let _ = self.f.seek_write(data, start );
    }

    fn size(&self) -> u64 {
        *self.size.lock().unwrap()
    }

    fn commit(&mut self, size: u64)
    {
        *self.size.lock().unwrap() = size;
        self.f.set_len(size).unwrap();
        self.f.sync_all().unwrap();
    }
}

/// Optimised Storage
#[cfg(target_family = "windows")]
pub type MultiFileStorage = WindowsFileStorage;

/// Optimised Storage
#[cfg(target_family = "unix")]
pub type MultiFileStorage = UnixFileStorage;

/// Optimised Storage
#[cfg(not(any(target_family = "unix",target_family = "windows")))]
pub type MultiFileStorage = AnyFileStorage;

#[cfg(test)]
/// Get amount of testing from environment variable TA.
fn test_amount() -> usize {
    str::parse(&std::env::var("TA").unwrap_or("1".to_string())).unwrap()
}

#[test]
fn test_atomic_file() {
    use rand::Rng;
    /* Idea of test is to check AtomicFile and MemFile behave the same */

    let ta = test_amount();
    println!(" Test amount={}", ta);

    let mut rng = rand::thread_rng();

    for _ in 0..100 {
        let mut s1 = AtomicFile::new(MemFile::new(), MemFile::new());
        // let mut s1 = BasicAtomicFile::new(MemFile::new(), MemFile::new(), &Limits::default() );
        let mut s2 = MemFile::new();

        for _ in 0..1000 * ta {
            let off: usize = rng.r#gen::<usize>() % 100;
            let mut len = 1 + rng.r#gen::<usize>() % 20;
            let w: bool = rng.r#gen();
            if w {
                let mut bytes = Vec::new();
                while len > 0 {
                    len -= 1;
                    let b: u8 = rng.r#gen::<u8>();
                    bytes.push(b);
                }
                s1.write(off as u64, &bytes);
                s2.write(off as u64, &bytes);
            } else {
                let mut b2 = vec![0; len];
                let mut b3 = vec![0; len];
                s1.read(off as u64, &mut b2);
                s2.read(off as u64, &mut b3);
                assert!(b2 == b3);
            }
            if rng.r#gen::<usize>() % 50 == 0 {
                s1.commit(200);
                s2.commit(200);
            }
        }
    }
}
