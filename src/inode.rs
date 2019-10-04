extern crate libc;
extern crate fuse;
extern crate time;

use std::convert::TryInto;

#[path="block_mgr.rs"]
pub mod block_mgr;
pub use block_mgr::block_io;

use block_io::{Id, BLOCK_SIZE};
use block_mgr::BlockMgr;

struct InodeBody {
    dirty: bool,
    data: [u8; BLOCK_SIZE],
}

pub struct Inode {
    id: Id,
    body: std::cell::RefCell<InodeBody>,
}

const GENERATION_OFF: usize = 0;
const GENERATION_SIZE: usize = std::mem::size_of::<u64>();

const LENGTH_OFF: usize = GENERATION_OFF + GENERATION_SIZE;
const LENGTH_SIZE: usize = std::mem::size_of::<u32>();

const ATIME_OFF: usize = LENGTH_OFF + LENGTH_SIZE;
const ATIME_SIZE: usize = std::mem::size_of::<i64>() + std::mem::size_of::<i32>(); // sec + nsec

const MTIME_OFF: usize = ATIME_OFF + ATIME_SIZE;
const MTIME_SIZE: usize = std::mem::size_of::<i64>() + std::mem::size_of::<i32>(); // sec + nsec

const CTIME_OFF: usize = MTIME_OFF + MTIME_SIZE;
const CTIME_SIZE: usize = std::mem::size_of::<i64>() + std::mem::size_of::<i32>(); // sec + nsec

const MODE_OFF: usize = CTIME_OFF + CTIME_SIZE;
const MODE_SIZE: usize = std::mem::size_of::<u16>();

const NLINK_OFF: usize = MODE_OFF + MODE_SIZE;
const NLINK_SIZE: usize = std::mem::size_of::<u16>();

const UID_OFF: usize = NLINK_OFF + NLINK_SIZE;
const UID_SIZE: usize = std::mem::size_of::<u32>();

const GID_OFF: usize = UID_OFF + UID_SIZE;
const GID_SIZE: usize = std::mem::size_of::<u32>();

const INDEX_OFF: usize = GID_OFF + GID_SIZE;
const INDEX_SIZE: usize = std::mem::size_of::<Id>();

/// For layout of each inode is like:
/// [ generation (8B) | length (4B) | last access time (12B) | last modification time (12B) |
///   last change time (12B) | type + perm (2B) | link count (2B) | uid (4B) | gid (4B) |
///   block0 (Id) | block1 (Id) | ... ]
impl Inode {

    pub fn new(block_mgr: &mut BlockMgr, id: Id) -> Result<Inode, std::io::Error> {
        Ok(Inode { id: id, body: std::cell::RefCell::new(InodeBody { dirty: false, data: block_mgr.read_block(id)? }) })
    }

    pub fn id(&self) -> Id {
        self.id
    }

    pub fn flush(&self, block_mgr: &mut BlockMgr) -> Result<(), std::io::Error> {
        let mut body = self.body.borrow_mut();
        if body.dirty {
            block_mgr.write_block(self.id, &body.data)?;
            body.dirty = false;
        }
        Ok(())
    }

    pub fn generation(&self) -> u64 {
        let body = self.body.borrow();
        u64::from_le_bytes(body.data[GENERATION_OFF .. GENERATION_OFF + GENERATION_SIZE].try_into().unwrap())
    }

    // No need to set geneartion

    pub fn length(&self) -> u32 {
        let body = self.body.borrow();
        u32::from_le_bytes(body.data[LENGTH_OFF .. LENGTH_OFF + LENGTH_SIZE].try_into().unwrap())
    }

    pub fn set_length(&self, length: u32) {
        let mut body = self.body.borrow_mut();
        body.data[LENGTH_OFF .. LENGTH_OFF + LENGTH_SIZE].copy_from_slice(&length.to_le_bytes());
        body.dirty = true;
    }

    pub fn atime(&self) -> time::Timespec {
        let body = self.body.borrow();
        let sec = i64::from_le_bytes(body.data[ATIME_OFF .. ATIME_OFF + 8].try_into().unwrap());
        let nsec = i32::from_le_bytes(body.data[ATIME_OFF + 8 .. ATIME_OFF + 12].try_into().unwrap());
        time::Timespec { sec: sec, nsec: nsec }
    }

    pub fn set_atime(&self, atime: time::Timespec) {
        let mut body = self.body.borrow_mut();
        body.data[ATIME_OFF .. ATIME_OFF + 8].copy_from_slice(&atime.sec.to_le_bytes());
        body.data[ATIME_OFF + 8 .. ATIME_OFF + 12].copy_from_slice(&atime.nsec.to_le_bytes());
        body.dirty = true;
    }

    pub fn mtime(&self) -> time::Timespec {
        let body = self.body.borrow();
        let sec = i64::from_le_bytes(body.data[MTIME_OFF .. MTIME_OFF + 8].try_into().unwrap());
        let nsec = i32::from_le_bytes(body.data[MTIME_OFF + 8 .. MTIME_OFF + 12].try_into().unwrap());
        time::Timespec { sec: sec, nsec: nsec }
    }

    pub fn set_mtime(&self, mtime: time::Timespec) {
        let mut body = self.body.borrow_mut();
        body.data[MTIME_OFF .. MTIME_OFF + 8].copy_from_slice(&mtime.sec.to_le_bytes());
        body.data[MTIME_OFF + 8 .. MTIME_OFF + 12].copy_from_slice(&mtime.nsec.to_le_bytes());
        body.dirty = true;
    }

    pub fn ctime(&self) -> time::Timespec {
        let body = self.body.borrow();
        let sec = i64::from_le_bytes(body.data[CTIME_OFF .. CTIME_OFF + 8].try_into().unwrap());
        let nsec = i32::from_le_bytes(body.data[CTIME_OFF + 8 .. CTIME_OFF + 12].try_into().unwrap());
        time::Timespec { sec: sec, nsec: nsec }
    }

    pub fn set_ctime(&self, ctime: time::Timespec) {
        let mut body = self.body.borrow_mut();
        body.data[CTIME_OFF .. CTIME_OFF + 8].copy_from_slice(&ctime.sec.to_le_bytes());
        body.data[CTIME_OFF + 8 .. CTIME_OFF + 12].copy_from_slice(&ctime.nsec.to_le_bytes());
        body.dirty = true;
    }

    pub fn kind(&self) -> Result<fuse::FileType, std::io::Error> {
        let body = self.body.borrow();
        let mode = u16::from_le_bytes(body.data[MODE_OFF .. MODE_OFF + MODE_SIZE].try_into().unwrap());
        match mode as u32 & libc::S_IFMT {
            libc::S_IFREG => Ok(fuse::FileType::RegularFile),
            libc::S_IFDIR => Ok(fuse::FileType::Directory),
            libc::S_IFLNK => Ok(fuse::FileType::Symlink),
            _ => Err(std::io::Error::from_raw_os_error(libc::EINVAL))
        }
    }

    pub fn perm(&self) -> u16 {
        let body = self.body.borrow();
        let mode = u16::from_le_bytes(body.data[MODE_OFF .. MODE_OFF + MODE_SIZE].try_into().unwrap());
        mode & 0x0fff
    }

    // Set kind and perm together
    pub fn set_mode(&self, mode: u16) {
        let mut body = self.body.borrow_mut();
        body.data[MODE_OFF .. MODE_OFF + MODE_SIZE].copy_from_slice(&mode.to_le_bytes());
        body.dirty = true;
    }

    pub fn nlink(&self) -> u16 {
        let body = self.body.borrow();
        u16::from_le_bytes(body.data[NLINK_OFF .. NLINK_OFF + NLINK_SIZE].try_into().unwrap())
    }

    pub fn set_nlink(&self, nlink: u16) {
        let mut body = self.body.borrow_mut();
        body.data[NLINK_OFF .. NLINK_OFF + NLINK_SIZE].copy_from_slice(&nlink.to_le_bytes());
        body.dirty = true;
    }

    pub fn uid(&self) -> u32 {
        let body = self.body.borrow();
        u32::from_le_bytes(body.data[UID_OFF .. UID_OFF + UID_SIZE].try_into().unwrap())
    }

    pub fn set_uid(&self, uid: u32) {
        let mut body = self.body.borrow_mut();
        body.data[UID_OFF .. UID_OFF + UID_SIZE].copy_from_slice(&uid.to_le_bytes());
        body.dirty = true;
    }

    pub fn gid(&self) -> u32 {
        let body = self.body.borrow();
        u32::from_le_bytes(body.data[GID_OFF .. GID_OFF + GID_SIZE].try_into().unwrap())
    }

    pub fn set_gid(&self, gid: u32) {
        let mut body = self.body.borrow_mut();
        body.data[GID_OFF .. GID_OFF + GID_SIZE].copy_from_slice(&gid.to_le_bytes());
        body.dirty = true;
    }

    pub fn data_block(&self, index: usize) -> Id {
        let body = self.body.borrow();
        Id::from_le_bytes(
            body.data[INDEX_OFF + index * INDEX_SIZE .. INDEX_OFF + (index + 1) * INDEX_SIZE]
            .try_into().unwrap()
        )
    }

    /// Set data block pointer. Need to flush manually later
    pub fn set_data_block(&self, index: usize, data_block: Id) {
        let mut body = self.body.borrow_mut();
        body.data[INDEX_OFF + index * INDEX_SIZE .. INDEX_OFF + (index + 1) * INDEX_SIZE]
            .copy_from_slice(&data_block.to_le_bytes());
        body.dirty = true;
    }
}

impl Drop for Inode {
    fn drop(&mut self) {
        let body = self.body.borrow();
        assert!(!body.dirty); // Everything should be flushed manually
    }
}

