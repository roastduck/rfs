extern crate libc;
extern crate fuse;
extern crate time;

use std::convert::TryInto;

#[path="block_mgr.rs"]
pub mod block_mgr;
pub use block_mgr::block_io;

use block_io::{Id, BLOCK_SIZE};
use block_mgr::BlockMgr;

pub struct Inode {
    id: Id,
    dirty: bool,
    data: [u8; BLOCK_SIZE],
}

const generation_off: usize = 0;
const generation_size: usize = std::mem::size_of::<u64>();

const length_off: usize = generation_off + generation_size;
const length_size: usize = std::mem::size_of::<u32>();

const atime_off: usize = length_off + length_size;
const atime_size: usize = std::mem::size_of::<i64>() + std::mem::size_of::<i32>(); // sec + nsec

const mtime_off: usize = atime_off + atime_size;
const mtime_size: usize = std::mem::size_of::<i64>() + std::mem::size_of::<i32>(); // sec + nsec

const ctime_off: usize = mtime_off + mtime_size;
const ctime_size: usize = std::mem::size_of::<i64>() + std::mem::size_of::<i32>(); // sec + nsec

const type_perm_off: usize = ctime_off + ctime_size;
const type_perm_size: usize = std::mem::size_of::<u16>();

const nlink_off: usize = type_perm_off + type_perm_size;
const nlink_size: usize = std::mem::size_of::<u16>();

const uid_off: usize = nlink_off + nlink_size;
const uid_size: usize = std::mem::size_of::<u32>();

const gid_off: usize = uid_off + uid_size;
const gid_size: usize = std::mem::size_of::<u32>();

const index_off: usize = gid_off + gid_size;
const index_size: usize = std::mem::size_of::<Id>();

/// For layout of each inode is like:
/// [ generation (8B) | length (4B) | last access time (12B) | last modification time (12B) |
///   last change time (12B) | type + perm (2B) | link count (2B) | uid (4B) | gid (4B) |
///   block0 (Id) | block1 (Id) | ... ]
impl Inode {

    pub fn new(block_mgr: &mut BlockMgr, id: Id) -> Result<Inode, std::io::Error> {
        Ok(Inode { id: id, dirty: false, data: block_mgr.read_block(id)? })
    }

    pub fn id(&self) -> Id {
        self.id
    }

    pub fn flush(&mut self, block_mgr: &mut BlockMgr) -> Result<(), std::io::Error> {
        if self.dirty {
            block_mgr.write_block(self.id, &self.data)?;
            self.dirty = false;
        }
        Ok(())
    }

    pub fn generation(&self) -> u64 {
        u64::from_le_bytes(self.data[generation_off .. generation_off + generation_size].try_into().unwrap())
    }

    // No need to set geneartion

    pub fn length(&self) -> u32 {
        u32::from_le_bytes(self.data[length_off .. length_off + length_size].try_into().unwrap())
    }

    pub fn set_length(&mut self, length: u32) {
        self.data[length_off .. length_off + length_size].copy_from_slice(&length.to_le_bytes());
        self.dirty = true;
    }

    pub fn atime(&self) -> time::Timespec {
        let sec = i64::from_le_bytes(self.data[atime_off .. atime_off + 8].try_into().unwrap());
        let nsec = i32::from_le_bytes(self.data[atime_off + 8 .. atime_off + 12].try_into().unwrap());
        time::Timespec { sec: sec, nsec: nsec }
    }

    pub fn set_atime(&mut self, atime: time::Timespec) {
        self.data[atime_off .. atime_off + 8].copy_from_slice(&atime.sec.to_le_bytes());
        self.data[atime_off + 8 .. atime_off + 12].copy_from_slice(&atime.nsec.to_le_bytes());
        self.dirty = true;
    }

    pub fn mtime(&self) -> time::Timespec {
        let sec = i64::from_le_bytes(self.data[mtime_off .. mtime_off + 8].try_into().unwrap());
        let nsec = i32::from_le_bytes(self.data[mtime_off + 8 .. mtime_off + 12].try_into().unwrap());
        time::Timespec { sec: sec, nsec: nsec }
    }

    pub fn set_mtime(&mut self, mtime: time::Timespec) {
        self.data[mtime_off .. mtime_off + 8].copy_from_slice(&mtime.sec.to_le_bytes());
        self.data[mtime_off + 8 .. mtime_off + 12].copy_from_slice(&mtime.nsec.to_le_bytes());
        self.dirty = true;
    }

    pub fn ctime(&self) -> time::Timespec {
        let sec = i64::from_le_bytes(self.data[ctime_off .. ctime_off + 8].try_into().unwrap());
        let nsec = i32::from_le_bytes(self.data[ctime_off + 8 .. ctime_off + 12].try_into().unwrap());
        time::Timespec { sec: sec, nsec: nsec }
    }

    pub fn set_ctime(&mut self, ctime: time::Timespec) {
        self.data[ctime_off .. ctime_off + 8].copy_from_slice(&ctime.sec.to_le_bytes());
        self.data[ctime_off + 8 .. ctime_off + 12].copy_from_slice(&ctime.nsec.to_le_bytes());
        self.dirty = true;
    }

    pub fn kind(&self) -> Result<fuse::FileType, std::io::Error> {
        let type_perm = u16::from_le_bytes(self.data[type_perm_off .. type_perm_off + type_perm_size].try_into().unwrap());
        match type_perm >> 12 {
            0 => Ok(fuse::FileType::RegularFile),
            1 => Ok(fuse::FileType::Directory),
            2 => Ok(fuse::FileType::Symlink),
            _ => Err(std::io::Error::from_raw_os_error(libc::EINVAL))
        }
    }

    pub fn set_kind(&mut self, kind: fuse::FileType) -> Result<(), std::io::Error> {
        let mut type_perm = u16::from_le_bytes(self.data[type_perm_off .. type_perm_off + type_perm_size].try_into().unwrap());
        type_perm = (type_perm & 0x0fff) | (match kind {
            fuse::FileType::RegularFile => 0,
            fuse::FileType::Directory => 1,
            fuse::FileType::Symlink => 2,
            _ => return Err(std::io::Error::from_raw_os_error(libc::EINVAL))
        } << 12);
        self.data[type_perm_off .. type_perm_off + type_perm_size].copy_from_slice(&type_perm.to_le_bytes());
        self.dirty = true;
        Ok(())
    }

    pub fn perm(&self) -> u16 {
        let type_perm = u16::from_le_bytes(self.data[type_perm_off .. type_perm_off + type_perm_size].try_into().unwrap());
        type_perm & 0x0fff
    }

    pub fn set_perm(&mut self, perm: u16) {
        let mut type_perm = u16::from_le_bytes(self.data[type_perm_off .. type_perm_off + type_perm_size].try_into().unwrap());
        type_perm = (type_perm & 0xf000) | perm;
        self.data[type_perm_off .. type_perm_off + type_perm_size].copy_from_slice(&type_perm.to_le_bytes());
        self.dirty = true;
    }

    pub fn nlink(&self) -> u16 {
        u16::from_le_bytes(self.data[nlink_off .. nlink_off + nlink_size].try_into().unwrap())
    }

    pub fn set_nlink(&mut self, nlink: u16) {
        self.data[nlink_off .. nlink_off + nlink_size].copy_from_slice(&nlink.to_le_bytes());
        self.dirty = true;
    }

    pub fn uid(&self) -> u32 {
        u32::from_le_bytes(self.data[uid_off .. uid_off + uid_size].try_into().unwrap())
    }

    pub fn set_uid(&mut self, uid: u32) {
        self.data[uid_off .. uid_off + uid_size].copy_from_slice(&uid.to_le_bytes());
        self.dirty = true;
    }

    pub fn gid(&self) -> u32 {
        u32::from_le_bytes(self.data[gid_off .. gid_off + gid_size].try_into().unwrap())
    }

    pub fn set_gid(&mut self, gid: u32) {
        self.data[gid_off .. gid_off + gid_size].copy_from_slice(&gid.to_le_bytes());
        self.dirty = true;
    }

    pub fn data_block(&self, index: usize) -> Id {
        Id::from_le_bytes(
            self.data[index_off + index * index_size .. index_off + (index + 1) * index_size]
            .try_into().unwrap()
        )
    }

    /// Set data block pointer. Need to flush manually later
    pub fn set_data_block(&mut self, index: usize, data_block: Id) {
        self.data[index_off + index * index_size .. index_off + (index + 1) * index_size]
            .copy_from_slice(&data_block.to_le_bytes());
        self.dirty = true;
    }
}

impl Drop for Inode {
    fn drop(&mut self) {
        assert!(!self.dirty); // Everything should be flushed manually
    }
}

