#[path="block_mgr.rs"]
mod block_mgr;

use std::convert::TryInto;

pub use block_mgr::{BlockMgr, Id, BLOCK_SIZE, FakeMemBlockIO};

pub struct Inode {
    id: Id,
    dirty: bool,
    data: [u8; BLOCK_SIZE],
}

const length_size: usize = std::mem::size_of::<u32>();
const index_size: usize = std::mem::size_of::<Id>();

/// For layout of each inode is like:
/// [ length (u32) | block0 (Id) | block1 (Id) | ... ]
impl Inode {

    pub fn new(block_mgr: &mut BlockMgr, id: Id) -> Result<Inode, std::io::Error> {
        Ok(Inode { id: id, dirty: false, data: block_mgr.read_block(id)? })
    }

    pub fn id(&self) -> Id {
        self.id
    }

    pub fn flush(&mut self, block_mgr: &mut BlockMgr) -> Result<(), std::io::Error> {
        if self.dirty {
            block_mgr.write_block(self.id, &self.data)
        } else {
            Ok(())
        }
    }

    pub fn length(&self) -> u32 {
        u32::from_le_bytes(self.data[0 .. length_size].try_into().unwrap())
    }

    /// Set data block count. Need to flush manually later
    pub fn set_length(&mut self, length: u32) {
        self.data[0 .. length_size].copy_from_slice(&length.to_le_bytes());
        self.dirty = true;
    }

    pub fn data_block(&self, index: usize) -> Id {
        Id::from_le_bytes(
            self.data[length_size + index * index_size .. length_size + (index + 1) * index_size]
            .try_into().unwrap()
        )
    }

    /// Set data block pointer. Need to flush manually later
    pub fn set_data_block(&mut self, index: usize, data_block: Id) {
        self.data[length_size + index * index_size .. length_size + (index + 1) * index_size]
            .copy_from_slice(&data_block.to_le_bytes());
        self.dirty = true;
    }
}

