#[path="block_mgr.rs"]
mod block_mgr;

use std::convert::TryInto;

use block_mgr::*;

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

    fn new(block_mgr: &mut BlockMgr, id: Id) -> Result<Inode, std::io::Error> {
        Ok(Inode { id: id, dirty: false, data: block_mgr.read_block(id)? })
    }

    fn flush(&mut self, block_mgr: &mut BlockMgr) -> Result<(), std::io::Error> {
        if self.dirty {
            block_mgr.write_block(self.id, &self.data)
        } else {
            Ok(())
        }
    }

    fn length(&self) -> u32 {
        u32::from_le_bytes(self.data[0 .. length_size].try_into().unwrap())
    }

    /// Set data block count. Need to flush manually later
    fn set_length(&mut self, length: u32) {
        self.data[0 .. length_size].copy_from_slice(&length.to_le_bytes());
        self.dirty = true;
    }

    fn data_block(&self, index: usize) -> Id {
        Id::from_le_bytes(
            self.data[length_size + index * index_size .. length_size + (index + 1) * index_size]
            .try_into().unwrap()
        )
    }

    /// Set data block pointer. Need to flush manually later
    fn set_data_block(&mut self, index: usize, data_block: Id) {
        self.data[length_size + index * index_size .. length_size + (index + 1) * index_size]
            .copy_from_slice(&data_block.to_le_bytes());
        self.dirty = true;
    }
}

pub struct InodeMgr {
    block_mgr: Box<BlockMgr>,
}

impl InodeMgr {
    pub fn new(block_mgr: Box<BlockMgr>) -> InodeMgr {
        InodeMgr { block_mgr: block_mgr }
    }

    pub fn is_formatted(&mut self) -> Result<bool, std::io::Error> {
        self.block_mgr.is_formatted()
    }

    pub fn init(&mut self, need_format: bool) -> Result<(), std::io::Error> {
        self.block_mgr.init(need_format)?;
        if need_format {
            let root_inode = self.new_inode()?;
            assert_eq!(root_inode.id, 0);
        }
        Ok(())
    }

    pub fn new_inode(&mut self) -> Result<Inode, std::io::Error> {
        let id = self.block_mgr.new_block()?;
        self.block_mgr.write_block(id, &[0; BLOCK_SIZE])?;
        Inode::new(&mut*self.block_mgr, id)
    }

    pub fn read_inode(&mut self, id: Id) -> Result<Inode, std::io::Error> {
        Inode::new(&mut*self.block_mgr, id)
    }

    pub fn read_root_inode(&mut self) -> Result<Inode, std::io::Error> {
        self.read_inode(0)
    }

    pub fn del_inode(&mut self, inode: &Inode) -> Result<(), std::io::Error> {
        self.block_mgr.del_block(inode.id)
    }

    pub fn read_file(&mut self, inode: &Inode, offset: usize, count: usize)
                      -> Result<Vec<u8>, std::io::Error> {
        let length = inode.length() as usize;
        if offset >= length {
            return Ok(vec![])
        }

        let start = offset;
        let end = std::cmp::min(length, offset + count);
        let mut ret = vec![];
        ret.reserve(end - start);

        let start_block = (start + BLOCK_SIZE - 1) / BLOCK_SIZE; // First full block
        let end_block = end / BLOCK_SIZE; // Last full block
        if start % BLOCK_SIZE != 0 {
            let id = inode.data_block(start_block - 1);
            if id > 0 {
                let block = self.block_mgr.read_block(id)?;
                ret.extend_from_slice(&block[start % BLOCK_SIZE ..]);
            } else {
                ret.extend(vec![0; BLOCK_SIZE - start % BLOCK_SIZE])
            }
        }
        for i in start_block .. end_block {
            let id = inode.data_block(i);
            if id > 0 {
                let block = self.block_mgr.read_block(id)?;
                ret.extend(block.iter());
            } else {
                ret.extend([0; BLOCK_SIZE].iter())
            }
        }
        if end % BLOCK_SIZE != 0 {
            let id = inode.data_block(end_block);
            if id > 0 {
                let block = self.block_mgr.read_block(id)?;
                ret.extend_from_slice(&block[.. end % BLOCK_SIZE]);
            } else {
                ret.extend(vec![0; end % BLOCK_SIZE])
            }
        }

        Ok(ret)
    }

    fn write_file_impl(&mut self, inode: &mut Inode, offset: usize, data: &[u8], write_cnt: &mut usize)
                        -> Result<(), std::io::Error> {
        let start = offset;
        let end = start + data.len();
        let start_block = (start + BLOCK_SIZE - 1) / BLOCK_SIZE; // First full block
        let end_block = end / BLOCK_SIZE; // Last full block

        *write_cnt = 0;
        if start % BLOCK_SIZE != 0 {
            let mut id = inode.data_block(start_block - 1);
            let mut block = if id == 0 {
                id = self.block_mgr.new_block()?;
                inode.set_data_block(start_block - 1, id);
                [0; BLOCK_SIZE]
            } else {
                self.block_mgr.read_block(id)?
            };
            block[start % BLOCK_SIZE ..].copy_from_slice(&data[.. BLOCK_SIZE - start % BLOCK_SIZE]);
            self.block_mgr.write_block(id, &block)?;
            *write_cnt += BLOCK_SIZE - start % BLOCK_SIZE;
            inode.set_length(std::cmp::max(inode.length(), (offset + *write_cnt) as u32))
        }
        for i in start_block .. end_block {
            let mut id = inode.data_block(i);
            if id == 0 {
                id = self.block_mgr.new_block()?;
                inode.set_data_block(i, id);
            }
            self.block_mgr.write_block(id, &data[*write_cnt .. *write_cnt + BLOCK_SIZE])?;
            *write_cnt += BLOCK_SIZE;
            inode.set_length(std::cmp::max(inode.length(), (offset + *write_cnt) as u32))
        }
        if end % BLOCK_SIZE != 0 {
            let mut id = inode.data_block(end_block);
            let mut block = if id == 0 {
                id = self.block_mgr.new_block()?;
                inode.set_data_block(end_block, id);
                [0; BLOCK_SIZE]
            } else {
                self.block_mgr.read_block(id)?
            };
            block[.. end % BLOCK_SIZE].copy_from_slice(&data[*write_cnt ..]);
            self.block_mgr.write_block(id, &block)?;
            *write_cnt += end % BLOCK_SIZE;
            inode.set_length(std::cmp::max(inode.length(), (offset + *write_cnt) as u32))
        }
        assert_eq!(*write_cnt, data.len());
        Ok(())
    }

    pub fn write_file(&mut self, inode: &mut Inode, offset: usize, data: &[u8])
                       -> Result<usize, std::io::Error> {
        let mut write_cnt = 0;
        let result = self.write_file_impl(inode, offset, data, &mut write_cnt);
        inode.flush(&mut*self.block_mgr)?;
        if let Err(error) = result {
            if let Some(errno) = error.raw_os_error() {
                if errno != 28 { // ENOSPC
                    return Err(error)
                }
            }
        }
        Ok(write_cnt)
    }

    pub fn truncate_file(&mut self, inode: &mut Inode, length: usize) -> Result<(), std::io::Error> {
        if length < inode.length() as usize {
            let block_cnt = (length + BLOCK_SIZE - 1) / BLOCK_SIZE - 1;
            let old_block_cnt = (inode.length() as usize + BLOCK_SIZE - 1) / BLOCK_SIZE;
            for i in block_cnt + 1 .. old_block_cnt {
                let id = inode.data_block(i);
                if id > 0 {
                    inode.set_data_block(i, 0);
                    self.block_mgr.del_block(id)?;
                }
            }

            if length % BLOCK_SIZE != 0 {
                let id = inode.data_block(length / BLOCK_SIZE);
                if id > 0 {
                    let mut block = self.block_mgr.read_block(id)?;
                    for i in length % BLOCK_SIZE .. BLOCK_SIZE {
                        block[i] = 0;
                    }
                    self.block_mgr.write_block(id, &block[..])?;
                }
            }
        }
        inode.set_length(length as u32);
        inode.flush(&mut*self.block_mgr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init() -> Result<Box<InodeMgr>, std::io::Error> {
        let block_io = Box::new(FakeMemBlockIO::new());
        let block_mgr = Box::new(BlockMgr::new(block_io));
        let mut inode_mgr = Box::new(InodeMgr::new(block_mgr));
        let need_format = !inode_mgr.is_formatted()?;
        inode_mgr.init(need_format)?;
        Ok(inode_mgr)
    }

    #[test]
    fn test_write_parts() -> Result<(), std::io::Error> {
        let mut inode_mgr = init()?;
        let mut file = vec![];
        for i in 0 .. 10000 {
            file.push((i % 256) as u8)
        }

        let mut inode = inode_mgr.read_root_inode()?;
        inode_mgr.write_file(&mut inode, 0, &file[0 .. 2000])?;
        inode_mgr.write_file(&mut inode, 2000, &file[2000 .. 8000])?;
        inode_mgr.write_file(&mut inode, 8000, &file[8000 .. 10000])?;
        let file_read = inode_mgr.read_file(&inode, 0, 10000)?;
        assert_eq!(file_read, file);
        Ok(())
    }

    #[test]
    fn test_read_parts() -> Result<(), std::io::Error> {
        let mut inode_mgr = init()?;
        let mut file = vec![];
        for i in 0 .. 10000 {
            file.push((i % 256) as u8)
        }

        let mut inode = inode_mgr.read_root_inode()?;
        inode_mgr.write_file(&mut inode, 0, &file[..])?;
        let read0 = inode_mgr.read_file(&inode, 0, 2000)?;
        assert_eq!(read0[..], file[0 .. 2000]);
        let read1 = inode_mgr.read_file(&inode, 2000, 6000)?;
        assert_eq!(read1[..], file[2000 .. 8000]);
        let read2 = inode_mgr.read_file(&inode, 8000, 2000)?;
        assert_eq!(read2[..], file[8000 .. 10000]);
        Ok(())
    }

    #[test]
    fn test_hole() -> Result<(), std::io::Error> {
        let mut inode_mgr = init()?;
        let mut file = vec![];
        for i in 0 .. 3000 {
            file.push((i % 256) as u8)
        }

        let mut inode = inode_mgr.read_root_inode()?;
        inode_mgr.write_file(&mut inode, 6000, &file[..])?;
        let file_read = inode_mgr.read_file(&inode, 0, 9000)?;
        assert_eq!(file_read[.. 6000], [0; 6000][..]);
        assert_eq!(file_read[6000 ..], file[..]);
        Ok(())
    }

    #[test]
    fn test_truncate_file() -> Result<(), std::io::Error> {
        let mut inode_mgr = init()?;
        let mut file = vec![];
        for i in 0 .. 9000 {
            file.push((i % 256) as u8)
        }

        let mut inode = inode_mgr.read_root_inode()?;
        inode_mgr.write_file(&mut inode, 0, &file[..])?;
        inode_mgr.truncate_file(&mut inode, 6000)?;
        inode_mgr.truncate_file(&mut inode, 10000)?;
        let file_read = inode_mgr.read_file(&inode, 0, 999999)?;
        assert_eq!(file_read[.. 6000], file[.. 6000]);
        assert_eq!(file_read[6000 ..], [0; 4000][..]);
        Ok(())
    }
}

