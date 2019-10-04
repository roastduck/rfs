extern crate libc;

use std::convert::TryInto;

#[path="inode.rs"]
pub mod inode;
pub use inode::{block_io, block_mgr};

use inode::*;
use block_io::{Id, BLOCK_SIZE};
use block_mgr::BlockMgr;

const INODE_TALBE_SIZE: usize = Id::max_value() as usize + 1;

pub struct FileMgr {
    block_mgr: Box<BlockMgr>,
    inode_table: Vec<std::rc::Weak<Inode>>,
}

impl FileMgr {
    pub fn new(block_mgr: Box<BlockMgr>) -> FileMgr {
        let mut obj = FileMgr { block_mgr: block_mgr, inode_table: Vec:: new() };
        obj.inode_table.resize_with(INODE_TALBE_SIZE, || std::rc::Weak::new());
        obj
    }

    pub fn is_formatted(&mut self) -> Result<bool, std::io::Error> {
        self.block_mgr.is_formatted()
    }

    pub fn init(&mut self, need_format: bool) -> Result<(), std::io::Error> {
        self.block_mgr.init(need_format)?;
        if need_format {
            let root_inode = self.new_inode()?;
            assert_eq!(root_inode.id(), 1);
        }
        Ok(())
    }

    pub fn new_inode(&mut self) -> Result<std::rc::Rc<Inode>, std::io::Error> {
        let id = self.block_mgr.new_block()?;
        let mut block = self.block_mgr.read_block(id)?;
        let mut generation = u64::from_le_bytes(block[0 .. 8].try_into().unwrap());
        generation = generation.overflowing_add(1).0;
        block[0 .. 8].copy_from_slice(&generation.to_le_bytes());
        block[8 ..].copy_from_slice(&[0; BLOCK_SIZE - 8]);
        self.block_mgr.write_block(id, &block)?;
        self.read_inode(id)
    }

    pub fn read_inode(&mut self, id: Id) -> Result<std::rc::Rc<Inode>, std::io::Error> {
        if let Some(inode) = self.inode_table[id as usize - 1].upgrade() {
            return Ok(inode)
        }
        let inode = std::rc::Rc::new(Inode::new(&mut*self.block_mgr, id)?);
        self.inode_table[id as usize - 1] = std::rc::Rc::downgrade(&inode);
        Ok(inode)
    }

    pub fn read_root_inode(&mut self) -> Result<std::rc::Rc<Inode>, std::io::Error> {
        self.read_inode(1)
    }

    pub fn del_inode(&mut self, inode: &Inode) -> Result<(), std::io::Error> {
        self.block_mgr.del_block(inode.id())
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

        if start / BLOCK_SIZE == end / BLOCK_SIZE {
            let id = inode.data_block(start / BLOCK_SIZE);
            if id > 0 {
                let block = self.block_mgr.read_block(id)?;
                return Ok(Vec::from(&block[start % BLOCK_SIZE .. end % BLOCK_SIZE]));
            } else {
                return Ok(vec![0; end - start]);
            }
        }

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

    pub fn write_file(&mut self, inode: &Inode, offset: usize, data: &[u8]) -> Result<usize, std::io::Error> {
        let start = offset;
        let end = start + data.len();

        if start / BLOCK_SIZE == end / BLOCK_SIZE {
            let blkno = start / BLOCK_SIZE;
            let mut id = inode.data_block(blkno);
            let mut block = if id == 0 {
                id = self.block_mgr.new_block()?;
                inode.set_data_block(blkno, id);
                [0; BLOCK_SIZE]
            } else {
                self.block_mgr.read_block(id)?
            };
            block[start % BLOCK_SIZE .. end % BLOCK_SIZE].copy_from_slice(data);
            self.block_mgr.write_block(id, &block)?;
            inode.set_length(std::cmp::max(inode.length(), (offset + data.len()) as u32));

            inode.flush(&mut*self.block_mgr)?;
            return Ok(data.len())
        }

        let start_block = (start + BLOCK_SIZE - 1) / BLOCK_SIZE; // First full block
        let end_block = end / BLOCK_SIZE; // Last full block
        let mut write_cnt = 0;
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
            write_cnt += BLOCK_SIZE - start % BLOCK_SIZE;
            inode.set_length(std::cmp::max(inode.length(), (offset + write_cnt) as u32))
        }
        for i in start_block .. end_block {
            let mut id = inode.data_block(i);
            if id == 0 {
                id = self.block_mgr.new_block()?;
                inode.set_data_block(i, id);
            }
            self.block_mgr.write_block(id, &data[write_cnt .. write_cnt + BLOCK_SIZE])?;
            write_cnt += BLOCK_SIZE;
            inode.set_length(std::cmp::max(inode.length(), (offset + write_cnt) as u32))
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
            block[.. end % BLOCK_SIZE].copy_from_slice(&data[write_cnt ..]);
            self.block_mgr.write_block(id, &block)?;
            write_cnt += end % BLOCK_SIZE;
            inode.set_length(std::cmp::max(inode.length(), (offset + write_cnt) as u32))
        }
        assert_eq!(write_cnt, data.len());

        inode.flush(&mut*self.block_mgr)?;
        Ok(write_cnt)
    }

    pub fn truncate_file(&mut self, inode: &Inode, length: usize) -> Result<(), std::io::Error> {
        if length < inode.length() as usize {
            let block_cnt_plus_one = (length + BLOCK_SIZE - 1) / BLOCK_SIZE;
            let old_block_cnt = (inode.length() as usize + BLOCK_SIZE - 1) / BLOCK_SIZE;
            for i in block_cnt_plus_one .. old_block_cnt {
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

    pub fn flush(&mut self, inode: &Inode) -> Result<(), std::io::Error> {
        inode.flush(&mut*self.block_mgr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use block_io::FakeMemBlockIO;

    fn init() -> Result<Box<FileMgr>, std::io::Error> {
        let block_io = Box::new(FakeMemBlockIO::new());
        let block_mgr = Box::new(BlockMgr::new(block_io));
        let mut inode_mgr = Box::new(FileMgr::new(block_mgr));
        let need_format = !inode_mgr.is_formatted()?;
        inode_mgr.init(need_format)?;
        Ok(inode_mgr)
    }

    #[test]
    fn test_write_inside_1_block() -> Result<(), std::io::Error> {
        let mut inode_mgr = init()?;
        let inode = inode_mgr.read_root_inode()?;
        inode_mgr.write_file(&inode, 5, &[1, 2, 3, 4, 5])?;
        let file_read = inode_mgr.read_file(&inode, 0, BLOCK_SIZE)?;
        assert_eq!(file_read, [0, 0, 0, 0, 0, 1, 2, 3, 4, 5]);
        Ok(())
    }

    #[test]
    fn test_read_inside_1_block() -> Result<(), std::io::Error> {
        let mut inode_mgr = init()?;
        let inode = inode_mgr.read_root_inode()?;
        inode_mgr.write_file(&inode, 0, &[0, 0, 0, 0, 0, 1, 2, 3, 4, 5])?;
        let file_read = inode_mgr.read_file(&inode, 5, 5)?;
        assert_eq!(file_read, [1, 2, 3, 4, 5]);
        Ok(())
    }

    #[test]
    fn test_write_parts() -> Result<(), std::io::Error> {
        let mut inode_mgr = init()?;
        let mut file = vec![];
        for i in 0 .. 10000 {
            file.push((i % 256) as u8)
        }

        let inode = inode_mgr.read_root_inode()?;
        inode_mgr.write_file(&inode, 0, &file[0 .. 2000])?;
        inode_mgr.write_file(&inode, 2000, &file[2000 .. 8000])?;
        inode_mgr.write_file(&inode, 8000, &file[8000 .. 10000])?;
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

        let inode = inode_mgr.read_root_inode()?;
        inode_mgr.write_file(&inode, 0, &file[..])?;
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

        let inode = inode_mgr.read_root_inode()?;
        inode_mgr.write_file(&inode, 6000, &file[..])?;
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

        let inode = inode_mgr.read_root_inode()?;
        inode_mgr.write_file(&inode, 0, &file[..])?;
        inode_mgr.truncate_file(&inode, 6000)?;
        inode_mgr.truncate_file(&inode, 10000)?;
        let file_read = inode_mgr.read_file(&inode, 0, 999999)?;
        assert_eq!(file_read[.. 6000], file[.. 6000]);
        assert_eq!(file_read[6000 ..], [0; 4000][..]);
        Ok(())
    }

    #[test]
    fn test_share_inode() -> Result<(), std::io::Error> {
        let mut inode_mgr = init()?;
        let inode_a = inode_mgr.read_root_inode()?;
        let inode_b = inode_mgr.read_root_inode()?;
        inode_a.set_uid(1);
        inode_b.set_uid(2);
        assert_eq!(inode_a.uid(), inode_b.uid());
        inode_mgr.flush(&inode_a)?;
        Ok(())
    }
}

