#[path="block_io.rs"]
mod block_io;

use block_io::*;

pub struct BlockMgr {
    block_io: Box<dyn BlockIO>,
    bitmap_block: [u8; BLOCK_SIZE],
}

impl BlockMgr {
    fn format(&mut self) -> Result<(), std::io::Error> {
        let mut super_block = [0; BLOCK_SIZE];
        super_block[0 .. 4].copy_from_slice(&[114, 102, 115, 46]);
        self.block_io.write(0, &super_block)?;
        self.block_io.write(1, &[0; BLOCK_SIZE])?; // bitmap block
        Ok(())
    }

    fn first_empty_block(&self) -> Result<Id, std::io::Error> {
        for i in 0 .. BLOCK_SIZE {
            let occupied = (!self.bitmap_block[i]).trailing_zeros() as usize;
            if occupied != 8 {
                return Ok((i * 8 + occupied) as Id)
            }
        }
        Err(std::io::Error::new(std::io::ErrorKind::Other, "No empty block"))
    }

    pub fn new(block_io: Box<dyn BlockIO>) -> BlockMgr {
        BlockMgr { block_io: block_io, bitmap_block: [0; BLOCK_SIZE] }
    }

    pub fn init(&mut self) -> Result<(), std::io::Error> {
        let super_block = self.block_io.read(0)?;
        if &super_block[0 .. 4] != [114, 102, 115, 46] {
            self.format()?;
        }
        self.bitmap_block = self.block_io.read(1)?;
        Ok(())
    }

    pub fn new_block(&mut self) -> Result<Id, std::io::Error> {
        let id = self.first_empty_block()?;
        self.bitmap_block[(id / 8) as usize] |= 1 << (id % 8);
        self.block_io.write(1, &self.bitmap_block)?;
        Ok(id)
    }

    pub fn del_block(&mut self, id: Id) -> Result<(), std::io::Error> {
        self.bitmap_block[(id / 8) as usize] &= !(1 << (id % 8));
        self.block_io.write(1, &self.bitmap_block)?;
        Ok(())
    }

    pub fn read_block(&mut self, id: Id) -> Result<[u8; BLOCK_SIZE], std::io::Error> {
        assert!((self.bitmap_block[(id / 8) as usize] & (1 << (id % 8))) != 0);
        self.block_io.read(id)
    }

    pub fn write_block(&mut self, id: Id, data: &[u8; BLOCK_SIZE]) -> Result<(), std::io::Error> {
        assert!((self.bitmap_block[(id / 8) as usize] & (1 << (id % 8))) != 0);
        self.block_io.write(id, data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_del_blocks() -> Result<(), std::io::Error> {
        let mut block_mgr = BlockMgr::new(Box::new(FakeMemBlockIO::new()));
        for i in 0 .. 32 {
            let id = block_mgr.new_block()?;
            assert_eq!(id, i);
        }
        block_mgr.del_block(20)?;
        block_mgr.del_block(10)?;
        let id = block_mgr.new_block()?;
        assert_eq!(id, 10);
        Ok(())
    }
}

