pub const BLOCK_SIZE: usize = 4096;
pub type Id = u16;

pub trait BlockIO {
    fn read(&mut self, blockId: Id) -> Result<[u8; BLOCK_SIZE], std::io::Error>;
    fn write(&mut self, blockId: Id, data: &[u8; BLOCK_SIZE]) -> Result<(), std::io::Error>;
}

pub struct FakeMemBlockIO {
    blocks: Vec<Box<[u8; BLOCK_SIZE]>>,
}

impl FakeMemBlockIO {
    fn ensure_length(&mut self, blockId: Id) {
        while self.blocks.len() <= blockId as usize {
            self.blocks.push(Box::new([0; BLOCK_SIZE]));
        }
    }

    pub fn new() -> FakeMemBlockIO {
        FakeMemBlockIO { blocks: Vec::new() }
    }
}

impl BlockIO for FakeMemBlockIO {
    fn read(&mut self, blockId: Id) -> Result<[u8; BLOCK_SIZE], std::io::Error> {
        self.ensure_length(blockId);
        Ok(*self.blocks[blockId as usize])
    }

    fn write(&mut self, blockId: Id, data: &[u8; BLOCK_SIZE]) -> Result<(), std::io::Error> {
        self.ensure_length(blockId);
        *self.blocks[blockId as usize] = *data;
        Ok(())
    }
}

