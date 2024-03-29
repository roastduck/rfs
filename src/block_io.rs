pub const BLOCK_SIZE: usize = 4096;
pub type Id = u16;

pub trait BlockIO {
    fn read(&mut self, block_id: Id) -> Result<[u8; BLOCK_SIZE], std::io::Error>;
    fn write(&mut self, block_id: Id, data: &[u8]) -> Result<(), std::io::Error>;
}

pub struct FakeMemBlockIO {
    blocks: Vec<Box<[u8; BLOCK_SIZE]>>,
}

impl FakeMemBlockIO {
    fn ensure_length(&mut self, block_id: Id) {
        while self.blocks.len() <= block_id as usize {
            self.blocks.push(Box::new([0; BLOCK_SIZE]));
        }
    }

    pub fn new() -> FakeMemBlockIO {
        FakeMemBlockIO { blocks: Vec::new() }
    }
}

impl BlockIO for FakeMemBlockIO {
    fn read(&mut self, block_id: Id) -> Result<[u8; BLOCK_SIZE], std::io::Error> {
        self.ensure_length(block_id);
        Ok(*self.blocks[block_id as usize])
    }

    fn write(&mut self, block_id: Id, data: &[u8]) -> Result<(), std::io::Error> {
        assert_eq!(data.len(), BLOCK_SIZE);
        self.ensure_length(block_id);
        self.blocks[block_id as usize].copy_from_slice(data);
        Ok(())
    }
}

pub struct FileBlockIO {
    path: std::path::PathBuf,
}

impl FileBlockIO {
    pub fn new(path: std::path::PathBuf) -> Result<FileBlockIO, std::io::Error> {
        std::fs::create_dir_all(&path)?;
        Ok(FileBlockIO { path: path })
    }
}

impl BlockIO for FileBlockIO {
    fn read(&mut self, block_id: Id) -> Result<[u8; BLOCK_SIZE], std::io::Error> {
        let mut path = std::path::PathBuf::from(&self.path);
        path.push(format!("blk-{}", block_id));
        let mut data = [0; BLOCK_SIZE];
        if path.is_file() {
            data.copy_from_slice(&std::fs::read(&path)?[..]);
        }
        Ok(data)
    }

    fn write(&mut self, block_id: Id, data: &[u8]) -> Result<(), std::io::Error> {
        let mut path = std::path::PathBuf::from(&self.path);
        path.push(format!("blk-{}", block_id));
        std::fs::write(&path, data)
    }
}

