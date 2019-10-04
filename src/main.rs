extern crate env_logger;
extern crate time;
extern crate fuse;
extern crate libc;

use std::convert::TryInto;

mod file_mgr;
use file_mgr::*;
use block_io::{Id, BLOCK_SIZE, FakeMemBlockIO};
use block_mgr::BlockMgr;
use inode::Inode;

const DIR_ITEM_SIZE: usize = 64;
const DIR_ITEM_INODE_SIZE: usize = std::mem::size_of::<Id>();
const DIR_ITME_NAME_LEN_SIZE: usize = 1;
const DIR_ITEM_NAME_SIZE: usize = DIR_ITEM_SIZE - DIR_ITEM_INODE_SIZE - DIR_ITME_NAME_LEN_SIZE;
const MAX_NAME_LEN: usize = DIR_ITEM_NAME_SIZE - 1;

struct Rfs {
    file_mgr: Box<FileMgr>,
}

impl Rfs {
    fn new(file_mgr: Box<FileMgr>) -> Rfs {
        Rfs { file_mgr: file_mgr }
    }

    fn as_id(x: u64) -> Result<Id, std::io::Error> {
        if x > Id::max_value() as u64 {
            Err(std::io::Error::from_raw_os_error(libc::EBADF))
        } else {
            Ok(x as Id)
        }
    }

    fn parse_dir_item(item: &[u8]) -> (Id, std::ffi::OsString) {
        let ino = Id::from_le_bytes(item[.. DIR_ITEM_INODE_SIZE].try_into().unwrap());
        let name_len = item[DIR_ITEM_INODE_SIZE] as usize;
        let name = std::str::from_utf8(&item[
            DIR_ITEM_INODE_SIZE + DIR_ITME_NAME_LEN_SIZE .. DIR_ITEM_INODE_SIZE + DIR_ITME_NAME_LEN_SIZE + name_len
        ]).unwrap();
        (ino, std::ffi::OsString::from(name))
    }

    fn assemby_dir_itme(ino: Id, name: &std::ffi::OsStr) -> Result<[u8; DIR_ITEM_SIZE], std::io::Error> {
        let name_str = name.to_string_lossy();
        let name_bytes = name_str.as_bytes();
        if name_bytes.len() > MAX_NAME_LEN {
            return Err(std::io::Error::from_raw_os_error(libc::ENAMETOOLONG))
        }
        let mut ret = [0; DIR_ITEM_SIZE];
        ret[.. DIR_ITEM_INODE_SIZE].copy_from_slice(&ino.to_le_bytes());
        ret[DIR_ITEM_INODE_SIZE] = name_bytes.len() as u8;
        ret[
            DIR_ITEM_INODE_SIZE + DIR_ITME_NAME_LEN_SIZE .. DIR_ITEM_INODE_SIZE + DIR_ITME_NAME_LEN_SIZE + name_bytes.len()
        ].copy_from_slice(&name_bytes);
        Ok(ret)
    }

    fn set_newly_created(&mut self, _req: &fuse::Request, inode: &Inode, mode: u16)
                        -> Result<(), std::io::Error> {
        let now = time::now_utc().to_timespec();
        inode.set_atime(now);
        inode.set_mtime(now);
        inode.set_ctime(now);
        inode.set_mode(mode);
        inode.set_nlink(1);
        inode.set_uid(_req.uid());
        inode.set_gid(_req.gid());
        self.file_mgr.flush(inode)
    }

    fn link_with_nlink_unchanged(&mut self, id: Id, newparent: &Inode, _newname: &std::ffi::OsStr)
                                 -> Result<(), std::io::Error> {
        let item = Rfs::assemby_dir_itme(id, _newname)?;
        let end_of_file = newparent.length() as usize;
        self.file_mgr.write_file(newparent, end_of_file, &item)?;
        Ok(())
    }

    fn init_impl(&mut self, _req: &fuse::Request) -> Result<(), std::io::Error> {
        let need_format = !self.file_mgr.is_formatted()?;
        self.file_mgr.init(need_format)?;
        let root = self.file_mgr.read_root_inode()?;
        self.set_newly_created(_req, &root, 0o040755)?;
        self.link_with_nlink_unchanged(root.id(), &root, &std::ffi::OsString::from("."))?;
        self.link_with_nlink_unchanged(root.id(), &root, &std::ffi::OsString::from(".."))?;
        Ok(())
    }

    fn lookup_impl(&mut self, _req: &fuse::Request, parent: &Inode, _name: &std::ffi::OsStr)
                   -> Result<(fuse::FileAttr, u64 /* generation */), std::io::Error> {
        let mut offset: usize = 0;
        loop {
            let item = self.file_mgr.read_file(parent, offset * DIR_ITEM_SIZE, DIR_ITEM_SIZE)?;
            if item.is_empty() {
                break Err(std::io::Error::from_raw_os_error(libc::ENOENT))
            }
            let (ino, name) = Rfs::parse_dir_item(&item);
            if name == _name {
                let inode = self.open_impl(_req, ino as u64, 0)?;
                let attr = self.getattr_impl(_req, &inode)?;
                let generation = inode.generation();
                break Ok((attr, generation))
            }
            offset += 1;
        }
    }

    fn getattr_impl(&mut self, _req: &fuse::Request, inode: &Inode) -> Result<fuse::FileAttr, std::io::Error> {
        Ok(fuse::FileAttr {
            ino: inode.id() as u64,
            size: inode.length() as u64,
            blocks: ((inode.length() as usize + BLOCK_SIZE - 1) / BLOCK_SIZE) as u64,
            atime: inode.atime(),
            mtime: inode.mtime(),
            ctime: inode.ctime(),
            crtime: time::Timespec::new(0, 0), // macOS only
            kind: inode.kind()?,
            perm: inode.perm(),
            nlink: inode.nlink() as u32,
            uid: inode.uid(),
            gid: inode.gid(),
            rdev: 0,
            flags: 0 // macOS only
        })
    }

    fn setattr_impl(
        &mut self, _req: &fuse::Request, inode: &Inode, _mode: Option<u32>, _uid: Option<u32>, _gid: Option<u32>,
        _size: Option<u64>, _atime: Option<time::Timespec>, _mtime: Option<time::Timespec>, _crtime: Option<time::Timespec>,
        _chgtime: Option<time::Timespec>, _bkuptime: Option<time::Timespec>, _flags: Option<u32>
    ) -> Result<fuse::FileAttr, std::io::Error> {
        if let Some(mode) = _mode { inode.set_mode(mode as u16); }
        if let Some(uid) = _uid { inode.set_uid(uid); }
        if let Some(gid) = _gid { inode.set_gid(gid); }
        if let Some(size) = _size { self.file_mgr.truncate_file(inode, size as usize)?; }
        if let Some(atime) = _atime { inode.set_atime(atime); }
        if let Some(mtime) = _mtime { inode.set_mtime(mtime); }
        if let Some(ctime) = _chgtime { inode.set_ctime(ctime); }
        self.file_mgr.flush(inode)?;
        self.getattr_impl(_req, &inode)
    }

    fn link_impl(&mut self, _req: &fuse::Request, inode: &Inode, newparent: &Inode, _newname: &std::ffi::OsStr)
                 -> Result<(fuse::FileAttr, u64 /* generation */), std::io::Error> {
        inode.set_nlink(inode.nlink() + 1);
        self.file_mgr.flush(inode)?;
        let attr = self.getattr_impl(_req, &inode)?;
        let generation = inode.generation();
        self.link_with_nlink_unchanged(inode.id(), newparent, _newname)?;
        Ok((attr, generation))
    }

    fn unlink_impl(&mut self, _req: &fuse::Request, parent: &Inode, _name: &std::ffi::OsStr) -> Result<(), std::io::Error> {
        let mut offset: usize = 0;
        let inode = loop {
            let item = self.file_mgr.read_file(parent, offset * DIR_ITEM_SIZE, DIR_ITEM_SIZE)?;
            if item.is_empty() {
                return Err(std::io::Error::from_raw_os_error(libc::ENOENT))
            }
            let (ino, name) = Rfs::parse_dir_item(&item);
            if name == _name {
                break self.open_impl(_req, ino as u64, 0)?;
            }
            offset += 1;
        };

        if inode.kind()? == fuse::FileType::Directory && inode.length() as usize > 2 * DIR_ITEM_SIZE { // 2 = "." + ".."
            return Err(std::io::Error::from_raw_os_error(libc::ENOTEMPTY));
        }

        let last_offset = parent.length() as usize / DIR_ITEM_SIZE - 1;
        if offset < last_offset {
            let last_item = self.file_mgr.read_file(parent, last_offset * DIR_ITEM_SIZE, DIR_ITEM_SIZE)?;
            self.file_mgr.write_file(parent, offset * DIR_ITEM_SIZE, &last_item[..])?;
        }
        self.file_mgr.truncate_file(parent, parent.length() as usize - DIR_ITEM_SIZE)?;

        let nlink = inode.nlink() - 1;
        if nlink > 0 {
            inode.set_nlink(nlink);
            self.file_mgr.flush(&inode)?;
        } else {
            self.file_mgr.del_inode(&inode)?;
        }
        Ok(())
    }

    fn read_impl(&mut self, _req: &fuse::Request, inode: &Inode, _offset: i64, _size: u32)
            -> Result<Vec<u8>, std::io::Error> {
        if _offset < 0 {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        self.file_mgr.read_file(inode, _offset as usize, _size as usize)
    }

    fn write_impl(&mut self, _req: &fuse::Request, inode: &Inode, _offset: i64, _data: &[u8], _flags: u32)
                  ->Result<usize, std::io::Error> {
        if _offset < 0 {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        self.file_mgr.write_file(inode, _offset as usize, _data)
    }

    fn mkdir_impl(&mut self, _req: &fuse::Request, parent: &Inode, _name: &std::ffi::OsStr, _mode: u16)
                  -> Result<(fuse::FileAttr, u64 /* generation */), std::io::Error> {
        let inode = self.file_mgr.new_inode()?;
        self.set_newly_created(_req, &inode, libc::S_IFDIR as u16 | (0o7777 &_mode))?;
        self.link_with_nlink_unchanged(inode.id(), &inode, &std::ffi::OsString::from("."))?;
        self.link_with_nlink_unchanged(parent.id(), &inode, &std::ffi::OsString::from(".."))?;
        let attr = self.getattr_impl(_req, &inode)?;
        let generation = inode.generation();

        let item = Rfs::assemby_dir_itme(inode.id(), _name)?;
        let end_of_file = parent.length() as usize;
        self.file_mgr.write_file(parent, end_of_file, &item)?;
        Ok((attr, generation))
    }

    fn open_impl(&mut self, _req: &fuse::Request, _ino: u64, _flags: u32)
                    -> Result<std::rc::Rc<Inode>, std::io::Error> {
        // TODO: Check permissions and flags
        self.file_mgr.read_inode(Rfs::as_id(_ino)?)
    }

    fn readdir_impl(&mut self, _req: &fuse::Request, inode: &Inode, _offset: i64, reply: &mut fuse::ReplyDirectory)
                    -> Result<(), std::io::Error> {
        if _offset < 0 {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL))
        }
        let mut offset = _offset as usize;
        loop {
            let item = self.file_mgr.read_file(&inode, offset * DIR_ITEM_SIZE, DIR_ITEM_SIZE)?;
            if item.is_empty() {
                break
            }
            let (ino, name) = Rfs::parse_dir_item(&item);
            let kind = self.open_impl(_req, ino as u64, 0)?.kind()?;
            if reply.add(ino as u64, offset as i64 + 1, kind, &name) {
                break
            }
            offset += 1;
        }
        Ok(())
    }

    fn create_impl(&mut self, _req: &fuse::Request, parent: &Inode, _name: &std::ffi::OsStr, _mode: u16, _flags: u32)
                   -> Result<(std::rc::Rc<Inode>, fuse::FileAttr, u64 /* generation */), std::io::Error> {
        // TODO: Check permissions and flags
        let inode = self.file_mgr.new_inode()?;
        self.set_newly_created(_req, &inode, libc::S_IFREG as u16 | (0o7777 &_mode))?;
        let attr = self.getattr_impl(_req, &inode)?;
        let generation = inode.generation();

        let item = Rfs::assemby_dir_itme(inode.id(), _name)?;
        let end_of_file = parent.length() as usize;
        self.file_mgr.write_file(parent, end_of_file, &item)?;
        Ok((inode, attr, generation))
    }
}

impl fuse::Filesystem for Rfs {
    fn init(&mut self, _req: &fuse::Request) -> Result<(), libc::c_int> {
        if let Err(err) = self.init_impl(_req) {
            return Err(err.raw_os_error().unwrap());
        }
        Ok(())
    }

    fn lookup(&mut self, _req: &fuse::Request, _parent: u64, _name: &std::ffi::OsStr, reply: fuse::ReplyEntry) {
        match self.open_impl(_req, _parent, 0) {
            Ok(inode) => match self.lookup_impl(_req, &*inode, _name) {
                Ok((attr, generation)) => reply.entry(&time::Timespec::new(0, 0), &attr, generation),
                Err(err) => reply.error(err.raw_os_error().unwrap())
            },
            Err(err) => reply.error(err.raw_os_error().unwrap())
        }
    }

    fn getattr(&mut self, _req: &fuse::Request, _ino: u64, reply: fuse::ReplyAttr) {
        match self.open_impl(_req, _ino, 0) {
            Ok(inode) => match self.getattr_impl(_req, &*inode) {
                Ok(attr) => reply.attr(&time::Timespec::new(0, 0), &attr),
                Err(err) => reply.error(err.raw_os_error().unwrap())
            },
            Err(err) => reply.error(err.raw_os_error().unwrap())
        }
    }

    fn setattr(
        &mut self, _req: &fuse::Request, _ino: u64, _mode: Option<u32>, _uid: Option<u32>, _gid: Option<u32>,
        _size: Option<u64>, _atime: Option<time::Timespec>, _mtime: Option<time::Timespec>, _fh: Option<u64>,
        _crtime: Option<time::Timespec>, _chgtime: Option<time::Timespec>, _bkuptime: Option<time::Timespec>,
        _flags: Option<u32>, reply: fuse::ReplyAttr
    ) {
        let result = if let Some(fh) = _fh {
            let inode = unsafe { &*(fh as *const Inode) };
            self.setattr_impl(_req, inode, _mode, _uid, _gid, _size, _atime, _mtime, _crtime, _chgtime, _bkuptime, _flags)
        } else {
            match self.open_impl(_req, _ino, _flags.unwrap_or_default() ) {
                Ok(inode) => self.setattr_impl(
                    _req, &inode, _mode, _uid, _gid, _size, _atime, _mtime, _crtime, _chgtime, _bkuptime, _flags
                ),
                Err(err) => return reply.error(err.raw_os_error().unwrap())
            }
        };
        match result {
            Ok(attr) => reply.attr(&time::Timespec::new(0, 0), &attr),
            Err(err) => reply.error(err.raw_os_error().unwrap())
        }
    }

    fn link(&mut self, _req: &fuse::Request, _ino: u64, _newparent: u64, _newname: &std::ffi::OsStr, reply: fuse::ReplyEntry) {
        match self.open_impl(_req, _newparent, 0) {
            Ok(newparent) => match self.open_impl(_req, _ino, 0) {
                Ok(inode) => match self.link_impl(_req, &inode, &newparent, _newname) {
                    Ok((attr, generation)) => reply.entry(&time::Timespec::new(0, 0), &attr, generation),
                    Err(err) => reply.error(err.raw_os_error().unwrap())
                },
                Err(err) => reply.error(err.raw_os_error().unwrap())
            },
            Err(err) => reply.error(err.raw_os_error().unwrap())
        }
    }

    fn unlink(&mut self, _req: &fuse::Request, _parent: u64, _name: &std::ffi::OsStr, reply: fuse::ReplyEmpty) {
        match self.open_impl(_req, _parent, 0) {
            Ok(parent) => if let Err(err) = self.unlink_impl(_req, &parent, _name) {
                reply.error(err.raw_os_error().unwrap())
            } else {
                reply.ok()
            },
            Err(err) => reply.error(err.raw_os_error().unwrap())
        }
    }

    fn open(&mut self, _req: &fuse::Request, _ino: u64, _flags: u32, reply: fuse::ReplyOpen) {
        match self.open_impl(_req, _ino, _flags) {
            Ok(inode) => reply.opened(std::rc::Rc::into_raw(inode) as u64, _flags),
            Err(err) => reply.error(err.raw_os_error().unwrap())
        }
    }

    fn read(&mut self, _req: &fuse::Request, _ino: u64, _fh: u64, _offset: i64, _size: u32, reply: fuse::ReplyData) {
        let inode = unsafe { &*(_fh as *const Inode) };
        match self.read_impl(_req, &inode, _offset, _size) {
            Ok(data) => reply.data(&data[..]),
            Err(err) => reply.error(err.raw_os_error().unwrap())
        }
    }

    fn write(
        &mut self, _req: &fuse::Request, _ino: u64, _fh: u64, _offset: i64, _data: &[u8], _flags: u32,
        reply: fuse::ReplyWrite
    ) {
        let inode = unsafe { &*(_fh as *const Inode) };
        match self.write_impl(_req, inode, _offset, _data, _flags) {
            Ok(size) => reply.written(size as u32),
            Err(err) => reply.error(err.raw_os_error().unwrap())
        }
    }

    fn flush(&mut self, _req: &fuse::Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: fuse::ReplyEmpty) {
        reply.ok();
    }

    fn release(
        &mut self, _req: &fuse::Request, _ino: u64, _fh: u64, _flags: u32, _lock_owner: u64,
        _flush: bool, reply: fuse::ReplyEmpty
    ) {
        unsafe { std::rc::Rc::from_raw(_fh as *const Inode); }
        reply.ok();
    }

    fn fsync(&mut self, _req: &fuse::Request, _ino: u64, _fh: u64, _datasync: bool, reply: fuse::ReplyEmpty) {
        reply.ok();
    }

    fn mkdir(&mut self, _req: &fuse::Request, _parent: u64, _name: &std::ffi::OsStr, _mode: u32, reply: fuse::ReplyEntry) {
        match self.open_impl(_req, _parent, 0) {
            Ok(parent) => match self.mkdir_impl(_req, &parent, _name, _mode as u16) {
                Ok((attr, generation)) => reply.entry(&time::Timespec::new(0, 0), &attr, generation),
                Err(err) => reply.error(err.raw_os_error().unwrap())
            },
            Err(err) => reply.error(err.raw_os_error().unwrap())
        }
    }

    fn rmdir(&mut self, _req: &fuse::Request, _parent: u64, _name: &std::ffi::OsStr, reply: fuse::ReplyEmpty) {
        match self.open_impl(_req, _parent, 0) {
            Ok(parent) => if let Err(err) = self.unlink_impl(_req, &parent, _name) {
                reply.error(err.raw_os_error().unwrap())
            } else {
                reply.ok()
            },
            Err(err) => reply.error(err.raw_os_error().unwrap())
        }
    }

    fn opendir(&mut self, _req: &fuse::Request, _ino: u64, _flags: u32, reply: fuse::ReplyOpen) {
        match self.open_impl(_req, _ino, _flags) {
            Ok(inode) => reply.opened(std::rc::Rc::into_raw(inode) as u64, _flags),
            Err(err) => reply.error(err.raw_os_error().unwrap())
        }
    }

    fn readdir(&mut self, _req: &fuse::Request, _ino: u64, _fh: u64, _offset: i64, _reply: fuse::ReplyDirectory) {
        let mut reply = _reply;
        let inode = unsafe { &*(_fh as *const Inode) };
        if let Err(err) = self.readdir_impl(_req, inode, _offset, &mut reply) {
            reply.error(err.raw_os_error().unwrap())
        } else {
            reply.ok()
        }
    }

    fn releasedir(&mut self, _req: &fuse::Request, _ino: u64, _fh: u64, _flags: u32, reply: fuse::ReplyEmpty) {
        unsafe { std::rc::Rc::from_raw(_fh as *const Inode); }
        reply.ok();
    }

    fn fsyncdir(&mut self, _req: &fuse::Request, _ino: u64, _fh: u64, _datasync: bool, reply: fuse::ReplyEmpty) {
        reply.ok();
    }

    fn create(
        &mut self, _req: &fuse::Request, _parent: u64, _name: &std::ffi::OsStr, _mode: u32, _flags: u32, reply: fuse::ReplyCreate
    ) {
        match self.open_impl(_req, _parent, 0) {
            Ok(parent) => match self.create_impl(_req, &parent, _name, _mode as u16,  _flags) {
                Ok((inode, attr, generation)) => reply.created(
                    &time::Timespec::new(0, 0), &attr, generation, std::rc::Rc::into_raw(inode) as u64, _flags
                ),
                Err(err) => reply.error(err.raw_os_error().unwrap())
            },
            Err(err) => reply.error(err.raw_os_error().unwrap())
        }
    }
}

fn main() -> Result<(), std::io::Error> {
    env_logger::init();

    let argv: Vec<std::ffi::OsString> = std::env::args_os().collect();
    let argv_ref: Vec<&std::ffi::OsStr> = argv.iter().map(|x| x.as_ref()).collect();
    if argv.len() < 2 {
        println!("Usage: {:?} mount_point [options ...]", argv[0]);
        std::process::exit(-1);
    }

    let block_io = Box::new(FakeMemBlockIO::new()); // TODO: Use real block IO
    let block_mgr = Box::new(BlockMgr::new(block_io));
    let file_mgr = Box::new(FileMgr::new(block_mgr));
    fuse::mount(Rfs::new(file_mgr), &argv_ref[1], &argv_ref[2 ..])
}

