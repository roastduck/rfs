## 实验报告

### 编译

***`bin`目录下已提供有预编译版本，正常情况下可跳过编译。***

1. 安装Rust工具链；
2. `cargo build --release`

### 运行

```
bin/rfs-x86_64 <挂载点> [其他FUSE参数...]
```

**请注意参数顺序。**

可以设置的环境变量：

1. **`STORAGE_DIR=<任意路径> `: 储存持久化信息的路径。 默认为`/tmp/rfs`。**
2. `RUST_LOG=debug`: 打印调试信息。
3. `FAKE_STORAGE`: 不使用持久化存储，仅使用内存（调试用）。

其他FUSE参数有：

```
-o allow_other         allow access to other users
-o allow_root          allow access to root
-o auto_unmount        auto unmount on process termination
-o nonempty            allow mounts over non-empty file/dir
-o default_permissions enable permission checking by kernel
-o fsname=NAME         set filesystem name
-o subtype=NAME        set filesystem type
-o large_read          issue large read requests (2.4 only)
-o max_read=N          set maximum size of read requests
```

### 设计

FUSE以下，系统分为4层：

1. 数据块读写层，体现在`src/block_io.rs`。此层负责处理单个数据块的独写，直接与持久化存储交互。
2. 数据块管理层，体现在`src/block_mgr.rs`。此层负责管理数据块的分配与释放，并维护数据块0作为超级块以管理文件系统元信息，以及数据块1作为表示各个数据块是否空闲的bitmap。
3. inode层，体现在`src/inode.rs`。此层负责管理文件元信息，包括数据块索引及文件属性。数据块索引包括直接储存在inode块上的直接索引，和一个间接索引块。文件属性包括generation（用于支持NFS）、长度、创建/修改/访问时间、类型及权限、引用计数，和用户及组编号。
4. 文件层，体现在`src/file_mgr.rs`。此层负责协调跨数据块的文件读写，并在文件长度改变时负责分配或释放数据块。
5. 文件系统层，体现在`src/main.rs`，负责在文件层之上实现FUSE需要提供的所有原语。

注意，文件系统的某些功能，例如部分文件权限的管理，及软链接路径的解析等，在FUSE之上实现，与本程序无关。

### 思考题

*1，当目录下有大量小文件时（成千上万），可能优化方法*

1. 使用可感知数据块的索引数据结构（例如B树）在目录中存储数据项，而不是顺序存储；
2. 打开目录后，将目录内容缓存至内存中，并在内存中建立索引数据结构（例如哈希表）。

*2，文件系统不同层提供的功能，Fuse的接口分别使用了哪些层的功能，以及分层必要性*

1. FUSE访问文件的数据及元数据，是File Layer的功能。
2. FUSE使用inode number区分文件，是inode Number Layer的功能。
3. FUSE的许多接口需要在特定文件夹中按文件名查找文件，是File Name Layer的功能。
4. Path Name Layer、Absolute Path Name Layer及Symbolic Link Layer不被FUSE接口使用，而是在FUSE内或FUSE之上实现。

分层可以降低软件的开发难度，使单独一层更易于开发、调试和优化，并增加软件的可移植性。但严格依照上述分层并不是必要的。FUSE与许多层都有交互，这样分层并不能减少层间依赖。若采用不恰当的分层，反而会增加软件的复杂性。