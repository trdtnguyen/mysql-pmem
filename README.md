# mysql-pmem

Author: Trong-Dat Nguyen

This work modify mysql-5.7.20 to test pmem

# install

## Dependencies
* [ndctl](https://github.com/pmem/ndctl) (for building PMDK and NVDIMM management)
* [PMDK](https://github.com/pmem/pmdk) (for libpmem* libraries)
* libaio (for Native Linux AIO)

* You should install dependencies before [building mysql from source code](https://dev.mysql.com/doc/mysql-sourcebuild-excerpt/5.7/en/)

## Notice: 
* We recommend you build the original MySQL first before building PB-NVM
* If you could not install **libndctl** and **libdaxctl** (v60.1 or later)  during install PMDK, you also skip those libraries by using `$ export NDCTL_ENABLE=n`


# Configuration

Additional PB-NVM's variables in my.cnf file:
```
innodb_pmem_home_dir=/mnt/pmem1
innodb_pmem_pool_size=16384
innodb_pmem_buf_size = 256
innodb_pmem_buf_n_buckets = 64
innodb_pmem_buf_bucket_size = 512
innodb_pmem_buf_flush_pct = 1
innodb_pmem_n_flush_threads = 32
innodb_pmem_flush_threshold = 30
innodb_pmem_n_space_bits = 5
innodb_pmem_page_per_bucket_bits = 10
```
