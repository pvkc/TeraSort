![alt text](https://travis-ci.com/pvkc/TeraSort.svg?branch=master)
# Tera Sort
Disk based Sort algorithm. Simply put, external sorts are used when the data to be sorted does not fit into the main memory of the computer. It has to be written to an external disk, broken down into chunks and loaded into memory whenever the data is needed. To attain maximum efficiency, the I/O operations as well as the sort mechanism itself are parallelized.


# Benchmarks
The input to the program is a 100 byte records of which the first 10 bytes is used as sort key. The input is generated using [gensort](http://www.ordinal.com/gensort.html). The output is verified using [valsort](http://www.ordinal.com/gensort.html). Two tests were run on AWS EC2 using this code with input sizes of 128GB and 1 TB. The machines used and the time taken is shown below.


| i3.large (128 GB Input) | i3.4xlarge (1TB Input) |
|:-----------------------:|:---------------------:|
| 6984 Sec                | 37750.4 Sec            |

Earlier versions of code was able to sort 1TB input in 42278 sec. That's approximately a 11% improvement in the runtime.
