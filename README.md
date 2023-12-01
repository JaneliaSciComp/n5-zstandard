# n5-zstandard

[Zstandard](https://facebook.github.io/zstd/) Compression for N5

This library uses the Zstandard Java Native Interface implementation by [Luben Karavelov](https://github.com/luben): [zstd-jni](https://github.com/luben/zstd-jni)

# Compression parameters

A value of `0` usually indicates the default according to the [Zstandard manual](https://facebook.github.io/zstd/zstd_manual.html).

## Standard parameters
* level (default: 3)- standard levels range from 1 to 22. Negative levels offer faster compression at the cost of compression ratio.
* nbWorkers - Number of worker threads to spawn
* windowLog - Maximum allowed back-reference distance, expressed as a power of 2

## Advanced Parameters
* hashLog - Size of the initial probe table, as a power of 2
* chainLog - Size of the multi-probe search table, as a power of 2
* searchLog - Number of search attempts, as a power of 2
* minMatch - Minimum size of searched matches
* targetLength - Impact of this field depends on strategy
* strategy - See ZSTD\_strategy enum definition
* jobSize - Size of a compression job. This value is enforced only when nbWorkers >= 1
* overlapLog - Control the overlap size, as a fraction of window size
* dict - Dictionary for compression

## zstd-jni Parameters
* [useChecksums](https://www.javadoc.io/static/com.github.luben/zstd-jni/1.5.5-10/com/github/luben/zstd/ZstdOutputStream.html#setChecksum(boolean) - Enable checksums for the compressed stream.
* [setCloseFrameOnFlush](https://www.javadoc.io/static/com.github.luben/zstd-jni/1.5.5-10/com/github/luben/zstd/ZstdOutputStream.html#setCloseFrameOnFlush(boolean) - Enable closing the frame on flush.

### zstd-jni documentation

https://www.javadoc.io/doc/com.github.luben/zstd-jni/latest/index.html

# Zstandard documentation

The Zstadard manual contains important details about using Zstandard.

https://facebook.github.io/zstd/zstd_manual.html
