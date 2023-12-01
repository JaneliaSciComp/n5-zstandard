package org.janelia.scicomp.n5.zstandard;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.NoPool;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import org.janelia.saalfeldlab.n5.BlockReader;
import org.janelia.saalfeldlab.n5.BlockWriter;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.Compression.CompressionType;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;
import org.janelia.saalfeldlab.n5.DefaultBlockWriter;

/**
 * Zstandard compression for N5
 * 
 * Implementation wrapper around zstd-jni
 * https://github.com/luben/zstd-jni
 * 
 * See the Zstandard manual for details on parameters.
 * https://facebook.github.io/zstd/zstd_manual.html
 * 
 * @author mkitti
 *
 */
@CompressionType("zstd")
public class ZstandardCompression implements DefaultBlockReader, DefaultBlockWriter, Compression {
	private static final long serialVersionUID = 5811954066059985371L;
	
	/*
	 * Compression level
	 * 
	 * Standard compression level is between 1 and 22
	 * Negative compression levels offer speed
	 * 
	 * Default: 3
	 */
	@CompressionParameter
	private int level = 3;
	
	/*
	 * Number of Worker Threads to spawn
	 * 
	 * Default: 0 (do not spawn any workers)
	 */
	@CompressionParameter
	private int nbWorkers = 0;
	
	/*
	 * Maximum allowed back-reference distance, expressed as a power of 2
	 * 
	 * Default: 0	
	 */
	@CompressionParameter
	private int windowLog = 0;
	
	/*
	 * Size of the initial probe table, as a power of 2
	 * 
	 * Default: 0
	 */
	@CompressionParameter
	private int hashLog = 0;

	/*
	 * Size of the multi-probe search table, as a power of 2
	 * 
	 * Default: 0
	 */
	@CompressionParameter
	private int chainLog = 0;

	/*
	 * Number of search attempts, as a power of 2
	 * 
	 * Default: 0
	 */
	@CompressionParameter
	private int searchLog = 0;

	/*
	 * Minimum size of searched matches
	 * 
	 * Default: 0
	 */
	@CompressionParameter
	private int minMatch = 0;

	/*
	 * Impact of this field depends on strategy
	 * 
	 * Default: 0
	 */
	@CompressionParameter
	private int targetLength = 0;
	
	/*
	 * See ZSTD_strategy enum definition
	 * 
	 * Default: 0
	 */
	@CompressionParameter
	private int strategy = 0;
	
	/*
	 * Size of a compression job. This value is enforced only when nbWorkers >= 1
	 * 
	 * Default: 0
	 */
	@CompressionParameter
	private int jobSize = 0;
	
	/*
	 * Control the overlap size, as a fraction of window size
	 * 
	 * Default: 0
	 */
	@CompressionParameter
	private int overlapLog = 0;
	
	/*
	 *  Enable checksums for the compressed stream
	 * 
	 * Default: false
	 */	
	@CompressionParameter
	private boolean useChecksums = false;
	
	/*
	 * Enable closing the frame on flush.
	 * 
	 * Default: false
	 */
	@CompressionParameter
	private boolean setCloseFrameOnFlush = false;
	
	/*
	 * Dictionary for compression as a byte array
	 * 
	 * Default: null
	 */
	@CompressionParameter
	private byte[] dict = null;
	
	/*
	 * Configure how buffers are recycled
	 */
	private BufferPool bufferPool = NoPool.INSTANCE;
	
	/*
	 * Sets the following default parameters
	 * level: 3
	 * nbWorkers: 0
	 * windowLog: 0
	 * 
	 * Note that this uses a default compression level of 3 matching the C library.
	 * Some libraries such as numcodecs use a default compression level of 1.
	 */
	public ZstandardCompression() {
		// C library uses compression level 3 as standard
		// numcodecs uses compression level 1 as standard
		this(3);
	}

	public ZstandardCompression(int level) {
		this(level, 0, 0);
	}
	
	public ZstandardCompression(int level, int nbWorkers) {
		this(level, nbWorkers, 0);
	}
	
	/**
	 * @param level -    Compression level (default: 3)
	 * @param nbWorkers  Number of worker threads (default: 0)
	 * @param windowLog  Maximum allowed back-reference distance, expressed as a power of 2
	 */
	public ZstandardCompression(int level, int nbWorkers, int windowLog) {
		this.level = level;
		this.nbWorkers = nbWorkers;
		this.windowLog = windowLog;
	}
	
	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public int getNbWorkers() {
		return nbWorkers;
	}

	public void setNbWorkers(int nbWorkers) {
		this.nbWorkers = nbWorkers;
	}

	public int getWindowLog() {
		return windowLog;
	}

	public void setWindowLog(int windowLog) {
		this.windowLog = windowLog;
	}
	
	public int getHashLog() {
		return hashLog;
	}

	public void setHashLog(int hashLog) {
		this.hashLog = hashLog;
	}

	public int getChainLog() {
		return chainLog;
	}

	public void setChainLog(int chainLog) {
		this.chainLog = chainLog;
	}

	public int getSearchLog() {
		return searchLog;
	}

	public void setSearchLog(int searchLog) {
		this.searchLog = searchLog;
	}

	public int getMinMatch() {
		return minMatch;
	}

	public void setMinMatch(int minMatch) {
		this.minMatch = minMatch;
	}

	public int getTargetLength() {
		return targetLength;
	}

	public void setTargetLength(int targetLength) {
		this.targetLength = targetLength;
	}

	public int getStrategy() {
		return strategy;
	}

	public void setStrategy(int strategy) {
		this.strategy = strategy;
	}

	public int getJobSize() {
		return jobSize;
	}

	public void setJobSize(int jobSize) {
		this.jobSize = jobSize;
	}

	public int getOverlapLog() {
		return overlapLog;
	}

	public void setOverlapLog(int overlapLog) {
		this.overlapLog = overlapLog;
	}

	public boolean isUseChecksums() {
		return useChecksums;
	}

	public void setUseChecksums(boolean useChecksums) {
		this.useChecksums = useChecksums;
	}

	public boolean isSetCloseFrameOnFlush() {
		return setCloseFrameOnFlush;
	}

	public void setSetCloseFrameOnFlush(boolean setCloseFrameOnFlush) {
		this.setCloseFrameOnFlush = setCloseFrameOnFlush;
	}

	public byte[] getDict() {
		return dict;
	}

	public void setDict(byte[] dict) {
		this.dict = dict;
	}
	
	public BufferPool getBufferPool() {
		return bufferPool;
	}

	public void setBufferPool(BufferPool bufferPool) {
		this.bufferPool = bufferPool;
	}


	@Override
	public BlockReader getReader() {
		return this;
	}

	@Override
	public BlockWriter getWriter() {
		return this;
	}

	@Override
	public OutputStream getOutputStream(OutputStream out) throws IOException {
		ZstdOutputStream zstdOut = new ZstdOutputStream(out, bufferPool);
		// standard parameters
		zstdOut.setLevel(level);
		zstdOut.setWorkers(nbWorkers);
		zstdOut.setLong(windowLog);
		
		// advanced parameters
		zstdOut.setHashLog(hashLog);
		zstdOut.setChainLog(chainLog);
		zstdOut.setSearchLog(searchLog);
		zstdOut.setMinMatch(minMatch);
		zstdOut.setTargetLength(targetLength);
		zstdOut.setStrategy(strategy);
		zstdOut.setJobSize(jobSize);
		zstdOut.setOverlapLog(overlapLog);
		
		// zstd-jni parameters
		zstdOut.setChecksum(useChecksums);
		zstdOut.setCloseFrameOnFlush(setCloseFrameOnFlush);
		
		// dictionary
		if (dict != null) {
			zstdOut.setDict(dict);
		}
		
		return zstdOut;
	}

	@Override
	public InputStream getInputStream(InputStream in) throws IOException {
		return new ZstdInputStream(in, bufferPool);
	}

}
