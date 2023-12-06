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
 * Implementation wrapper around <a href="https://github.com/luben/zstd-jni">zstd-jni</a>.
 * 
 * See the <a href="https://facebook.github.io/zstd/zstd_manual.html">Zstandard manual</a> for details on parameters.
 * 
 * 
 * @author mkitti
 *
 */
@CompressionType("zstd")
public class ZstandardCompression implements DefaultBlockReader, DefaultBlockWriter, Compression {
	private static final long serialVersionUID = 5811954066059985371L;
	
	/*
	 * Default compression level from zstd.h
	 */
	public static final int ZSTD_CLEVEL_DEFAULT = 3;
	
	/*
	 * Compression level
	 * 
	 * Standard compression level is between 1 and 22
	 * Negative compression levels offer speed
	 * 
	 * Default: 3
	 */
	@CompressionParameter
	private int level = ZSTD_CLEVEL_DEFAULT;
	
	/*
	 * Number of Worker Threads to spawn
	 * 
	 * Default: 0 (do not spawn any workers)
	 */
	private int nbWorkers = 0;
	
	/*
	 * Maximum allowed back-reference distance, expressed as a power of 2
	 * 
	 * Default: 0	
	 */
	private int windowLog = 0;
	
	/*
	 * Size of the initial probe table, as a power of 2
	 * 
	 * Default: 0
	 */
	private int hashLog = 0;

	/*
	 * Size of the multi-probe search table, as a power of 2
	 * 
	 * Default: 0
	 */
	private int chainLog = 0;

	/*
	 * Number of search attempts, as a power of 2
	 * 
	 * Default: 0
	 */
	private int searchLog = 0;

	/*
	 * Minimum size of searched matches
	 * 
	 * Default: 0
	 */
	private int minMatch = 0;

	/*
	 * Impact of this field depends on strategy
	 * 
	 * Default: 0
	 */
	private int targetLength = 0;
	
	/*
	 * See ZSTD_strategy enum definition
	 * 
	 * Default: 0
	 */
	private int strategy = 0;
	
	/*
	 * Size of a compression job. This value is enforced only when nbWorkers >= 1
	 * 
	 * Default: 0
	 */
	private int jobSize = 0;
	
	/*
	 * Control the overlap size, as a fraction of window size
	 * 
	 * Default: 0
	 */
	private int overlapLog = 0;
	
	/*
	 *  Enable checksums for the compressed stream
	 * 
	 * Default: false
	 */	
	private boolean useChecksums = false;
	
	/*
	 * Enable closing the frame on flush.
	 * 
	 * Default: false
	 */
	private boolean setCloseFrameOnFlush = false;
	
	/*
	 * Dictionary for compression as a byte array
	 * 
	 * Default: null
	 */
	private byte[] dict = null;
	
	/*
	 * Configure how buffers are recycled
	 */
	private BufferPool bufferPool = NoPool.INSTANCE;

	
	/**
	 * Create ZstandardCompression with level equal to the constant ZSTD_CLEVEL_DEFAULT (value: {@value ZstandardCompression#ZSTD_CLEVEL_DEFAULT})
	 *
	 * Note that this uses a default compression level of 3 matching the C library.
	 * Some libraries such as numcodecs use a default compression level of 1.
	 */
	public ZstandardCompression() {
		// C library uses compression level 3 as standard
		// numcodecs uses compression level 1 as standard
		this(ZSTD_CLEVEL_DEFAULT);
	}

	/**
	 * Create ZstandardCompression with the specified compression level.
	 * 
	 * @param level The standard compression levels are normally between 1 to 22. Negative compression levels offer greater speed.
	 *              The default value is  {@value ZstandardCompression#ZSTD_CLEVEL_DEFAULT}.
	 */
	public ZstandardCompression(int level) {
		this.level = level;
	}
	
	/*
	 * Get the compression level
	 */
	public int getLevel() {
		return level;
	}

	/**
	 * Set the compression level
	 * 
	 * The standard compression levels are normally between 1 to 22. Negative compression levels offer greater speed.
	 */
	public void setLevel(int level) {
		this.level = level;
	}

	/**
	 * Get the Number of Worker Threads to spawn
	 * 
	 * Default: 0 (do not spawn any workers)
	 */
	public int getNbWorkers() {
		return nbWorkers;
	}
	
	/**
	 * Set the Number of Worker Threads to spawn
	 * 
	 * Default: 0 (do not spawn any workers)
	 */
	public void setNbWorkers(int nbWorkers) {
		this.nbWorkers = nbWorkers;
	}

	/**
	 * Get the maximum allowed back-reference distance, expressed as a power of 2
	 * 
	 * Default: 0	
	 */
	public int getWindowLog() {
		return windowLog;
	}

	/**
	 * Set the maximum allowed back-reference distance, expressed as a power of 2
	 * 
	 * Default: 0	
	 */
	public void setWindowLog(int windowLog) {
		this.windowLog = windowLog;
	}
	
	/**
	 * Set the size of the initial probe table, as a power of 2
	 * 
	 * Default: 0
	 */
	public int getHashLog() {
		return hashLog;
	}

	/**
	 * Get the size of the initial probe table, as a power of 2
	 * 
	 * Default: 0
	 */
	public void setHashLog(int hashLog) {
		this.hashLog = hashLog;
	}

	/**
	 * Get the size of the multi-probe search table, as a power of 2
	 * 
	 * Default: 0
	 */
	public int getChainLog() {
		return chainLog;
	}

	/**
	 * Set the size of the multi-probe search table, as a power of 2
	 * 
	 * Default: 0
	 */
	public void setChainLog(int chainLog) {
		this.chainLog = chainLog;
	}

	/**
	 * Get the number of search attempts, as a power of 2
	 * 
	 * Default: 0
	 */
	public int getSearchLog() {
		return searchLog;
	}

	/**
	 * Set the number of search attempts, as a power of 2
	 * 
	 * Default: 0
	 */
	public void setSearchLog(int searchLog) {
		this.searchLog = searchLog;
	}

	/**
	 * Get the minimum size of searched matches
	 * 
	 * Default: 0
	 */
	public int getMinMatch() {
		return minMatch;
	}

	/**
	 * Set the minimum size of searched matches
	 * 
	 * Default: 0
	 */
	public void setMinMatch(int minMatch) {
		this.minMatch = minMatch;
	}

	/**
	 * Impact of this field depends on strategy
	 * 
	 * Default: 0
	 */
	public int getTargetLength() {
		return targetLength;
	}

	/**
	 * Impact of setting this field depends on strategy
	 * 
	 * Default: 0
	 */
	public void setTargetLength(int targetLength) {
		this.targetLength = targetLength;
	}

	/**
	 * See ZSTD_strategy enum definition
	 * 
	 * Default: 0
	 */
	public int getStrategy() {
		return strategy;
	}

	/**
	 * See ZSTD_strategy enum definition
	 * 
	 * Default: 0
	 */
	public void setStrategy(int strategy) {
		this.strategy = strategy;
	}

	/**
	 * Get the size of a compression job. This value is enforced only when {@code nbWorkers >= 1}
	 * 
	 * Default: 0
	 */
	public int getJobSize() {
		return jobSize;
	}

	/**
	 * Set the size of a compression job. This value is enforced only when {@code nbWorkers >= 1}
	 * 
	 * Default: 0
	 */
	public void setJobSize(int jobSize) {
		this.jobSize = jobSize;
	}

	/**
	 * Get the overlap size, as a fraction of window size
	 * 
	 * Default: 0
	 */
	public int getOverlapLog() {
		return overlapLog;
	}

	/**
	 * Set the overlap size, as a fraction of window size
	 * 
	 * Default: 0
	 */
	public void setOverlapLog(int overlapLog) {
		this.overlapLog = overlapLog;
	}

	/**
	 *  Check if checksums are used for the compressed stream
	 * 
	 * Default: false
	 */	
	public boolean isUseChecksums() {
		return useChecksums;
	}

	/**
	 *  Enable or disable checksums for the compressed stream
	 * 
	 * Default: false (disabled)
	 */	
	public void setUseChecksums(boolean useChecksums) {
		this.useChecksums = useChecksums;
	}

	/**
	 * Check if closing the frame on flush is enabled..
	 * 
	 * Default: false
	 */
	public boolean isSetCloseFrameOnFlush() {
		return setCloseFrameOnFlush;
	}

	/**
	 * Enable or disable closing the frame on flush.
	 * 
	 * Default: false
	 */
	public void setSetCloseFrameOnFlush(boolean setCloseFrameOnFlush) {
		this.setCloseFrameOnFlush = setCloseFrameOnFlush;
	}

	/**
	 * Get the dictionary for compression as a byte array
	 * 
	 * Default: null (no dictionary)
	 */
	public byte[] getDict() {
		return dict;
	}

	/**
	 * Set the dictionary for compression as a byte array
	 * 
	 * Default: null (no dictionary)
	 */
	public void setDict(byte[] dict) {
		this.dict = dict;
	}
	
	/**
	 * Get how buffers are recycled
	 */
	public BufferPool getBufferPool() {
		return bufferPool;
	}

	/**
	 * Configure how buffers are recycled
	 */
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
		ZstdInputStream zstdIn =  new ZstdInputStream(in, bufferPool);
		
		//is windowLog the same as windowLogMax?
		zstdIn.setLongMax(windowLog);
		
		if (dict != null) {
			zstdIn.setDict(dict);
		}
		
		return zstdIn;
	}

}
