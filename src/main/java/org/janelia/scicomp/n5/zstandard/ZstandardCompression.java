package org.janelia.scicomp.n5.zstandard;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import org.janelia.saalfeldlab.n5.BlockReader;
import org.janelia.saalfeldlab.n5.BlockWriter;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.Compression.CompressionType;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;
import org.janelia.saalfeldlab.n5.DefaultBlockWriter;

/**
 * @author mkitti
 *
 */
@CompressionType("zstd")
public class ZstandardCompression implements DefaultBlockReader, DefaultBlockWriter, Compression {
	private static final long serialVersionUID = 5811954066059985371L;
	
	@CompressionParameter
	private int level = 3;
	
	@CompressionParameter
	private int nbWorkers = 0;
	
	@CompressionParameter
	private int windowLog = 0;
	
	@CompressionParameter
	private int hashLog = 0;
	
	@CompressionParameter
	private int chainLog = 0;
	
	@CompressionParameter
	private int searchLog = 0;
	
	@CompressionParameter
	private int minMatch = 0;
	
	@CompressionParameter
	private int targetLength = 0;
	
	@CompressionParameter
	private int strategy = 0;

	@CompressionParameter
	private int jobSize = 0;
	
	@CompressionParameter
	private int overlapLog = 0;
	
	@CompressionParameter
	private boolean useChecksums = false;
	
	@CompressionParameter
	private boolean setCloseFrameOnFlush = false;
	
	@CompressionParameter
	private byte[] dict = null;
	
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
		ZstdOutputStream zstdOut = new ZstdOutputStream(out, level);
		// standard parameters
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
		return new ZstdInputStream(in);
	}

}
