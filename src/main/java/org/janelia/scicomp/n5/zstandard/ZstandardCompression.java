/*-
 * #%L
 * n5-zstandard
 * %%
 * Copyright (C) 2023 Howard Hughes Medical Institute
 * %%
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the HHMI nor the names of its contributors
 *    may be used to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.scicomp.n5.zstandard;

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.NoPool;
import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.Compression.CompressionType;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

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
public class ZstandardCompression implements Compression {
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

	/*
	 *
	 */
	private boolean advancedParameterSet = false;

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
		this.advancedParameterSet = true;
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
		this.advancedParameterSet = true;
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
		this.advancedParameterSet = true;
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
		this.advancedParameterSet = true;
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
		this.advancedParameterSet = true;
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
		this.advancedParameterSet = true;
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
		this.advancedParameterSet = true;
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
		this.advancedParameterSet = true;
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
		this.advancedParameterSet = true;
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
		this.advancedParameterSet = true;
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
		this.advancedParameterSet = true;
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
		this.advancedParameterSet = true;
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
		this.advancedParameterSet = true;
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
	public void useRecyclingBufferPool(boolean useRecycling) {
		if (useRecycling)
			bufferPool = RecyclingBufferPool.INSTANCE;
		else
			bufferPool = NoPool.INSTANCE;
	}

	/**
	 * Configure how buffers are recycled
	 */
	public void setBufferPool(BufferPool bufferPool) {
		this.bufferPool = bufferPool;
	}

	OutputStream getOutputStream(OutputStream out) throws IOException {
		ZstdOutputStream zstdOut = new ZstdOutputStream(out, bufferPool);
		// standard parameters
		if (level != 0)
			zstdOut.setLevel(level);

		if (advancedParameterSet) {
			if (nbWorkers != 0)
				zstdOut.setWorkers(nbWorkers);
			if (windowLog != 0)
				zstdOut.setLong(windowLog);

			// advanced parameters
			if (hashLog != 0)
				zstdOut.setHashLog(hashLog);
			if (chainLog != 0)
				zstdOut.setChainLog(chainLog);
			if (searchLog != 0)
				zstdOut.setSearchLog(searchLog);
			if (minMatch != 0)
				zstdOut.setMinMatch(minMatch);
			if (targetLength != 0)
				zstdOut.setTargetLength(targetLength);
			if (strategy != 0)
				zstdOut.setStrategy(strategy);
			if (jobSize != 0)
				zstdOut.setJobSize(jobSize);
			if (overlapLog != 0)
				zstdOut.setOverlapLog(overlapLog);

			// zstd-jni parameters
			if (useChecksums)
				zstdOut.setChecksum(useChecksums);
			if (setCloseFrameOnFlush)
				zstdOut.setCloseFrameOnFlush(setCloseFrameOnFlush);

			// dictionary
			if (dict != null)
				zstdOut.setDict(dict);
		}

		return zstdOut;
	}

	private InputStream getInputStream(InputStream in) throws IOException {
		ZstdInputStream zstdIn =  new ZstdInputStream(in, bufferPool);

		//is windowLog the same as windowLogMax?
		zstdIn.setLongMax(windowLog);

		if (dict != null) {
			zstdIn.setDict(dict);
		}

		return zstdIn;
	}

	@Override
	public ReadData decode(final ReadData readData, final int decodedLength) throws IOException {
		final InputStream inflater = getInputStream(readData.inputStream());
		return ReadData.from(inflater, decodedLength);
	}

	/*
	 * We override encode in order to use zstd's buffer API. In doing so, we
	 * include the size of the dataBlock in the frame header. This allows
	 * decompression software to determine the output buffer length via
	 * ZSTD_getFrameContentSize.
	 *
	 * As of December 2023, zarr-developers/numcodecs contained a bug where the
	 * deprecated function ZSTD_getDecompressedSize was incorrectly interpreted
	 * as returning an error when it was indicating an unknown size value.
	 * https://github.com/zarr-developers/numcodecs/issues/499
	 * This addresses the issue by including the size in the frame header.
	 *
	 * An alternate approach would be to use ZSTD_CCtx_setPledgedSrcSize with
	 * the streaming API.
	 */
	@Override
	public ReadData encode(final ReadData readData) throws IOException {

		//consider reusing this context
		ZstdCompressCtx ctx = new ZstdCompressCtx();
		try {
			ctx.setLevel(level);

			if(advancedParameterSet) {
				if (chainLog != 0)
					ctx.setChainLog(chainLog);
				if (useChecksums)
					ctx.setChecksum(useChecksums);
				if (hashLog != 0)
					ctx.setHashLog(hashLog);
				if (jobSize != 0)
					ctx.setJobSize(jobSize);
				if (windowLog != 0)
					ctx.setLong(windowLog);
				if (minMatch != 0)
					ctx.setMinMatch(minMatch);
				if (overlapLog != 0)
					ctx.setOverlapLog(overlapLog);
				if (searchLog != 0)
					ctx.setSearchLog(searchLog);
				if (strategy != 0)
					ctx.setStrategy(strategy);
				if (targetLength != 0)
					ctx.setTargetLength(targetLength);
				if (windowLog != 0)
					ctx.setWindowLog(windowLog);
				if (nbWorkers != 0)
					ctx.setWorkers(nbWorkers);
			}

			//compress does accept a ByteBuffer but it must be direct
			final byte[] outputBuffer = ctx.compress(readData.allBytes());
			return ReadData.from(outputBuffer);

		} finally {
			ctx.close();
		}
	}

}
