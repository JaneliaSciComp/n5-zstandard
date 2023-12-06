package org.janelia.scicomp.n5.zstandard;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.janelia.saalfeldlab.n5.BlockReader;
import org.janelia.saalfeldlab.n5.BlockWriter;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.Compression.CompressionType;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;
import org.janelia.saalfeldlab.n5.DefaultBlockWriter;

/**
 * Zstandard compression for N5
 * 
 * Implementation wrapper around Apache Commons Compress
 * 
 * See the Zstandard manual for details on parameters.
 * https://facebook.github.io/zstd/zstd_manual.html
 * 
 * @author mkitti
 *
 */
@CompressionType("zstd")
public class ZstandardCompression implements DefaultBlockReader, DefaultBlockWriter, Compression {

	private static final long serialVersionUID = 8592416400988371189L;

	/**
	 * Compression level
	 * 
	 * Standard compression level is between 1 and 22
	 * Negative compression levels offer speed
	 * 
	 * Note that zarr-developers/numcodecs defaults to 1
	 * 
	 * Default: 3 (see ZSTD_CLEVEL_DEFAULT)
	 */
	@CompressionParameter
	private int level = 3;

	/**
	 * Default compression level from zstd.h
	 */
	public static final int ZSTD_CLEVEL_DEFAULT = 3;

	/**
	 * Create Zstandard compression with level equal to the constant ZSTD_CLEVEL_DEFAULT (value: {@value ZstdCompression#ZSTD_CLEVEL_DEFAULT})
	 */
	public ZstandardCompression() {
		this.level = ZSTD_CLEVEL_DEFAULT;
	}

	/**
	 * @param level The standard compression levels are normally between 1 to 22. Negative compression levels offer greater speed.
	 */
	public ZstandardCompression(int level) {
		this.level = level;
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
		ZstdCompressorOutputStream zstdOut = new ZstdCompressorOutputStream(out, level);
		return zstdOut;
	}

	@Override
	public InputStream getInputStream(InputStream in) throws IOException {
		ZstdCompressorInputStream zstdIn = new ZstdCompressorInputStream(in);
		return zstdIn;
	}

}

