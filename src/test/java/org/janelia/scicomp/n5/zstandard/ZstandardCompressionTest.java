/*-
 * #%L
 * n5-zstandard
 * %%
 * Copyright (C) 2023 Howard Hughes Medical Institute
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of HHMI nor the names of its contributors may be used to
 *    endorse or promote products derived from this software without specific
 *    prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.scicomp.n5.zstandard;

import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;

import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.github.luben.zstd.Zstd;
import com.google.gson.GsonBuilder;

/**
 * Lazy {@link ZstandardCompression} test using the abstract base class.
 *
 * @author Mark Kittisopikul &lt;kittisopikulm@janelia.hhmi.org&gt;
 */
@RunWith(Parameterized.class)
public class ZstandardCompressionTest extends AbstractN5Test {
	
    @Parameters
    public static Collection<Object[]> data() {
    	int m = Zstd.minCompressionLevel();
    	int M = Zstd.maxCompressionLevel();
    	System.out.println("Minimum compression level: " + m);
    	System.out.println("Maximum compression level: " + M);
    	//Comments below are with 16 MiB of int16 255 +/- 32;
		return Arrays.asList(new Object[][] {
			{ m,0}, { m,2}, { m,4}, { m,8}, // test  0 -  3, 100%,  3.5s, 1 thread
			{-2,0}, {-2,2}, {-2,4}, {-2,8}, // test  4 -  7,  97%,  4.5s, 1 thread
			{-1,0}, {-1,2}, {-1,4}, {-1,8}, // test  8 - 11,  96%,  5.0s, 8 threads
			//Compression level 0 means default (3)
			{ 1,0}, { 1,2}, { 1,4}, { 1,8}, // test 12 - 15,  55%,  5.5s, 8 threads
			{ 2,0}, { 2,2}, { 2,4}, { 2,8}, // test 16 - 19,  53%,  8.1s, 4 threads
			{ 3,0}, { 3,2}, { 3,4}, { 3,8}, // test 20 - 23,  51%, 14.0s, 4 threads
			{ 6,0}, { 6,2}, { 6,4}, { 6,8}, // test 24 - 27,  49%, 25.5s, 2 threads
			//{12,0}, {12,1}, {12,2}, {12,4}, // test 28 - 31
			//{15,0}, {15,1}, {15,2}, {15,4}, // test 32 - 35
			//{ M,8}, { M,1}, { M,2}, { M,4}  // test 36 - 39
		});
    }
    
    private int level = 0;
    private int nbWorkers = 0;
    private int megabytesToCompress = 16;
    
    public ZstandardCompressionTest(int level, int nbWorkers) {
    	this.level = level;
    	this.nbWorkers = nbWorkers;
    }


	private static String testDirPath = createTestDirPath("n5-test");

	private static String createTestDirPath(final String dirName) {
		try {
			return Files.createTempDirectory(dirName).toString();
		} catch (final IOException exc) {
			return System.getProperty("user.home") + "/tmp/" + dirName;
		}
	}

	@Override
	protected N5Reader createN5Reader(final String location, final GsonBuilder gson) throws IOException, URISyntaxException {

		return new N5FSReader(location, gson);
	}

	@Override
	protected N5Writer createN5Writer(final String location, final GsonBuilder gson) throws IOException, URISyntaxException {

		return new N5FSWriter(location, gson);
	}

	@Override
	protected String tempN5Location() throws URISyntaxException {

		final String basePath = new File(tempN5PathName()).toURI().normalize().getPath();
		return new URI("file", null, basePath, null).toString();
	}

	private static String tempN5PathName() {

		try {
			final File tmpFile = Files.createTempDirectory("n5-test-").toFile();
			tmpFile.deleteOnExit();
			return tmpFile.getCanonicalPath();
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override protected N5Writer createN5Writer() throws IOException, URISyntaxException {

		return new N5FSWriter(tempN5Location(), new GsonBuilder()) {
			@Override public void close() {

				super.close();
				remove();
			}
		};
	}

	@Override
	protected Compression[] getCompressions() {
		Compression[] compressions = {new ZstandardCompression()};
		return compressions;
	}

	@Test
	public void testDefaultNbWorkers() throws IOException, URISyntaxException {

		final String zstdDatasetName = datasetName + "-zstdworkerstest";
		try (N5Writer n5 = createN5Writer()) {

			ZstandardCompression compressor = new ZstandardCompression(3);
			n5.createDataset( zstdDatasetName, dimensions, blockSize, DataType.UINT64, compressor);

			if (!n5.exists(zstdDatasetName))
				fail("Dataset does not exist");

			try {
				final DatasetAttributes info = n5.getDatasetAttributes(zstdDatasetName);
				Assert.assertArrayEquals(dimensions, info.getDimensions());
				Assert.assertArrayEquals(blockSize, info.getBlockSize());
				Assert.assertEquals(DataType.UINT64, info.getDataType());
				Assert.assertEquals(ZstandardCompression.class, info.getCompression().getClass());

				@SuppressWarnings("unchecked")
				final Map<String, Object> map = n5.getAttribute(zstdDatasetName, "compression", Map.class);
				//kittisopikulm: nbWorkers is not a compression parameter, so we cannot read it back
				//Assert.assertEquals(10, ((Double) map.get("nbWorkers")).intValue());
				Field nbWorkersField = ZstandardCompression.class.getDeclaredField("nbWorkers");
				nbWorkersField.setAccessible(true);
				//Assert.assertEquals(10, nbWorkersField.get(info.getCompression()));

				map.remove("nbWorkers");
				map.put("level", ((Double) map.get("level")).intValue());
				n5.setAttribute(zstdDatasetName, "compression", map);

				final DatasetAttributes info2 = n5.getDatasetAttributes(zstdDatasetName);
				Assert.assertArrayEquals(dimensions, info2.getDimensions());
				Assert.assertArrayEquals(blockSize, info2.getBlockSize());
				Assert.assertEquals(DataType.UINT64, info2.getDataType());
				Assert.assertEquals(ZstandardCompression.class, info2.getCompression().getClass());
				nbWorkersField = ZstandardCompression.class.getDeclaredField("nbWorkers");
				nbWorkersField.setAccessible(true);
				Assert.assertEquals(0, nbWorkersField.get(info2.getCompression()));

			} catch (final IllegalAccessException | IllegalArgumentException | NoSuchFieldException e) {
				fail("Cannot access nbWorkers field");
				e.printStackTrace();
			}

		}
	}
	
	@Test
	public void testManyOutputStreams() throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024*1024*2*megabytesToCompress/2);
		DataOutputStream data = new DataOutputStream(bytes);
		Random rand = new Random(1230);
		//rand.nextBytes(data);
		for(int i=0; i< 1024*1024*megabytesToCompress/2; ++i) {
			data.writeShort((short) (rand.nextGaussian() * Short.MAX_VALUE/2048.0 + 255));
		}
		byte[] byteArray = bytes.toByteArray();
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();;
		OutputStream zstdOut;
		ZstandardCompression compressor = new ZstandardCompression(this.level);
		compressor.setNbWorkers(this.nbWorkers);
		for(int i=0; i < 100; ++i) {
			out.reset();
			zstdOut = compressor.getOutputStream(out);
			zstdOut.write(byteArray);
			zstdOut.close();
		}
		System.out.println("Compression level: " + this.level + ", Threads: " + this.nbWorkers +
				", Ratio: " + out.size() + " / " + byteArray.length + 
				" (" + (float) out.size()/byteArray.length*100 + "%)");
	}
}
