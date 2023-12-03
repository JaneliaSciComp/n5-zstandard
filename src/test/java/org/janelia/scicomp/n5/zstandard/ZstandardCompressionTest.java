package org.janelia.scicomp.n5.zstandard;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Map;

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
import org.junit.function.ThrowingRunnable;

import com.google.gson.GsonBuilder;

/**
 * Lazy {@link ZstandardCompression} test using the abstract base class.
 *
 * @author Mark Kittisopikul &lt;kittisopikulm@janelia.hhmi.org&gt;
 */
public class ZstandardCompressionTest extends AbstractN5Test {

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

}
