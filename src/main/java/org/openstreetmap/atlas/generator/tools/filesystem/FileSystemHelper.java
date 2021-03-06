package org.openstreetmap.atlas.generator.tools.filesystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.streaming.resource.HDFSWalker;
import org.openstreetmap.atlas.streaming.compression.Compressor;
import org.openstreetmap.atlas.streaming.compression.Decompressor;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.InputStreamResourceCloseable;
import org.openstreetmap.atlas.streaming.resource.OutputStreamWritableResourceCloseable;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.streaming.resource.ResourceCloseable;
import org.openstreetmap.atlas.streaming.resource.WritableResource;
import org.openstreetmap.atlas.streaming.resource.WritableResourceCloseable;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class that helps generating {@link Resource} and {@link WritableResource} from a Hadoop
 * Path.
 *
 * @author matthieun
 * @author tony
 */
public final class FileSystemHelper
{
    public static final String FILE_NOT_FOUND = "File not found";

    private static final Map<String, String> DEFAULT = Maps.hashMap("fs.file.impl",
            RawLocalFileSystem.class.getCanonicalName());
    private static final Logger logger = LoggerFactory.getLogger(FileSystemHelper.class);

    private static final String UNABLE_TO_READ = "Unable to read {}";
    private static final String UNABLE_TO_OPEN = "Unable to open {}";
    private static final String FILESYSTEM_NOT_CLOSED = "FileSystem not properly closed";

    /**
     * Deletes given path using given configuration settings.
     *
     * @param path
     *            Path to delete
     * @param recursive
     *            If given path is a directory and this is set, then directory and all child items
     *            will be deleted, otherwise throws Exception.
     * @param configuration
     *            Configuration settings to use as context
     * @return true if deletion succeeded
     */
    public static boolean delete(final String path, final boolean recursive,
            final Map<String, String> configuration)
    {
        try (FileSystem fileSystem = new FileSystemCreator().get(path, configuration))
        {
            return fileSystem.delete(new Path(path), recursive);
        }
        catch (final Exception e)
        {
            throw new CoreException("Unable to delete {}", path, e);
        }
    }

    /**
     * Check if the given path exists.
     *
     * @param path
     *            The path to check
     * @return If the given path exists
     */
    public static boolean exists(final String path)
    {
        return exists(path, DEFAULT);
    }

    /**
     * Check if the given path exists.
     *
     * @param path
     *            The path to check
     * @param configuration
     *            The configuration map
     * @return If the given path exists
     */
    public static boolean exists(final String path, final Map<String, String> configuration)
    {
        try (FileSystem fileSystem = new FileSystemCreator().get(path, configuration))
        {
            return fileSystem.exists(new Path(path));
        }
        catch (final IOException exception)
        {
            throw new CoreException("Failed to determine existence of {}", path, exception);
        }
    }

    public static boolean isDirectory(final String path, final Map<String, String> configuration)
    {
        try (FileSystem fileSystem = new FileSystemCreator().get(path, configuration))
        {
            return fileSystem.isDirectory(new Path(path));
        }
        catch (final IOException exception)
        {
            throw new CoreException("Failed to determine existence of directory {}", path,
                    exception);
        }
    }

    public static boolean isFile(final String path, final Map<String, String> configuration)
    {
        try (FileSystem fileSystem = new FileSystemCreator().get(path, configuration))
        {
            return fileSystem.isFile(new Path(path));
        }
        catch (final IOException exception)
        {
            throw new CoreException("Failed to determine existence of file {}", path, exception);
        }
    }

    /**
     * @param directory
     *            The directory from which to recursively load files
     * @param configuration
     *            The configuration (containing the filesystem definition)
     * @param filter
     *            The path filter. If null, all the files will be returned.
     * @return a list of {@link Resource}s
     */
    public static List<Resource> listResourcesRecursively(final String directory,
            final Map<String, String> configuration, final PathFilter filter)
    {
        final List<Resource> resources = new ArrayList<>();

        try (FileSystem fileSystem = new FileSystemCreator().get(directory, configuration))
        {
            streamPathsRecursively(directory, configuration, filter)
                    .map(path -> getResource(fileSystem, path)).forEach(resources::add);
        }
        catch (final IOException e)
        {
            logger.error(FILESYSTEM_NOT_CLOSED, e);
        }

        return resources;
    }

    /**
     * Creates a new directory for given path using given configuration settings.
     *
     * @param path
     *            Path to use for directory creation operation
     * @param configuration
     *            Configuration settings to use as context
     * @return true if create operation succeeded
     */
    public static boolean mkdir(final String path, final Map<String, String> configuration)
    {
        try (FileSystem fileSystem = new FileSystemCreator().get(path, configuration))
        {
            return fileSystem.mkdirs(new Path(path));
        }
        catch (final Exception e)
        {
            throw new CoreException("Unable to mkdir {}", path, e);
        }
    }

    /**
     * Renames a source path to a destination path using the given configuration settings. This
     * assumes that the source and destination path file systems are the same. Hence, the source
     * path is used as a reference while initializing file system.
     *
     * @param sourcePath
     *            Path to rename from
     * @param destinationPath
     *            Path to rename to
     * @param configuration
     *            Configuration settings to use as context
     * @return true if rename operation succeeded
     */
    public static boolean rename(final String sourcePath, final String destinationPath,
            final Map<String, String> configuration)
    {
        try (FileSystem fileSystem = new FileSystemCreator().get(sourcePath, configuration))
        {
            return fileSystem.rename(new Path(sourcePath), new Path(destinationPath));
        }
        catch (final Exception e)
        {
            throw new CoreException("Unable to rename {} to {}", sourcePath, destinationPath, e);
        }
    }

    /**
     * @param path
     *            The path to create the resource from
     * @return A {@link Resource} coming from the default {@link RawLocalFileSystem}
     */
    public static Resource resource(final String path)
    {
        return resource(path, DEFAULT);
    }

    /**
     * This may return an {@link AutoCloseable} resource. This should be checked and closed when it
     * is no longer needed.
     *
     * @param path
     *            The path to create the resource from
     * @param configuration
     *            The configuration defining the {@link FileSystem}
     * @return A {@link Resource} coming from the appropriate {@link FileSystem}
     */
    public static ResourceCloseable resource(final String path,
            final Map<String, String> configuration)
    {
        final Path hadoopPath = new Path(path);
        // This is closed by the ResourceCloseable
        final FileSystem fileSystem = new FileSystemCreator().get(path, configuration);
        try
        {
            final InputStreamResourceCloseable resource = new InputStreamResourceCloseable(() ->
            {
                try
                {
                    return fileSystem.open(hadoopPath);
                }
                catch (final FileNotFoundException fileNotFoundException)
                {
                    throw new CoreException("Unable to open {}. {}", hadoopPath, FILE_NOT_FOUND,
                            fileNotFoundException);
                }
                catch (final Exception e)
                {
                    throw new CoreException(UNABLE_TO_OPEN, hadoopPath, e);
                }
            }, fileSystem);
            resource.withName(hadoopPath.getName());

            if (hadoopPath.getName().endsWith(FileSuffix.GZIP.toString()))
            {
                resource.setDecompressor(Decompressor.GZIP);
            }

            return resource;
        }
        catch (final Exception e)
        {
            throw new CoreException(UNABLE_TO_READ, hadoopPath, e);
        }
    }

    /**
     * List resources, but omit the hadoop "_SUCCESS" file.
     *
     * @param directory
     *            The directory from which to load files
     * @param configuration
     *            The configuration defining the {@link FileSystem}
     * @return a list of {@link Resource}s which contains all the files but the _SUCCESS file
     */
    public static List<Resource> resources(final String directory,
            final Map<String, String> configuration)
    {
        return resources(directory, configuration, path -> !path.getName().endsWith("_SUCCESS"));
    }

    /**
     * @param directory
     *            The directory from which to load files
     * @param configuration
     *            The configuration defining the {@link FileSystem}
     * @param filter
     *            The path filter. If null, all the files will be returned.
     * @return a list of {@link Resource}s
     */
    public static List<Resource> resources(final String directory,
            final Map<String, String> configuration, final PathFilter filter)
    {
        try (FileSystem fileSystem = new FileSystemCreator().get(directory, configuration))
        {
            final FileStatus[] fileStatusList = filter == null
                    ? fileSystem.listStatus(new Path(directory))
                    : fileSystem.listStatus(new Path(directory), filter);
            return Stream.of(fileStatusList).map(FileStatus::getPath)
                    .map(path -> getResource(fileSystem, path)).collect(Collectors.toList());
        }
        catch (final IOException e)
        {
            logger.error(FILESYSTEM_NOT_CLOSED, e);
        }
        catch (final Exception e)
        {
            throw new CoreException("Could not locate files on directory {}", directory, e);
        }

        return Collections.emptyList();
    }

    /**
     * @param directory
     *            The directory from which to recursively stream paths
     * @param configuration
     *            The configuration (containing the filesystem definition)
     * @param filter
     *            The path filter. If null, all the paths will be returned.
     * @return a stream of {@link Path}s
     */
    public static Stream<Path> streamPathsRecursively(final String directory,
            final Map<String, String> configuration, final PathFilter filter)
    {
        return streamPathsRecursively(directory, configuration, filter, HDFSWalker.WALK_ALL);
    }

    /**
     * @param directory
     *            The directory from which to recursively stream paths
     * @param configuration
     *            The configuration (containing the filesystem definition)
     * @param filter
     *            The path filter. If null, all the paths will be returned.
     * @param maxDepth
     *            The maximum depth to look for
     * @return a stream of {@link Path}s
     */
    public static Stream<Path> streamPathsRecursively(final String directory,
            final Map<String, String> configuration, final PathFilter filter, final int maxDepth)
    {
        try (FileSystem fileSystem = new FileSystemCreator().get(directory, configuration))
        {
            return new HDFSWalker(maxDepth).usingConfiguration(fileSystem.getConf())
                    .walk(new Path(directory))
                    .map(HDFSWalker.debug(path -> logger.trace("{}", path)))
                    .map(FileStatus::getPath).filter(path -> filter == null || filter.accept(path));
        }
        catch (final IOException e)
        {
            logger.error(FILESYSTEM_NOT_CLOSED, e);
        }
        return Stream.empty();
    }

    /**
     * @param path
     *            The path to create the resource from
     * @return A {@link WritableResource} coming from the default {@link RawLocalFileSystem}
     */
    public static WritableResource writableResource(final String path)
    {
        return writableResource(path, DEFAULT);
    }

    /**
     * @param path
     *            The path to create the resource from
     * @param configuration
     *            The configuration defining the {@link FileSystem}
     * @return A {@link WritableResource} coming from the appropriate {@link FileSystem}
     */
    public static WritableResourceCloseable writableResource(final String path,
            final Map<String, String> configuration)
    {
        final Path hadoopPath = new Path(path);
        // This is closed by the WriteableResourceCloseable
        final FileSystem fileSystem = new FileSystemCreator().get(path, configuration);
        try
        {
            // Closed by the WritableResourceCloseable
            final OutputStream out;
            try
            {
                out = fileSystem.create(hadoopPath);
            }
            catch (final Exception e)
            {
                throw new CoreException(UNABLE_TO_OPEN, hadoopPath, e);
            }

            final OutputStreamWritableResourceCloseable resource = new OutputStreamWritableResourceCloseable(
                    out, fileSystem, out);
            resource.setName(hadoopPath.getName());
            if (resource.isGzipped())
            {
                resource.setCompressor(Compressor.GZIP);
            }

            return resource;
        }
        catch (final Exception e)
        {
            throw new CoreException(UNABLE_TO_READ, hadoopPath, e);
        }
    }

    /**
     * Get a resource from a FileSystem and a Path in that FileSystem
     *
     * @param fileSystem
     *            The FileSystem with the resource
     * @param path
     *            The path to the resource on the FileSystem
     * @return An InputStream for the resource
     */
    private static InputStreamResource getResource(final FileSystem fileSystem, final Path path)
    {
        try
        {
            // This doesn't actually use an AutoCloseable resource in InputStreamResource
            final InputStreamResource resource = new InputStreamResource(() ->
            {
                try
                {
                    return fileSystem.open(path);
                }
                catch (final Exception e)
                {
                    throw new CoreException(UNABLE_TO_OPEN, path, e);
                }
            }).withName(path.getName());

            if (path.getName().endsWith(FileSuffix.GZIP.toString()))
            {
                resource.setDecompressor(Decompressor.GZIP);
            }

            return resource;
        }
        catch (final Exception e)
        {
            throw new CoreException(UNABLE_TO_READ, path, e);
        }
    }

    private FileSystemHelper()
    {
    }
}
