import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 * Author: baojianfeng
 * Date: 2018-09-30
 */
public class FileUtil {

    /**
     * delete the output directory in case it is existed
     * @param conf configuration
     * @param dirPath directory path
     * @throws Exception
     */
    public static void removeOutputDir(Configuration conf, String dirPath) throws Exception {
        Path targetPath = new Path(dirPath);
        FileSystem fs = targetPath.getFileSystem(conf);
        if (fs.exists(targetPath)) {
            boolean deleteRes = fs.delete(targetPath, true);
            if (deleteRes)
                System.out.println(targetPath + " deleted");
            else
                System.out.println(targetPath + " deletion failed");
        }
    }

    /**
     * print output file
     * @param conf configuration
     * @param filePath file path
     * @throws IOException
     */
    public static void printFileContent(Configuration conf, String filePath) throws IOException {
        InputStream in = null;
        Path file = new Path(filePath);
        FileSystem fs = file.getFileSystem(conf);
        try {
            in = fs.open(file);
            IOUtils.copyBytes(in, System.out, 4096, true);
        } finally {
            if (in != null) {
                IOUtils.closeStream(in);
            }
        }
    }
}

