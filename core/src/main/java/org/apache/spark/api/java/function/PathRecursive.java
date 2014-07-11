package org.apache.spark.api.java.function;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.ArrayList;

public class PathRecursive{
    public Path[] getRecursivePaths(FileSystem fs, String basePath)
        throws IOException, URISyntaxException {
        List<Path> result = new ArrayList<Path>();
        basePath = fs.getUri() + basePath;
        FileStatus[] listStatus = fs.globStatus(new Path(basePath+"/*"));
        for (FileStatus fstat : listStatus) {
            readSubDirectory(fstat, basePath, fs, result);
        }
        return (Path[]) result.toArray(new Path[result.size()]);
    }

    private void readSubDirectory(FileStatus fileStatus, String basePath,
        FileSystem fs, List<Path> paths) throws IOException, URISyntaxException {
        if (fileStatus.isDir()) {
            paths.add(fileStatus.getPath());
            String subPath = fileStatus.getPath().toString();
            FileStatus[] listStatus = fs.globStatus(new Path(subPath + "/*"));
            if (listStatus.length != 0) {
                for (FileStatus fst : listStatus ){
                    readSubDirectory(fst, subPath, fs, paths);
                }
            }
        }
    }
}
