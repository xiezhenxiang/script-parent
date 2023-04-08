package ai.plantdata.script.util.other;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @author xiezhenxiang 2019/12/20
 */
public class FileUtil {

    private static final char UNIX_SEPARATOR = '/';
    private static final char WINDOWS_SEPARATOR = '\\';
    private static final char SYSTEM_SEPARATOR = File.separatorChar;

    /**
     * 把文件压缩成zip或rar，调用后要关闭流
     * @author xiezhenxiang 2019/12/20
     * @param sourceFile sourceFile
     * @param zos zipOutPutStream
     * @param rootDir 自定义zip内的根目录，可以不传
     **/
    public static void compressToZip(File sourceFile, ZipOutputStream zos, String rootDir) {

        byte[] buf = new byte[1024 * 1024];
        String entryName = sourceFile.getName();
        if (StringUtils.isNotBlank(rootDir)) {
            entryName = rootDir + "/" + entryName;
        }
        try {
            if (sourceFile.isFile()) {
                zos.putNextEntry(new ZipEntry(entryName));
                int len;
                FileInputStream in = new FileInputStream(sourceFile);
                while ((len = in.read(buf)) != -1) {
                    zos.write(buf, 0, len);
                }
                zos.closeEntry();
                in.close();
            } else {
                File[] listFiles = sourceFile.listFiles();
                if (listFiles == null || listFiles.length == 0) {
                    zos.putNextEntry(new ZipEntry(entryName + "/"));
                    zos.closeEntry();
                } else {
                    for (File file : listFiles) {
                        compressToZip(file, zos, entryName);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException {
        decompressZip(new File("C:\\Users\\14307\\Desktop\\qb测试\\test.zip"), "C:\\Users\\14307\\Desktop\\zip");
    }


    /**
     * 解压文件
     * @author xiezhenxiang 2019/12/21
     **/
    public static void decompressZip(File inputFile, String outputDir) {
        if (outputDir.endsWith("/")) {
            outputDir = outputDir.substring(0, outputDir.length() - 1);
        }
        File outDirFile = new File(outputDir);
        if(!outDirFile.exists()){
            outDirFile.mkdirs();
        }
        try {
            ZipArchiveInputStream archiveInputStream = new ZipArchiveInputStream(new FileInputStream(inputFile));
            ArchiveEntry entry;
            while ((entry = archiveInputStream.getNextEntry()) != null) {
                if (entry.getName().startsWith("__MACOSX")) {
                    // 兼容mac系统
                    continue;
                }
                if (entry.isDirectory()) {
                    String dirPath = outputDir + "/" + entry.getName();
                    File dir = new File(dirPath);
                    dir.mkdirs();
                } else {
                    File targetFile = new File(outputDir + "/" + entry.getName());
                    FileOutputStream fos = new FileOutputStream(targetFile);
                    int len;
                    byte[] buf = new byte[1024 * 1024];
                    while ((len = archiveInputStream.read(buf)) != -1) {
                        fos.write(buf, 0, len);
                    }
                    fos.close();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void forceDeleteDir(File dir) {

        if (null != dir && dir.exists() && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (null != files) {
                for (File file : files) {
                    forceDeleteFile(file);
                }
            }
            dir.delete();
        }
    }

    public static void forceDeleteFile(File file) {
        if (null != file && file.exists() && file.isFile()) {
            boolean result = file.delete();
            int tryCount = 0;
            while (!result && tryCount ++ < 10) {
                System.gc();
                result = file.delete();
            }
        }
    }

    public static void forceMkdir(File directory) throws IOException {
        if (directory.exists()) {
            if (!directory.isDirectory()) {
                String message =
                        "File "
                                + directory
                                + " exists and is "
                                + "not a directory. Unable to create directory.";
                throw new IOException(message);
            }
        } else {
            if (!directory.mkdirs()) {
                // Double-check that some other thread or process hasn't made
                // the directory in the background
                if (!directory.isDirectory())
                {
                    String message =
                            "Unable to create directory " + directory;
                    throw new IOException(message);
                }
            }
        }
    }

    /**
     * 获取文件的编码格式
     */
    public static String getFileCharsetName(File file) throws IOException {
        InputStream inputStream = new FileInputStream(file);
        byte[] head = new byte[3];
        inputStream.read(head);
        // GBK或GB2312，即ANSI
        String charsetName = "GBK";
        if (head[0] == -1 && head[1] == -2) {
            charsetName = "UTF-16";
        } else if (head[0] == -2 && head[1] == -1 ) {
            // 包含两种编码格式：UCS2-Big-Endian和UCS2-Little-Endian
            charsetName = "Unicode";
        } else if(head[0] == -27 && head[1]== -101 && head[2] == -98) {
            // UTF-8(不含BOM)
            charsetName = "UTF-8";
        } else if(head[0] == -17 && head[1] == -69 && head[2] == -65) {
            // UTF-8-BOM
            charsetName = "UTF-8";
        }
        inputStream.close();
        return charsetName;
    }

    public static void forceDelete(File file) throws IOException {
        if (!file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            deleteDirectory(file);
        } else {
            if (!file.delete()) {
                String message = "Unable to delete file: " + file;
                throw new IOException(message);
            }
        }
    }

    public static void forceDeleteOnExit(File file) throws IOException {
        if (file.isDirectory()) {
            deleteDirectoryOnExit(file);
        } else {
            file.deleteOnExit();
        }
    }

    private static void deleteDirectoryOnExit(File directory) throws IOException {
        if (!directory.exists()) {
            return;
        }

        directory.deleteOnExit();
        if (!isSymlink(directory)) {
            cleanDirectoryOnExit(directory);
        }
    }

    private static void cleanDirectoryOnExit(File directory) throws IOException {
        if (!directory.exists()) {
            String message = directory + " does not exist";
            throw new IllegalArgumentException(message);
        }

        if (!directory.isDirectory()) {
            String message = directory + " is not a directory";
            throw new IllegalArgumentException(message);
        }

        File[] files = directory.listFiles();
        if (files == null) {  // null if security restricted
            throw new IOException("Failed to list contents of " + directory);
        }

        IOException exception = null;
        for (File file : files) {
            try {
                forceDeleteOnExit(file);
            } catch (IOException ioe) {
                exception = ioe;
            }
        }

        if (null != exception) {
            throw exception;
        }
    }

    public static void deleteDirectory(File directory) throws IOException {
        if (!directory.exists()) {
            return;
        }
        if (!isSymlink(directory)) {
            cleanDirectory(directory);
        }
        if (!directory.delete()) {
            String message =
                    "Unable to delete directory " + directory + ".";
            throw new IOException(message);
        }
    }

    private static boolean isSymlink(File file) throws IOException {
        if (file == null) {
            throw new NullPointerException("File must not be null");
        }
        if (isSystemWindows()) {
            return false;
        }
        File fileInCanonicalDir = null;
        if (file.getParent() == null) {
            fileInCanonicalDir = file;
        } else {
            File canonicalDir = file.getParentFile().getCanonicalFile();
            fileInCanonicalDir = new File(canonicalDir, file.getName());
        }

        if (fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile())) {
            return false;
        } else {
            return true;
        }
    }

    private static boolean isSystemWindows() {
        return SYSTEM_SEPARATOR == WINDOWS_SEPARATOR;
    }

    public static void cleanDirectory(File directory) throws IOException {
        if (!directory.exists()) {
            String message = directory + " does not exist";
            throw new IllegalArgumentException(message);
        }

        if (!directory.isDirectory()) {
            String message = directory + " is not a directory";
            throw new IllegalArgumentException(message);
        }

        File[] files = directory.listFiles();
        if (files == null) {  // null if security restricted
            throw new IOException("Failed to list contents of " + directory);
        }

        IOException exception = null;
        for (File file : files) {
            try {
                forceDelete(file);
            } catch (IOException ioe) {
                exception = ioe;
            }
        }

        if (null != exception) {
            throw exception;
        }
    }
}
