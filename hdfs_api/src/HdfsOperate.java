import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;
import java.net.URI;

public class HdfsOperate {
    private static FileSystem fs;

    // 初始化HDFS连接
    public static void init() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        fs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf, "hadoop");
    }

    // 1. 上传文件 overwrite=true覆盖，false追加
    public static void upload(String localPath, String hdfsPath, boolean overwrite) throws IOException {
        Path src = new Path(localPath);
        Path dst = new Path(hdfsPath);
        if (fs.exists(dst)) {
            if (overwrite) {
                fs.copyFromLocalFile(false, true, new Path[]{src}, dst);
            } else {
                FSDataOutputStream out = fs.append(dst);
                FileInputStream in = new FileInputStream(localPath);
                byte[] buf = new byte[1024];
                int len;
                while ((len = in.read(buf)) > 0) out.write(buf, 0, len);
                in.close();
                out.close();
            }
        } else {
            fs.copyFromLocalFile(false, false, new Path[]{src}, dst);
        }
    }

    // 2. 下载文件，同名自动重命名
    public static void download(String hdfsPath, String localDir) throws IOException {
        Path src = new Path(hdfsPath);
        String fileName = src.getName();
        File localFile = new File(localDir + "/" + fileName);
        if (localFile.exists()) {
            String newName = fileName + ".bak." + System.currentTimeMillis();
            fs.copyToLocalFile(false, src, new Path(localDir + "/" + newName), true);
        } else {
            fs.copyToLocalFile(false, src, new Path(localDir), true);
        }
    }

    // 3. 输出文件内容
    public static void cat(String hdfsPath) throws IOException {
        FSDataInputStream in = fs.open(new Path(hdfsPath));
        byte[] buf = new byte[1024];
        int len;
        while ((len = in.read(buf)) > 0) System.out.write(buf, 0, len);
        in.close();
    }

    // 4. 获取文件详细信息
    public static void getFileInfo(String hdfsPath) throws IOException {
        FileStatus s = fs.getFileStatus(new Path(hdfsPath));
        System.out.println("权限：" + s.getPermission());
        System.out.println("大小：" + s.getLen() + " 字节");
        System.out.println("修改时间：" + s.getModificationTime());
        System.out.println("路径：" + s.getPath());
    }

    // 5. 递归列出目录所有文件
    public static void listDirRecursive(String hdfsPath) throws IOException {
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(hdfsPath), true);
        while (it.hasNext()) {
            LocatedFileStatus f = it.next();
            System.out.println(f.getPermission() + "\t" + f.getLen() + "\t" + f.getPath());
        }
    }

    // 6. 创建文件（自动建父目录）、删除文件
    public static void createFile(String hdfsPath) throws IOException {
        Path path = new Path(hdfsPath);
        Path parent = path.getParent();
        if (!fs.exists(parent)) fs.mkdirs(parent);
        fs.createNewFile(path);
    }

    public static void deleteFile(String hdfsPath) throws IOException {
        fs.delete(new Path(hdfsPath), false);
    }

    // 7. 创建目录、删除空目录
    public static void mkdir(String hdfsPath) throws IOException {
        fs.mkdirs(new Path(hdfsPath));
    }

    public static void rmdir(String hdfsPath) throws IOException {
        Path path = new Path(hdfsPath);
        if (fs.getFileStatus(path).isDirectory() && fs.listStatus(path).length == 0) {
            fs.delete(path, false);
        } else System.out.println("目录非空，无法删除");
    }

    // 8. 追加内容到开头/结尾
    public static void appendContent(String hdfsPath, String content, boolean toEnd) throws IOException {
        Path path = new Path(hdfsPath);
        if (toEnd) {
            FSDataOutputStream out = fs.append(path);
            out.write(content.getBytes());
            out.close();
        } else {
            FSDataInputStream in = fs.open(path);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buf = new byte[1024];
            int len;
            while ((len = in.read(buf)) > 0) baos.write(buf, 0, len);
            in.close();
            FSDataOutputStream out = fs.create(path, true);
            out.write(content.getBytes());
            out.write(baos.toByteArray());
            out.close();
        }
    }

    // 9. 删除文件
    public static void delete(String hdfsPath) throws IOException {
        fs.delete(new Path(hdfsPath), false);
    }

    // 10. 移动文件
    public static void move(String src, String dst) throws IOException {
        fs.rename(new Path(src), new Path(dst));
    }

    public static void close() throws IOException {
        fs.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        init();
        // 功能测试示例
        mkdir("/user/hadoop/java_test");
        createFile("/user/hadoop/java_test/test.txt");
        appendContent("/user/hadoop/java_test/test.txt", "hello hadoop\n", true);
        cat("/user/hadoop/java_test/test.txt");
        getFileInfo("/user/hadoop/java_test/test.txt");
        close();
    }
}

