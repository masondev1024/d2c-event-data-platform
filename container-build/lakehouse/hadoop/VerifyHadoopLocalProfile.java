/* Run explicitly against the final image's actual classpath. No remote service
 * is contacted; test data is created in a new temporary directory only. */
import java.nio.file.Files;
import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenRenewer;

public class VerifyHadoopLocalProfile {
  private static void require(boolean value, String message) {
    if (!value) throw new IllegalStateException(message);
  }

  private static <T> Set<String> instantiateServices(Class<T> service) {
    Set<String> providers = new HashSet<>();
    // Iterate directly: Hadoop's internal FS/Cluster discovery catches certain
    // LinkageErrors and logs them, which would conceal an incomplete profile.
    for (T provider : ServiceLoader.load(service)) {
      providers.add(provider.getClass().getName());
    }
    require(!providers.isEmpty(), "No advertised providers instantiated: " + service.getName());
    System.out.println("service=" + service.getName() + ";providers=" + providers);
    return providers;
  }

  private static void requireOrigin(Class<?> type, String file) {
    String origin = type.getProtectionDomain().getCodeSource().getLocation().toString();
    require(origin.endsWith("/" + file), "Unexpected/shadowed class origin: " + type.getName() + ": " + origin);
  }

  private static void requireAbsent(String name) throws Exception {
    try {
      Class.forName(name, false, VerifyHadoopLocalProfile.class.getClassLoader());
      throw new IllegalStateException("Excluded subsystem supplied by another JAR: " + name);
    } catch (ClassNotFoundException expected) {
      // Whole module was not selected into either newly assembled JAR.
    }
  }

  public static void main(String[] args) throws Exception {
    requireOrigin(Configuration.class, "hadoop-client-api-3.5.0-d2c-local1.jar");
    Class<?> jackson = Class.forName("org.apache.hadoop.shaded.com.fasterxml.jackson.databind.ObjectMapper");
    requireOrigin(jackson, "hadoop-client-runtime-3.5.0-d2c-local1.jar");
    Object version = Class.forName("org.apache.hadoop.shaded.com.fasterxml.jackson.databind.cfg.PackageVersion")
        .getField("VERSION").get(null);
    require(version.toString().equals("2.21.7"), "Unexpected shaded Jackson implementation: " + version);
    for (String name : new String[] {
        "org.apache.hadoop.yarn.client.api.YarnClient",
        "org.apache.hadoop.yarn.client.api.impl.YarnClientImpl",
        "org.apache.hadoop.mapred.YarnClientProtocolProvider",
        "org.apache.hadoop.mapred.YARNRunner",
        "org.apache.hadoop.shaded.org.eclipse.jetty.client.HttpClient",
        "org.apache.hadoop.shaded.org.eclipse.jetty.http.HttpParser"}) {
      requireAbsent(name);
    }
    Set<String> filesystems = instantiateServices(FileSystem.class);
    require(filesystems.containsAll(Set.of(
        "org.apache.hadoop.fs.LocalFileSystem", "org.apache.hadoop.fs.viewfs.ViewFileSystem",
        "org.apache.hadoop.fs.HarFileSystem", "org.apache.hadoop.fs.http.HttpFileSystem",
        "org.apache.hadoop.fs.http.HttpsFileSystem", "org.apache.hadoop.hdfs.DistributedFileSystem",
        "org.apache.hadoop.hdfs.web.WebHdfsFileSystem", "org.apache.hadoop.hdfs.web.SWebHdfsFileSystem")),
        "An upstream filesystem provider failed to instantiate");
    require(instantiateServices(ClientProtocolProvider.class).equals(
        Set.of("org.apache.hadoop.mapred.LocalClientProtocolProvider")), "Incorrect local protocol services");
    instantiateServices(SecurityInfo.class);
    instantiateServices(TokenIdentifier.class);
    instantiateServices(TokenRenewer.class);

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    conf.set("mapreduce.framework.name", "local");
    java.nio.file.Path temporary = Files.createTempDirectory("d2c-hadoop-local-profile-");
    Path root = new Path(temporary.toUri());
    try (FileSystem fs = FileSystem.newInstanceLocal(conf)) {
      Path source = new Path(root, "created.txt");
      Path renamed = new Path(root, "renamed.txt");
      try (FSDataOutputStream stream = fs.create(source, false)) {
        stream.writeUTF("d2c-hadoop-3.5.0-local-profile");
        stream.hflush();
      }
      require(fs.rename(source, renamed), "Local filesystem rename failed");
      try (FSDataInputStream stream = fs.open(renamed)) {
        require(stream.readUTF().equals("d2c-hadoop-3.5.0-local-profile"), "Local filesystem read/write mismatch");
      }
      require(fs.listStatus(root).length == 1, "Local filesystem listing mismatch");
      Job job = Job.getInstance(conf, "d2c-local-api-probe");
      Path output = new Path(root, "output");
      FileOutputFormat.setOutputPath(job, output);
      TaskAttemptContextImpl context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
      FileOutputCommitter committer = new FileOutputCommitter(output, context);
      require(committer.getWorkPath() != null, "MapReduce output committer API unavailable");
      try (Cluster cluster = new Cluster(conf)) {
        require(cluster.getFileSystem().getUri().getScheme().equals("file"), "Local provider bootstrap failed");
      }
    }
    System.out.println("hadoop_local_profile_boot=PASS;jackson=2.21.7;remote_operations=NONE");
    System.out.println("YARN_Timeline_token_renew_cancel=UNSUPPORTED_NOT_INVOKED");
  }
}
