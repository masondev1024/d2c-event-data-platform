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
import org.apache.hadoop.mapred.Master;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.yarn.client.util.YarnClientUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

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

  private static void verifyLocalMaster() throws Exception {
    // Upstream 3.5.0 treats every non-CLASSIC framework, including LOCAL, as
    // YarnClientUtils.getRmPrincipal(conf). Exercise that real path rather than
    // treating the symbolic YARN reference as unreachable or changing Master.
    Class.forName(Master.class.getName(), true, VerifyHadoopLocalProfile.class.getClassLoader());
    requireOrigin(Master.class, "hadoop-client-api-3.5.0-d2c-local1.jar");
    requireOrigin(YarnClientUtils.class, "hadoop-client-api-3.5.0-d2c-local1.jar");
    require(Master.State.values().length == 2, "Master enum initialization failed");
    Configuration local = new Configuration(false);
    local.set(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    local.set(MRConfig.MASTER_ADDRESS, "localhost:8012");
    require(Master.getMasterAddress(local).equals("localhost"), "Local Master address lookup changed");
    require(Master.getMasterPrincipal(local) == null, "Unset local Master principal must remain null");
    local.set(YarnConfiguration.RM_ADDRESS, "localhost:8032");
    String fixturePrincipal = "rm/local-profile@D2C.TEST";
    local.set(YarnConfiguration.RM_PRINCIPAL, fixturePrincipal);
    require(fixturePrincipal.equals(Master.getMasterPrincipal(local)), "Local Master principal utility is unavailable");
    require(fixturePrincipal.equals(YarnClientUtils.getRmPrincipal(local)), "Retained principal utility changed");
    System.out.println("master_local_principal=PASS;interactive_websocket_transport=NOT_INVOKED");
  }

  private static void verifyShadedJlineDumbProvider() throws Exception {
    // The official 3.30.17 load(String) uses META-INF/jline/providers/<name>
    // and ClassLoader.loadClass on the descriptor value. Exercise reflection
    // through that actual loader, rather than merely finding a provider class.
    Class<?> spi = Class.forName("org.apache.hadoop.shaded.org.jline.terminal.spi.TerminalProvider");
    requireOrigin(spi, "hadoop-client-runtime-3.5.0-d2c-local1.jar");
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    try {
      thread.setContextClassLoader(spi.getClassLoader());
      Object provider = spi.getMethod("load", String.class).invoke(null, "dumb");
      require(provider.getClass().getName().equals(
          "org.apache.hadoop.shaded.org.jline.terminal.impl.DumbTerminalProvider"),
          "Shaded JLine descriptor did not instantiate the actual dumb provider");
      requireOrigin(provider.getClass(), "hadoop-client-runtime-3.5.0-d2c-local1.jar");
    } finally {
      thread.setContextClassLoader(previous);
    }
    System.out.println("jline_shaded_dumb_provider_loader=PASS;native_and_FFM_backends=NOT_INVOKED");
  }

  public static void main(String[] args) throws Exception {
    requireOrigin(Configuration.class, "hadoop-client-api-3.5.0-d2c-local1.jar");
    Class<?> jackson = Class.forName("org.apache.hadoop.shaded.com.fasterxml.jackson.databind.ObjectMapper");
    requireOrigin(jackson, "hadoop-client-runtime-3.5.0-d2c-local1.jar");
    Object version = Class.forName("org.apache.hadoop.shaded.com.fasterxml.jackson.databind.cfg.PackageVersion")
        .getField("VERSION").get(null);
    require(version.toString().equals("2.21.7"), "Unexpected shaded Jackson implementation: " + version);
    for (String name : new String[] {
        "org.apache.hadoop.mapred.YarnClientProtocolProvider",
        "org.apache.hadoop.mapred.YARNRunner",
        "org.apache.hadoop.shaded.org.eclipse.jetty.client.HttpClient",
        "org.apache.hadoop.shaded.org.eclipse.jetty.http.HttpParser",
        "org.apache.hadoop.shaded.org.eclipse.jetty.websocket.client.WebSocketClient"}) {
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
    verifyLocalMaster();
    verifyShadedJlineDumbProvider();

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
