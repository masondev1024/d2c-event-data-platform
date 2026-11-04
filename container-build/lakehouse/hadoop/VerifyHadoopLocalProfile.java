/* Run explicitly against the final image's actual classpath. No remote service
 * is contacted; test data is created in a new temporary directory only. */
import java.nio.file.Files;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
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

  private static Object roundTrip(Object value) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
      output.writeObject(value);
    }
    try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return input.readObject();
    }
  }

  @SuppressWarnings("unchecked")
  private static void verifyFixedGuavaAndCurator() throws Exception {
    String runtime = "hadoop-client-runtime-3.5.0-d2c-local1.jar";
    for (String prefix : new String[] {"org.apache.hadoop.shaded.com.google.common.",
                                      "org.apache.hadoop.thirdparty.com.google.common."}) {
      for (String collection : new String[] {"CompactHashMap", "CompactHashSet"}) {
        Class<?> type = Class.forName(prefix + "collect." + collection);
        requireOrigin(type, runtime);
        Method create = type.getDeclaredMethod("create");
        create.setAccessible(true); // Guava's own compact implementations are package private.
        Object value = create.invoke(null);
        if (value instanceof Map) {
          ((Map<Object, Object>) value).put("d2c", "fixed-guava");
          require("fixed-guava".equals(((Map<?, ?>) roundTrip(value)).get("d2c")),
                  "Compact map serialization failed: " + prefix);
        } else {
          ((Collection<Object>) value).add("fixed-guava");
          require(((Collection<?>) roundTrip(value)).contains("fixed-guava"),
                  "Compact set serialization failed: " + prefix);
        }
      }
      Class<?> maker = Class.forName(prefix + "collect.MapMaker");
      requireOrigin(maker, runtime);
      Object builder = maker.getConstructor().newInstance();
      // Force the custom implementation with strong keys/values: a weak-key
      // deserialization assertion would legitimately race garbage collection.
      Class<?> equivalence = Class.forName(prefix + "base.Equivalence");
      requireOrigin(equivalence, runtime);
      Method keyEquivalence = maker.getDeclaredMethod("keyEquivalence", equivalence);
      keyEquivalence.setAccessible(true);
      keyEquivalence.invoke(builder, equivalence.getMethod("equals").invoke(null));
      Map<Object, Object> map = (Map<Object, Object>) maker.getMethod("makeMap").invoke(builder);
      require(map.getClass().getName().startsWith(prefix + "collect.MapMakerInternalMap"),
              "Actual MapMakerInternalMap not selected");
      requireOrigin(map.getClass(), runtime);
      map.put("d2c", "fixed-guava");
      Map<?, ?> copy = (Map<?, ?>) roundTrip(map);
      require(copy.size() == 1 && copy.containsValue("fixed-guava"), "MapMaker serialization failed");
    }
    String curator = "org.apache.hadoop.shaded.org.apache.curator.";
    Class<?> policy = Class.forName(curator + "RetryPolicy");
    Class<?> retry = Class.forName(curator + "retry.ExponentialBackoffRetry");
    Class<?> factory = Class.forName(curator + "framework.CuratorFrameworkFactory");
    Class<?> framework = Class.forName(curator + "framework.CuratorFramework");
    Class<?> mutex = Class.forName(curator + "framework.recipes.locks.InterProcessMutex");
    for (Class<?> type : new Class<?>[] {policy, retry, factory, framework, mutex}) requireOrigin(type, runtime);
    Object client = factory.getMethod("newClient", String.class, policy)
        .invoke(null, "127.0.0.1:1", retry.getConstructor(int.class, int.class).newInstance(10, 1));
    try {
      require(framework.getMethod("getState").invoke(client).toString().equals("LATENT"),
              "Curator client must remain unstarted");
      mutex.getConstructor(framework, String.class).newInstance(client, "/d2c-offline-probe");
    } finally {
      framework.getMethod("close").invoke(client);
    }
    requireAbsent(curator + "shaded.com.google.common.collect.CompactHashMap");
    System.out.println("guava_two_namespaces_small_serialization=PASS;curator_client_framework_recipes_init=PASS;zookeeper_network_calls=NONE");
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
    verifyFixedGuavaAndCurator();

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
      // Hadoop 3.5.0 Cluster has close(), but does not implement AutoCloseable.
      Cluster cluster = new Cluster(conf);
      try {
        require(cluster.getFileSystem().getUri().getScheme().equals("file"), "Local provider bootstrap failed");
      } finally {
        cluster.close();
      }
    }
    System.out.println("hadoop_local_profile_boot=PASS;jackson=2.21.7;remote_operations=NONE");
    System.out.println("YARN_Timeline_token_renew_cancel=UNSUPPORTED_NOT_INVOKED");
  }
}
