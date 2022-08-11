package io.hstream.distributed.testing;

import io.hstream.internal.ServerNode;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class TestUtils {

  private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);
  private static final DockerImageName defaultHStreamImageName =
      DockerImageName.parse("hstreamdb/hstream:latest");

  public static ServerNode optionsToNode(HServerCliOpts options) {
    return ServerNode.newBuilder()
        .setId(options.serverId)
        .setHost(options.address)
        .setPort(options.port)
        .build();
  }

  public static GenericContainer<?> makeZooKeeper() {
    return new GenericContainer<>(DockerImageName.parse("zookeeper")).withNetworkMode("host");
  }

  private static DockerImageName getHStreamImageName() {
    String hstreamImageName = System.getenv("HSTREAM_IMAGE_NAME");
    if (hstreamImageName == null || hstreamImageName.equals("")) {
      logger.info(
          "No env variable HSTREAM_IMAGE_NAME found, use default name {}", defaultHStreamImageName);
      return defaultHStreamImageName;
    } else {
      logger.info("Found env variable HSTREAM_IMAGE_NAME = {}", hstreamImageName);
      return DockerImageName.parse(hstreamImageName);
    }
  }

  public static GenericContainer<?> makeHStore(Path dataDir) {
    return new GenericContainer<>(getHStreamImageName())
        .withNetworkMode("host")
        .withFileSystemBind(
            dataDir.toAbsolutePath().toString(), "/data/hstore", BindMode.READ_WRITE)
        .withCommand(
            "bash",
            "-c",
            "ld-dev-cluster "
                + "--root /data/hstore "
                + "--use-tcp "
                + "--tcp-host "
                + "127.0.0.1 "
                + "--user-admin-port 6440 "
                + "--no-interactive")
        .waitingFor(Wait.forLogMessage(".*LogDevice Cluster running.*", 1));
  }

  public static GenericContainer<?> makeHServer(
      HServerCliOpts hserverConf, String seedNodes, Path dataDir) {
    return new GenericContainer<>(getHStreamImageName())
        .withNetworkMode("host")
        .withFileSystemBind(dataDir.toAbsolutePath().toString(), "/data/hstore", BindMode.READ_ONLY)
        .withCommand(
            "bash", "-c", " hstream-server" + hserverConf.toString() + " --seed-nodes " + seedNodes)
        .waitingFor(Wait.forLogMessage(".*Server is started on port.*", 1));
  }

  static class HServerCliOpts {
    public int serverId;
    public String address;
    public int port;
    public int internalPort;
    public String zkHost;

    public ServerNode toNode() {
      return ServerNode.newBuilder().setId(serverId).setHost(address).setPort(port).build();
    }

    public String toString() {
      return " --host "
          + "127.0.0.1 "
          + " --port "
          + port
          + " --internal-port "
          + internalPort
          + " --address "
          + address
          + " --server-id "
          + serverId
          + " --zkuri "
          + zkHost
          + ":2183"
          + " --store-config "
          + "/data/hstore/logdevice.conf "
          + " --store-admin-port "
          + "6440"
          + " --log-level "
          + "debug"
          + " --log-with-color"
          + " --store-log-level "
          + "error";
    }
  }

  public static HServerCliOpts makeHServerCliOpts(AtomicInteger count) throws IOException {
    HServerCliOpts options = new HServerCliOpts();
    options.serverId = count.incrementAndGet();
    ServerSocket socket = new ServerSocket(0);
    ServerSocket socket2 = new ServerSocket(0);
    options.port = socket.getLocalPort();
    socket.close();
    options.internalPort = socket2.getLocalPort();
    socket2.close();
    options.address = "127.0.0.1";
    options.zkHost = "127.0.0.1";
    return options;
  }

  public static List<GenericContainer<?>> bootstrapHServerCluster(
      List<HServerCliOpts> hserverConfs, String seedNodes, Path dataDir)
      throws IOException, InterruptedException {
    List<GenericContainer<?>> hServers = new ArrayList<>();
    for (HServerCliOpts hserverConf : hserverConfs) {
      var hServer = makeHServer(hserverConf, seedNodes, dataDir);
      hServers.add(hServer);
    }
    hServers.stream().parallel().forEach(GenericContainer::start);
    var res =
        hServers
            .get(0)
            .execInContainer(
                "bash",
                "-c",
                "hstream --host "
                    + hserverConfs.get(0).address
                    + " --port "
                    + hserverConfs.get(0).port
                    + " init ");
    return hServers;
  }

  // -----------------------------------------------------------------------------------------------

  public static void writeLog(ExtensionContext context, String entryName, String grp, String logs)
      throws Exception {
    String testClassName = context.getRequiredTestClass().getSimpleName();
    String testName = context.getTestMethod().get().getName();
    String filePathFromProject =
        ".logs/" + testClassName + "/" + testName + "/" + grp + "/" + entryName;
    logger.info("log to " + filePathFromProject);
    String fileName = "../" + filePathFromProject;

    File file = new File(fileName);
    file.getParentFile().mkdirs();
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    writer.write(logs);
    writer.close();
  }

  private static void printFlag(String flag, ExtensionContext context) {
    logger.info(
        "=====================================================================================");
    logger.info(
        "{} {} {} {}",
        flag,
        context.getRequiredTestInstance().getClass().getSimpleName(),
        context.getTestMethod().get().getName(),
        context.getDisplayName());
    logger.info(
        "=====================================================================================");
  }

  public static void printBeginFlag(ExtensionContext context) {
    printFlag("begin", context);
  }

  public static void printEndFlag(ExtensionContext context) {
    printFlag("end", context);
  }

  @FunctionalInterface
  public interface ThrowableRunner {
    void run() throws Throwable;
  }

  public static void silence(ThrowableRunner r) {
    try {
      r.run();
    } catch (Throwable e) {
      logger.info("ignored exception:{}", e.getMessage());
    }
  }
}
