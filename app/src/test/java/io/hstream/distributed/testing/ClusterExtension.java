package io.hstream.distributed.testing;

import static io.hstream.distributed.testing.TestUtils.HServerCliOpts;
import static io.hstream.distributed.testing.TestUtils.bootstrapHServerCluster;

import io.hstream.HStreamClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

public class ClusterExtension implements BeforeEachCallback, AfterEachCallback {

  static final int CLUSTER_SIZE = 3;

  private static final AtomicInteger count = new AtomicInteger(0);
  private static final Logger logger = LoggerFactory.getLogger(ClusterExtension.class);
  private final List<GenericContainer<?>> hServers = new ArrayList<>(CLUSTER_SIZE);
  private final List<String> hServerUrls = new ArrayList<>(CLUSTER_SIZE);
  private final List<String> hServerInnerUrls = new ArrayList<>(CLUSTER_SIZE);
  private String seedNodes;
  private Path dataDir;
  private GenericContainer<?> zk;
  private GenericContainer<?> hstore;
  private String grp;
  private long beginTime;

  private HStreamClient client;

  public ClusterExtension() {
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    beginTime = System.currentTimeMillis();
    grp = UUID.randomUUID().toString();
    TestUtils.printBeginFlag(context);

    dataDir = Files.createTempDirectory("hstream");

    String zkHost = "127.0.0.1";
    zk = TestUtils.makeZooKeeper();
    zk.start();
    logger.debug("zkHost: " + zkHost);

    hstore = TestUtils.makeHStore(dataDir);
    hstore.start();

    String hServerAddress = "127.0.0.1";
    List<HServerCliOpts> hserverConfs = new ArrayList<HServerCliOpts>(CLUSTER_SIZE);
    for (int i = 0; i < CLUSTER_SIZE; ++i) {
      int offset = count.incrementAndGet();
      int hServerPort = 1234 + offset;
      int hServerInnerPort = 6500 + offset;
      HServerCliOpts options = new HServerCliOpts();
      options.serverId = offset;
      options.port = hServerPort;
      options.internalPort = hServerInnerPort;
      options.address = hServerAddress;
      options.zkHost = zkHost;
      hserverConfs.add(options);
      hServerUrls.add(hServerAddress + ":" + hServerPort);
      hServerInnerUrls.add(hServerAddress + ":" + hServerInnerPort);
    }
    seedNodes = hServerInnerUrls.stream().reduce((url1, url2) -> url1 + "," + url2).get();
    hServers.addAll(bootstrapHServerCluster(hserverConfs, seedNodes, dataDir));
    Thread.sleep(3000);

    Object testInstance = context.getRequiredTestInstance();

    TestUtils.silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setCount", AtomicInteger.class)
                .invoke(testInstance, count));
    TestUtils.silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setDataDir", Path.class)
                .invoke(testInstance, dataDir));

    TestUtils.silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setHServers", List.class)
                .invoke(testInstance, hServers));

    TestUtils.silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setHServerUrls", List.class)
                .invoke(testInstance, hServerUrls));

    TestUtils.silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setSeedNodes", String.class)
                .invoke(testInstance, seedNodes));

    TestUtils.silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setLogMsgPathPrefix", String.class)
                .invoke(testInstance, grp));

    TestUtils.silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setExtensionContext", ExtensionContext.class)
                .invoke(testInstance, context));
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    Thread.sleep(100);

    // waiting for servers to flush logs
    for (int i = 0; i < hServers.size(); i++) {
      var hServer = hServers.get(i);
      TestUtils.writeLog(context, "hserver-" + i, grp, hServer.getLogs());
      hServer.close();
    }

    hServers.clear();
    hServerUrls.clear();
    hServerInnerUrls.clear();

    TestUtils.writeLog(context, "hstore", grp, hstore.getLogs());
    hstore.close();
    TestUtils.writeLog(context, "zk", grp, zk.getLogs());
    zk.close();

    logger.info("total time is = {}ms", System.currentTimeMillis() - beginTime);
    TestUtils.printEndFlag(context);
  }

}
