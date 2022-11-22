package io.hstream.distributed.testing;

import static io.hstream.distributed.testing.TestUtils.*;

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

public class SetupExtension implements BeforeEachCallback, AfterEachCallback {

  static final int CLUSTER_SIZE = 3;
  private static final AtomicInteger count = new AtomicInteger(0);
  private static final Logger logger = LoggerFactory.getLogger(SetupExtension.class);
  private final List<GenericContainer<?>> hServers = new ArrayList<>();
  private final List<String> hServerUrls = new ArrayList<>();
  private final List<String> hServerInnerUrls = new ArrayList<>();
  private String seedNodes;
  private Path dataDir;
  private GenericContainer<?> zk;
  private GenericContainer<?> rq;
  private GenericContainer<?> hstore;
  private String grp;
  private long beginTime;

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    beginTime = System.currentTimeMillis();

    grp = UUID.randomUUID().toString();
    printBeginFlag(context);

    dataDir = Files.createTempDirectory("hstream");

    zk = makeZooKeeper();
    zk.start();
    rq = makeRQLite();
    rq.start();
    String zkHost = "127.0.0.1";
    logger.debug("zkHost: " + zkHost);

    hstore = makeHStore(dataDir);
    hstore.start();
    String hstoreHost = "127.0.0.1";
    logger.debug("hstoreHost: " + hstoreHost);

    Object testInstance = context.getRequiredTestInstance();
    silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setCount", AtomicInteger.class)
                .invoke(testInstance, count));

    silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setDataDir", Path.class)
                .invoke(testInstance, dataDir));
    silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setLogMsgPathPrefix", String.class)
                .invoke(testInstance, grp));

    silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setExtensionContext", ExtensionContext.class)
                .invoke(testInstance, context));
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {

    count.set(0);
    writeLog(context, "hstore", grp, hstore.getLogs());
    hstore.close();
    writeLog(context, "zk", grp, zk.getLogs());
    zk.close();
    rq.close();

    logger.info("total time is = {}ms", System.currentTimeMillis() - beginTime);
    printEndFlag(context);
  }
}
