package io.hstream.distributed.testing;

import static io.hstream.distributed.testing.ClusterExtension.CLUSTER_SIZE;
import static io.hstream.distributed.testing.TestUtils.*;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.hstream.internal.HStreamApiGrpc;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(SetupExtension.class)
public class ClusterAddressTest {
  private static final Logger logger = LoggerFactory.getLogger(ClusterMembershipTest.class);
  private static AtomicInteger count;
  private Path dataDir;

  public void setDataDir(Path dataDir) {
    this.dataDir = dataDir;
  }

  public void setCount(AtomicInteger count) {
    this.count = count;
  }

  @Test
  @Timeout(60)
  void testSetAdvertisedAddress() throws Exception {
    List<HStreamApiGrpc.HStreamApiFutureStub> stubs = new ArrayList<>();
    List<ManagedChannel> channels = new ArrayList<>();
    List<GenericContainer<?>> hServers = new ArrayList<>();
    List<String> hServerInnerUrls = new ArrayList<>();
    List<TestUtils.HServerCliOpts> hServerConfs = new ArrayList<>();

    String adAddr = randText();
    for (int i = 0; i < CLUSTER_SIZE; ++i) {
      int offset = count.incrementAndGet();
      int hServerPort = 1234 + offset;
      int hServerInnerPort = 65000 + offset;
      TestUtils.HServerCliOpts options = new TestUtils.HServerCliOpts();
      options.serverId = Integer.toString(offset);
      options.port = hServerPort;
      options.internalPort = hServerInnerPort;
      options.address = adAddr;
      options.zkHost = "127.0.0.1";

      options.extra = " --gossip-address 127.0.0.1 ";
      hServerConfs.add(options);
      logger.info(options.toString());
      hServerInnerUrls.add("127.0.0.1:" + hServerInnerPort);
      stubs.add(newGrpcStub("127.0.0.1:" + hServerPort, channels));
    }
    var seedNodes = hServerInnerUrls.stream().reduce((url1, url2) -> url1 + "," + url2).get();
    hServers.addAll(bootstrapHServerCluster(hServerConfs, seedNodes, dataDir));
    hServers.forEach(h -> logger.info(h.getLogs()));

    var req = Empty.newBuilder().build();
    var fs = stubs.stream().map(s -> s.describeCluster(req)).collect(Collectors.toList());
    logger.info(fs.stream().map(TestUtils::doGetToString).collect(Collectors.toList()).toString());
    for (var f : fs) {
      Assertions.assertEquals(adAddr, f.get().getServerNodes(0).getHost());
    }

    for (var hServer : hServers) {
      hServer.close();
    }
    hServers.clear();
  }

  @Test
  @Timeout(60)
  void testDescribeShouldReturnCorrespondingAddress() throws Exception {
    List<ManagedChannel> channels = new ArrayList<>();
    List<GenericContainer<?>> hServers = new ArrayList<>();
    List<String> hServerInnerUrls = new ArrayList<>();
    List<TestUtils.HServerCliOpts> hServerConfs = new ArrayList<>();
    List<HStreamApiGrpc.HStreamApiFutureStub> stubs = new ArrayList<>();
    List<HStreamApiGrpc.HStreamApiFutureStub> stubs2 = new ArrayList<>();

    String adAddr = randText();
    String pubAddr = randText();

    for (int i = 0; i < CLUSTER_SIZE; ++i) {
      int offset = count.incrementAndGet();
      int hServerPort = 1234 + offset;
      int hServerInnerPort = 65000 + offset;
      int publicPort = 2000 + offset;
      TestUtils.HServerCliOpts options = new TestUtils.HServerCliOpts();
      options.serverId = Integer.toString(offset);
      options.port = hServerPort;
      options.internalPort = hServerInnerPort;
      options.address = adAddr;
      options.zkHost = "127.0.0.1";

      options.extra =
          " --gossip-address 127.0.0.1 --advertised-listeners public:hstream://"
              + pubAddr
              + ":"
              + publicPort
              + " ";
      hServerConfs.add(options);
      logger.info(options.toString());
      hServerInnerUrls.add("127.0.0.1:" + hServerInnerPort);
      stubs.add(newGrpcStub("127.0.0.1:" + hServerPort, channels));
      stubs2.add(newGrpcStub("127.0.0.1:" + publicPort, channels));
    }
    var seedNodes = hServerInnerUrls.stream().reduce((url1, url2) -> url1 + "," + url2).get();
    hServers.addAll(bootstrapHServerCluster(hServerConfs, seedNodes, dataDir));
    hServers.forEach(h -> logger.info(h.getLogs()));

    var req = Empty.newBuilder().build();
    var fs = stubs.stream().map(s -> s.describeCluster(req)).collect(Collectors.toList());
    logger.info(fs.stream().map(TestUtils::doGetToString).collect(Collectors.toList()).toString());
    for (var f : fs) {
      Assertions.assertEquals(adAddr, f.get().getServerNodes(0).getHost());
    }
    var gs = stubs2.stream().map(s -> s.describeCluster(req)).collect(Collectors.toList());
    logger.info(gs.stream().map(TestUtils::doGetToString).collect(Collectors.toList()).toString());
    for (var g : gs) {
      Assertions.assertEquals(pubAddr, g.get().getServerNodes(0).getHost());
    }

    for (var hServer : hServers) {
      hServer.close();
    }
    hServers.clear();
  }
}
