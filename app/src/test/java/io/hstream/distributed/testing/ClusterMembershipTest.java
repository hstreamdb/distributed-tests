package io.hstream.distributed.testing;

import static io.hstream.distributed.testing.ClusterExtension.CLUSTER_SIZE;
import static io.hstream.distributed.testing.TestUtils.*;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.hstream.internal.HStreamApiGrpc;
import io.hstream.internal.ServerNode;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(ClusterExtension.class)
public class ClusterMembershipTest {

  private static final Logger logger = LoggerFactory.getLogger(ClusterMembershipTest.class);
  private List<String> hServerUrls;
  private List<GenericContainer<?>> hServers;
  private static AtomicInteger count;
  private Path dataDir;
  private String seedNodes;
  private final List<HStreamApiGrpc.HStreamApiFutureStub> stubs = new ArrayList<>();
  private final List<ManagedChannel> channels = new ArrayList<>();

  public void setHServers(List<GenericContainer<?>> hServers) {
    this.hServers = hServers;
  }

  public void setHServerUrls(List<String> hServerUrls) {
    this.hServerUrls = hServerUrls;
  }

  public void setSeedNodes(String seedNodes) {
    this.seedNodes = seedNodes;
  }

  public void setDataDir(Path dataDir) {
    this.dataDir = dataDir;
  }

  public void setCount(AtomicInteger count) {
    this.count = count;
  }

  @BeforeEach
  public void setup() {
    for (var url : hServerUrls) {
      stubs.add(newGrpcStub(url, channels));
    }
  }

  @AfterEach
  public void teardown() {
    channels.forEach(ManagedChannel::shutdown);
  }

  @Test
  @Timeout(60)
  void testClusterBootStrap() throws Exception {
    var req = Empty.newBuilder().build();
    var fs = stubs.stream().map(s -> s.describeCluster(req)).collect(Collectors.toList());

    logger.info(fs.stream().map(TestUtils::doGetToString).collect(Collectors.toList()).toString());
    for (var f : fs) {
      Assertions.assertEquals(CLUSTER_SIZE, f.get().getServerNodesCount());
    }
  }

  @Test
  @Timeout(60)
  void testRestartOneSeed() throws Exception {
    var rand = new Random();
    var index = rand.nextInt(CLUSTER_SIZE - 1);
    var restartingNode = hServers.get(index);
    var req = Empty.newBuilder().build();
    var fs = stubs.stream().map(s -> s.describeCluster(req)).collect(Collectors.toList());
    logger.info(fs.stream().map(TestUtils::doGetToString).collect(Collectors.toList()).toString());
    for (var f : fs) {
      Assertions.assertEquals(CLUSTER_SIZE, f.get().getServerNodesCount());
    }
    restart(restartingNode);
    waitForMemberListSync(CLUSTER_SIZE, stubs);
  }

  @Test
  @Timeout(60)
  void testRestartMoreSeeds() throws Exception {
    var rand = new Random();
    var index = rand.nextInt(CLUSTER_SIZE - 1);
    var req = Empty.newBuilder().build();
    var fs = stubs.stream().map(s -> s.describeCluster(req)).collect(Collectors.toList());
    logger.info(fs.stream().map(TestUtils::doGetToString).collect(Collectors.toList()).toString());
    for (var f : fs) {
      Assertions.assertEquals(CLUSTER_SIZE, f.get().getServerNodesCount());
    }
    int i = 0;
    for (GenericContainer<?> hserver : hServers) {
      if (i != index) restart(hserver);
      i++;
    }
    waitForMemberListSync(CLUSTER_SIZE, stubs);
  }

  @Test
  @Timeout(60)
  void testSingleNodeJoin() throws Exception {
    var options = makeHServerCliOpts(count);
    var newServer = makeHServer(options, seedNodes, dataDir);
    newServer.start();

    var req = Empty.newBuilder().build();
    var newNode =
        ServerNode.newBuilder()
            .setId(options.serverId)
            .setHost(options.address)
            .setPort(options.port)
            .build();
    stubs.add(newGrpcStub(options.address, options.port, channels));
    waitForMemberListSync(CLUSTER_SIZE + 1, stubs);
    var fs = stubs.stream().map(s -> s.describeCluster(req)).collect(Collectors.toList());

    logger.info(fs.stream().map(TestUtils::doGetToString).collect(Collectors.toList()).toString());
    for (var f : fs) {
      var nodes =
          f.get().getServerNodesList().stream()
              .map(
                  (node) ->
                      ServerNode.newBuilder()
                          .setId(node.getId())
                          .setHost(node.getHost())
                          .setPort(node.getPort())
                          .build())
              .collect(Collectors.toList());
      Assertions.assertEquals(CLUSTER_SIZE + 1, f.get().getServerNodesCount());
      Assertions.assertTrue(nodes.contains(newNode));
    }

    newServer.close();
  }

  @Test
  @Timeout(60)
  void testMultipleNodesJoin() throws Exception {
    var newNodesNum = 3;
    var newNodesInternalUrls = new ArrayList<String>();
    var newServers = new ArrayList<GenericContainer>();
    var newNodes = new ArrayList<ServerNode>();
    for (int i = 0; i < newNodesNum; ++i) {
      var options = makeHServerCliOpts(count);
      var newServer = makeHServer(options, seedNodes, dataDir);
      newServers.add(newServer);
      newNodes.add(options.toNode());
      newNodesInternalUrls.add(options.address + ":" + options.port);
    }
    newServers.stream().parallel().forEach(GenericContainer::start);

    for (var url : newNodesInternalUrls) {
      stubs.add(newGrpcStub(url, channels));
    }

    waitForMemberListSync(CLUSTER_SIZE + newNodesNum, stubs);
    var req = Empty.newBuilder().build();
    var fs = stubs.stream().map(s -> s.describeCluster(req)).collect(Collectors.toList());

    logger.info(fs.stream().map(TestUtils::doGetToString).collect(Collectors.toList()).toString());
    for (var f : fs) {
      var nodes =
          f.get().getServerNodesList().stream()
              .map(
                  (node) ->
                      ServerNode.newBuilder()
                          .setId(node.getId())
                          .setHost(node.getHost())
                          .setPort(node.getPort())
                          .build())
              .collect(Collectors.toList());
      Assertions.assertEquals(CLUSTER_SIZE + newNodesNum, f.get().getServerNodesCount());
      Assertions.assertTrue(nodes.containsAll(newNodes));
    }

    newServers.stream().parallel().forEach(GenericContainer::close);
  }

  @Test
  @Timeout(60)
  void testSingleNodeLeave() throws Exception {
    var rand = new Random();
    var req = Empty.newBuilder().build();
    var fs = stubs.stream().map(s -> s.describeCluster(req)).collect(Collectors.toList());

    var index = rand.nextInt(CLUSTER_SIZE - 1);
    var leavingNode = hServers.get(index);

    for (var f : fs) {
      Assertions.assertEquals(CLUSTER_SIZE, f.get().getServerNodesCount());
      //      Assertions.assertTrue(f.get().getServerNodesList().contains(leavingNode));
    }

    leavingNode.close();
    hServers.remove(index);
    stubs.remove(index);
    waitForMemberListSync((CLUSTER_SIZE - 1), stubs);
  }
}
