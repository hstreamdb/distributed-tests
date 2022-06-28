package io.hstream.distributed.testing;

import static io.hstream.distributed.testing.ClusterExtension.CLUSTER_SIZE;
import static io.hstream.distributed.testing.TestUtils.*;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.hstream.internal.HStreamApiGrpc;
import io.hstream.internal.ServerNode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(ClusterExtension.class)
public class ClusterStartTest {

  private static final Logger logger = LoggerFactory.getLogger(ClusterStartTest.class);
  private List<String> hServerUrls;
  private static AtomicInteger count;
  private Path dataDir;
  private String seedNodes;
  private final List<HStreamApiGrpc.HStreamApiFutureStub> stubs = new ArrayList<>();
  private final List<ManagedChannel> channels = new ArrayList<>();

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

  private HStreamApiGrpc.HStreamApiFutureStub getStub(ServerNode node) {
    return getStub(node.getHost() + ":" + node.getPort());
  }

  private HStreamApiGrpc.HStreamApiFutureStub getStub(String url) {
    for (int i = 0; i < hServerUrls.size(); i++) {
      if (hServerUrls.get(i).equals(url)) {
        return stubs.get(i);
      }
    }
    return null;
  }

  @BeforeEach
  public void setup() {
    for (var url : hServerUrls) {
      var ss = url.split(":");
      var channel =
          ManagedChannelBuilder.forAddress(ss[0], Integer.parseInt(ss[1])).usePlaintext().build();
      channels.add(channel);
      stubs.add(HStreamApiGrpc.newFutureStub(channel));
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
    logger.info(fs.stream().map(f -> {
      try {
        return f.get().toString();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList()).toString());
    for (var f : fs) {
      Assertions.assertEquals(CLUSTER_SIZE, f.get().getServerNodesCount());
    }
  }

  @Test
  @Timeout(60)
  void testSingleNodeJoin() throws Exception {
    var options = makeHServerCliOpts(count);
    var newServer = makeHServer(options, seedNodes, dataDir);
    newServer.start();

    var req = Empty.newBuilder().build();
    var newNode = ServerNode.newBuilder().setId(options.serverId).setHost(options.address).setPort(options.port).build();
    var fs = stubs.stream().map(s -> s.describeCluster(req)).collect(Collectors.toList());
    logger.info(fs.stream().map(f -> {
      try {
        return f.get().toString();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList()).toString());
    for (var f : fs) {
      Assertions.assertEquals(CLUSTER_SIZE + 1, f.get().getServerNodesCount());
      Assertions.assertTrue(f.get().getServerNodesList().contains(newNode));
    }

    newServer.close();
  }

  @Test
  @Timeout(60)
  void testMultipleNodesJoin() throws Exception {
    var newNodesNum = 3;
    var newNodesInternalUrls = new ArrayList<String>();
    var newServers = new ArrayList<GenericContainer>();
    var newNodes   = new ArrayList<ServerNode>();
    for (int i = 0; i < newNodesNum; ++i) {
      var options = makeHServerCliOpts(count);
      var newServer = makeHServer(options, seedNodes, dataDir);
      newServers.add(newServer);
      newNodes.add(optionsToNode(options));
      newNodesInternalUrls.add(options.address + ":" + options.port);
    }
    newServers.stream().parallel().forEach(GenericContainer::start);

    for (var url : newNodesInternalUrls) {
      var ss = url.split(":");
      var channel =
              ManagedChannelBuilder.forAddress(ss[0], Integer.parseInt(ss[1])).usePlaintext().build();
      channels.add(channel);
      stubs.add(HStreamApiGrpc.newFutureStub(channel));
    }

    Thread.sleep(3000);
    var req = Empty.newBuilder().build();
    var fs = stubs.stream().map(s -> s.describeCluster(req)).collect(Collectors.toList());
    logger.info(fs.stream().map(f -> {
      try {
        return f.get().toString();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList()).toString());
    for (var f : fs) {
      Assertions.assertEquals(CLUSTER_SIZE + newNodesNum, f.get().getServerNodesCount());
      Assertions.assertTrue(f.get().getServerNodesList().containsAll(newNodes));
    }

    newServers.stream().parallel().forEach(GenericContainer::close);
  }


  //
}

// testSingleNodeJoin
// testMultipleNodesLeave
// testConsistencyInLookup
// testConsistencyInMemberList

