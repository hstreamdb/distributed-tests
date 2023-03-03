package io.hstream.distributed.testing;

import static io.hstream.distributed.testing.ClusterExtension.CLUSTER_SIZE;
import static io.hstream.distributed.testing.TestUtils.*;

import io.grpc.ManagedChannel;
import io.hstream.internal.HStreamApiGrpc;
import io.hstream.internal.LookupResourceRequest;
import io.hstream.internal.LookupSubscriptionRequest;
import io.hstream.internal.ResourceType;
import io.hstream.internal.ServerNode;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
public class LookupResourceTest {

  private static final Logger logger = LoggerFactory.getLogger(LookupResourceTest.class);
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
  void MultipleLookUpShouldReturnTheSameResults() throws Exception {
    var subscriptionId = randText();
    var req = LookupSubscriptionRequest.newBuilder().setSubscriptionId(subscriptionId).build();
    var fs = stubs.stream().map(s -> s.lookupSubscription(req)).collect(Collectors.toList());
    logger.info(fs.stream().map(TestUtils::doGetToString).collect(Collectors.toList()).toString());
    for (var f : fs) {
      Assertions.assertEquals(fs.get(0).get().getServerNode(), f.get().getServerNode());
    }
    var gs = Collections.nCopies(5, stubs.get(0).lookupSubscription(req));
    for (var g : gs) {
      Assertions.assertEquals(fs.get(0).get().getServerNode(), g.get().getServerNode());
    }
  }

  @Test
  @Timeout(60)
  void LookUpShouldReturnTheSameResultWithNewNodeJoin() throws Exception {
    var subscriptionId = randText();
    var req = LookupSubscriptionRequest.newBuilder().setSubscriptionId(subscriptionId).build();
    var fs = stubs.stream().map(s -> s.lookupSubscription(req)).collect(Collectors.toList());
    logger.info(fs.stream().map(TestUtils::doGetToString).collect(Collectors.toList()).toString());
    for (var f : fs) {
      Assertions.assertEquals(fs.get(0).get().getServerNode(), f.get().getServerNode());
    }
    var options = makeHServerCliOpts(count);
    var newServer = makeHServer(options, seedNodes, dataDir);
    newServer.start();
    hServers.add(newServer);
    var g = newGrpcStub(options.address, options.port, channels).lookupSubscription(req);
    Assertions.assertEquals(fs.get(0).get().getServerNode(), g.get().getServerNode());
  }

  @Test
  @Timeout(60)
  void LookUpShouldReturnNewResultWithNodeLeaving() throws Exception {
    var subscriptionId = randText();
    var req =
        LookupResourceRequest.newBuilder()
            .setResType(ResourceType.ResSubscription)
            .setResId(subscriptionId)
            .build();
    var f = stubs.get(0).lookupResource(req);
    var allocatedNode = f.get();
    var allocatedUrl = allocatedNode.getHost() + ':' + allocatedNode.getPort();
    var index = getHServerIndex(allocatedUrl, hServerUrls);
    Assertions.assertNotEquals(-1, index);
    var leavingNode = hServers.get(index);
    logger.info(allocatedNode.toString());
    leavingNode.close();
    hServers.remove(index);
    stubs.remove(index);
    waitForMemberListSync(CLUSTER_SIZE - 1, stubs);
    var gs = stubs.stream().map(s -> lookupWithRetry(s, req, 3)).collect(Collectors.toList());
    logger.info(gs.stream().map(ServerNode::toString).collect(Collectors.toList()).toString());
    for (var g : gs) {
      Assertions.assertNotEquals(allocatedNode, g);
      Assertions.assertEquals(gs.get(0), g);
    }
  }

  @Test
  @Timeout(60)
  void LookUpShouldReturnNewResultWithNodeLeavingAndThenJoin() throws Exception {
    var subscriptionId = randText();
    var req = LookupSubscriptionRequest.newBuilder().setSubscriptionId(subscriptionId).build();
    var f = stubs.get(0).lookupSubscription(req);
    var allocatedNode = f.get().getServerNode();
    var allocatedUrl = allocatedNode.getHost() + ':' + allocatedNode.getPort();
    var index = getHServerIndex(allocatedUrl, hServerUrls);
    Assertions.assertNotEquals(-1, index);
    var leavingNode = hServers.get(index);
    logger.info(allocatedNode.toString());
    leavingNode.close();
    hServers.remove(index);
    stubs.remove(index);
    waitForMemberListSync(CLUSTER_SIZE - 1, stubs);

    var options = makeHServerCliOpts(count);
    var newServer = makeHServer(options, seedNodes, dataDir);
    newServer.start();
    hServers.add(newServer);
    waitForMemberListSync(CLUSTER_SIZE, stubs);
    var newStub = newGrpcStub(options.address, options.port, channels);
    stubs.add(newStub);
    var gg = newStub.lookupSubscription(req);
    Assertions.assertNotEquals(allocatedNode, gg.get().getServerNode());

    var gs = stubs.stream().map(s -> s.lookupSubscription(req)).collect(Collectors.toList());
    logger.info(gs.stream().map(TestUtils::doGetToString).collect(Collectors.toList()).toString());
    for (var g : gs) {
      Assertions.assertEquals(gg.get().getServerNode(), g.get().getServerNode());
    }
  }
}
