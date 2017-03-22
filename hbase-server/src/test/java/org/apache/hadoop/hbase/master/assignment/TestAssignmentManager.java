/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.master.assignment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureScheduler;
import org.apache.hadoop.hbase.master.procedure.ProcedureSyncWait;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionRequest.RegionOpenInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionResponse.RegionOpeningState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

@Category({MasterTests.class, MediumTests.class})
public class TestAssignmentManager {
  private static final Log LOG = LogFactory.getLog(TestAssignmentManager.class);
  static {
    Logger.getLogger(MasterProcedureScheduler.class).setLevel(Level.TRACE);
  }
  @Rule public TestName name = new TestName();
  @Rule public final TestRule timeout =
      CategoryBasedTimeout.builder().withTimeout(this.getClass()).
        withLookingForStuckThread(true).build();

  private static final int PROC_NTHREADS = 64;
  private static final int NREGIONS = 1 * 1000;
  private static final int NSERVERS = Math.max(1, NREGIONS / 200);

  private HBaseTestingUtility UTIL;
  private MockRSProcedureDispatcher rsDispatcher;
  private MockMasterServices master;
  private AssignmentManager am;
  private NavigableMap<ServerName, SortedSet<byte []>> regionsToRegionServers =
      new ConcurrentSkipListMap<ServerName, SortedSet<byte []>>();

  private void setupConfiguration(Configuration conf) throws Exception {
    FSUtils.setRootDir(conf, UTIL.getDataTestDir());
    conf.setBoolean(WALProcedureStore.USE_HSYNC_CONF_KEY, false);
    conf.setInt(WALProcedureStore.SYNC_WAIT_MSEC_CONF_KEY, 10);
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, PROC_NTHREADS);
    conf.setInt(RSProcedureDispatcher.RS_RPC_STARTUP_WAIT_TIME_CONF_KEY, 1000);
    conf.setInt(AssignmentManager.ASSIGN_MAX_ATTEMPTS, 5);
  }

  @Before
  public void setUp() throws Exception {
    UTIL = new HBaseTestingUtility();
    setupConfiguration(UTIL.getConfiguration());
    master = new MockMasterServices(UTIL.getConfiguration(), this.regionsToRegionServers);
    rsDispatcher = new MockRSProcedureDispatcher(master);
    master.start(NSERVERS, rsDispatcher);
    am = master.getAssignmentManager();
    setUpMeta();
  }

  private void setUpMeta() throws Exception {
    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());
    am.assignMeta(HRegionInfo.FIRST_META_REGIONINFO);
    am.wakeMetaLoadedEvent();
    am.setFailoverCleanupDone(true);
  }

  @After
  public void tearDown() throws Exception {
    master.stop("tearDown");
  }

  @Test
  public void testAssignWithGoodExec() throws Exception {
    testAssign(new GoodRsExecutor());
  }

  @Test
  public void testAssignWithRandExec() throws Exception {
    final TableName tableName = TableName.valueOf("testAssignWithRandExec");
    final HRegionInfo hri = createRegionInfo(tableName, 1);

    rsDispatcher.setMockRsExecutor(new RandRsExecutor());

    AssignProcedure proc = am.createAssignProcedure(hri, false);
    //waitOnFuture(submitProcedure(am.createAssignProcedure(hri, false, false)));
    // TODO
  }

  @Test
  public void testSocketTimeout() throws Exception {
    final TableName tableName = TableName.valueOf(this.name.getMethodName());
    final HRegionInfo hri = createRegionInfo(tableName, 1);

    rsDispatcher.setMockRsExecutor(new SocketTimeoutRsExecutor(20, 3));
    waitOnFuture(submitProcedure(am.createAssignProcedure(hri, false)));

    rsDispatcher.setMockRsExecutor(new SocketTimeoutRsExecutor(20, 3));
    waitOnFuture(submitProcedure(am.createUnassignProcedure(hri, null, false)));
  }

  @Test
  public void testServerNotYetRunning() throws Exception {
    testRetriesExhaustedFailure(TableName.valueOf("testServerNotYetRunning"),
      new ServerNotYetRunningRsExecutor());
  }

  private void testRetriesExhaustedFailure(final TableName tableName,
      final MockRSExecutor executor) throws Exception {
    final HRegionInfo hri = createRegionInfo(tableName, 1);

    // Test Assign operation failure
    rsDispatcher.setMockRsExecutor(executor);
    try {
      waitOnFuture(submitProcedure(am.createAssignProcedure(hri, false)));
      fail("unexpected assign completion");
    } catch (RetriesExhaustedException e) {
      // expected exception
      LOG.info("expected exception from assign operation: " + e.getMessage(), e);
    }

    // Assign the region (without problems)
    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());
    waitOnFuture(submitProcedure(am.createAssignProcedure(hri, false)));

    // Test Unassign operation failure
    rsDispatcher.setMockRsExecutor(executor);
    waitOnFuture(submitProcedure(am.createUnassignProcedure(hri, null, false)));
  }


  @Test
  public void testIOExceptionOnAssignment() throws Exception {
    testFailedOpen(TableName.valueOf("testExceptionOnAssignment"),
      new FaultyRsExecutor(new IOException("test fault")));
  }

  @Test
  public void testDoNotRetryExceptionOnAssignment() throws Exception {
    testFailedOpen(TableName.valueOf("testDoNotRetryExceptionOnAssignment"),
      new FaultyRsExecutor(new DoNotRetryIOException("test do not retry fault")));
  }

  private void testFailedOpen(final TableName tableName,
      final MockRSExecutor executor) throws Exception {
    final HRegionInfo hri = createRegionInfo(tableName, 1);

    // Test Assign operation failure
    rsDispatcher.setMockRsExecutor(executor);
    try {
      waitOnFuture(submitProcedure(am.createAssignProcedure(hri, false)));
      fail("unexpected assign completion");
    } catch (RetriesExhaustedException e) {
      // expected exception
      LOG.info("REGION STATE " + am.getRegionStates().getRegionNode(hri));
      LOG.info("expected exception from assign operation: " + e.getMessage(), e);
      assertEquals(true, am.getRegionStates().getRegionState(hri).isFailedOpen());
    }
  }

  private void testAssign(final MockRSExecutor executor) throws Exception {
    testAssign(executor, NREGIONS);
  }

  private void testAssign(final MockRSExecutor executor, final int nregions) throws Exception {
    rsDispatcher.setMockRsExecutor(executor);

    AssignProcedure[] assignments = new AssignProcedure[nregions];

    long st = System.currentTimeMillis();
    bulkSubmit(assignments);

    for (int i = 0; i < assignments.length; ++i) {
      ProcedureTestingUtility.waitProcedure(
        master.getMasterProcedureExecutor(), assignments[i]);
      assertTrue(assignments[i].toString(), assignments[i].isSuccess());
    }
    long et = System.currentTimeMillis();
    float sec = ((et - st) / 1000.0f);
    LOG.info(String.format("[T] Assigning %dprocs in %s (%.2fproc/sec)",
        assignments.length, StringUtils.humanTimeDiff(et - st), assignments.length / sec));
  }

  @Test
  public void testAssignAnAssignedRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testAssignAnAssignedRegion");
    final HRegionInfo hri = createRegionInfo(tableName, 1);

    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());

    final Future<byte[]> futureA = submitProcedure(am.createAssignProcedure(hri, false));

    // wait first assign
    waitOnFuture(futureA);
    am.getRegionStates().isRegionInState(hri, State.OPEN);
    // Second should be a noop. We should recognize region is already OPEN internally
    // and skip out doing nothing.
    // wait second assign
    final Future<byte[]> futureB = submitProcedure(am.createAssignProcedure(hri, false));
    waitOnFuture(futureB);
    am.getRegionStates().isRegionInState(hri, State.OPEN);
    // TODO: What else can we do to ensure just a noop.
  }

  @Test
  public void testUnassignAnUnassignedRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testUnassignAnUnassignedRegion");
    final HRegionInfo hri = createRegionInfo(tableName, 1);

    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());

    // assign the region first
    waitOnFuture(submitProcedure(am.createAssignProcedure(hri, false)));

    final Future<byte[]> futureA = submitProcedure(am.createUnassignProcedure(hri, null, false));

    // Wait first unassign.
    waitOnFuture(futureA);
    am.getRegionStates().isRegionInState(hri, State.CLOSED);
    // Second should be a noop. We should recognize region is already CLOSED internally
    // and skip out doing nothing.
    final Future<byte[]> futureB = submitProcedure(am.createUnassignProcedure(hri, null, false));
    waitOnFuture(futureB);
    // Ensure we are still CLOSED.
    am.getRegionStates().isRegionInState(hri, State.CLOSED);
    // TODO: What else can we do to ensure just a noop.
  }

  private Future<byte[]> submitProcedure(final Procedure proc) {
    return ProcedureSyncWait.submitProcedure(master.getMasterProcedureExecutor(), proc);
  }

  private byte[] waitOnFuture(final Future<byte[]> future) throws Exception {
    try {
      return future.get();
    } catch (ExecutionException e) {
      throw (Exception)e.getCause();
    }
  }

  // ============================================================================================
  //  Helpers
  // ============================================================================================
  private void bulkSubmit(final AssignProcedure[] procs) throws Exception {
    final Thread[] threads = new Thread[PROC_NTHREADS];
    for (int i = 0; i < threads.length; ++i) {
      final int threadId = i;
      threads[i] = new Thread() {
        @Override
        public void run() {
          TableName tableName = TableName.valueOf("table-" + threadId);
          int n = (procs.length / threads.length);
          int start = threadId * n;
          int stop = start + n;
          for (int j = start; j < stop; ++j) {
            procs[j] = createAndSubmitAssign(tableName, j);
          }
        }
      };
      threads[i].start();
    }
    for (int i = 0; i < threads.length; ++i) {
      threads[i].join();
    }
    for (int i = procs.length - 1; i >= 0 && procs[i] == null; --i) {
      procs[i] = createAndSubmitAssign(TableName.valueOf("table-sync"), i);
    }
  }

  private AssignProcedure createAndSubmitAssign(TableName tableName, int regionId) {
    HRegionInfo hri = createRegionInfo(tableName, regionId);
    AssignProcedure proc = am.createAssignProcedure(hri, false);
    master.getMasterProcedureExecutor().submitProcedure(proc);
    return proc;
  }

  private UnassignProcedure createAndSubmitUnassign(TableName tableName, int regionId) {
    HRegionInfo hri = createRegionInfo(tableName, regionId);
    UnassignProcedure proc = am.createUnassignProcedure(hri, null, false);
    master.getMasterProcedureExecutor().submitProcedure(proc);
    return proc;
  }

  private HRegionInfo createRegionInfo(final TableName tableName, final long regionId) {
    return new HRegionInfo(tableName,
      Bytes.toBytes(regionId), Bytes.toBytes(regionId + 1), false, 0);
  }

  private void sendTransitionReport(final ServerName serverName,
      final RegionInfo regionInfo, final TransitionCode state) throws IOException {
    ReportRegionStateTransitionRequest.Builder req =
      ReportRegionStateTransitionRequest.newBuilder();
    req.setServer(ProtobufUtil.toServerName(serverName));
    req.addTransition(RegionStateTransition.newBuilder()
      .addRegionInfo(regionInfo)
      .setTransitionCode(state)
      .setOpenSeqNum(1)
      .build());
    am.reportRegionStateTransition(req.build());
  }

  private class NoopRsExecutor implements MockRSExecutor {
    public ExecuteProceduresResponse sendRequest(ServerName server,
        ExecuteProceduresRequest request) throws IOException {
      ExecuteProceduresResponse.Builder builder = ExecuteProceduresResponse.newBuilder();
      if (request.getOpenRegionCount() > 0) {
        for (OpenRegionRequest req: request.getOpenRegionList()) {
          OpenRegionResponse.Builder resp = OpenRegionResponse.newBuilder();
          for (RegionOpenInfo openReq: req.getOpenInfoList()) {
            RegionOpeningState state = execOpenRegion(server, openReq);
            if (state != null) {
              resp.addOpeningState(state);
            }
          }
          builder.addOpenRegion(resp.build());
        }
      }
      if (request.getCloseRegionCount() > 0) {
        for (CloseRegionRequest req: request.getCloseRegionList()) {
          CloseRegionResponse resp = execCloseRegion(server,
              req.getRegion().getValue().toByteArray());
          if (resp != null) {
            builder.addCloseRegion(resp);
          }
        }
      }
      return ExecuteProceduresResponse.newBuilder().build();
    }

    protected RegionOpeningState execOpenRegion(ServerName server, RegionOpenInfo regionInfo)
        throws IOException {
      return null;
    }

    protected CloseRegionResponse execCloseRegion(ServerName server, byte[] regionName)
        throws IOException {
      return null;
    }
  }

  private class GoodRsExecutor extends NoopRsExecutor {
    @Override
    protected RegionOpeningState execOpenRegion(ServerName server, RegionOpenInfo openReq)
        throws IOException {
      sendTransitionReport(server, openReq.getRegion(), TransitionCode.OPENED);
      // Concurrency?
      // Now update the state of our cluster in regionsToRegionServers.
      SortedSet<byte []> regions = regionsToRegionServers.get(server);
      if (regions == null) {
        regions = new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);
        regionsToRegionServers.put(server, regions);
      }
      HRegionInfo hri = HRegionInfo.convert(openReq.getRegion());
      if (regions.contains(hri.getRegionName())) {
        throw new UnsupportedOperationException(hri.getRegionNameAsString());
      }
      regions.add(hri.getRegionName());
      return RegionOpeningState.OPENED;
    }

    @Override
    protected CloseRegionResponse execCloseRegion(ServerName server, byte[] regionName)
        throws IOException {
      HRegionInfo hri = am.getRegionInfo(regionName);
      sendTransitionReport(server, HRegionInfo.convert(hri), TransitionCode.CLOSED);
      return CloseRegionResponse.newBuilder().setClosed(true).build();
    }
  }

  private static class ServerNotYetRunningRsExecutor implements MockRSExecutor {
    public ExecuteProceduresResponse sendRequest(ServerName server, ExecuteProceduresRequest req)
        throws IOException {
      throw new ServerNotRunningYetException("wait on server startup");
    }
  }

  private static class FaultyRsExecutor implements MockRSExecutor {
    private final IOException exception;

    public FaultyRsExecutor(final IOException exception) {
      this.exception = exception;
    }

    public ExecuteProceduresResponse sendRequest(ServerName server, ExecuteProceduresRequest req)
        throws IOException {
      throw exception;
    }
  }

  private class SocketTimeoutRsExecutor extends GoodRsExecutor {
    private final int maxSocketTimeoutRetries;
    private final int maxServerRetries;

    private ServerName lastServer;
    private int sockTimeoutRetries;
    private int serverRetries;

    public SocketTimeoutRsExecutor(int maxSocketTimeoutRetries, int maxServerRetries) {
      this.maxServerRetries = maxServerRetries;
      this.maxSocketTimeoutRetries = maxSocketTimeoutRetries;
    }

    public ExecuteProceduresResponse sendRequest(ServerName server, ExecuteProceduresRequest req)
        throws IOException {
      // SocketTimeoutException should be a temporary problem
      // unless the server will be declared dead.
      if (sockTimeoutRetries++ < maxSocketTimeoutRetries) {
        if (sockTimeoutRetries == 1) assertNotEquals(lastServer, server);
        lastServer = server;
        LOG.debug("Socket timeout for server=" + server + " retries=" + sockTimeoutRetries);
        throw new SocketTimeoutException("simulate socket timeout");
      } else if (serverRetries++ < maxServerRetries) {
        LOG.info("Mark server=" + server + " as dead. serverRetries=" + serverRetries);
        master.getServerManager().moveFromOnlineToDeadServers(server);
        sockTimeoutRetries = 0;
        throw new SocketTimeoutException("simulate socket timeout");
      } else {
        return super.sendRequest(server, req);
      }
    }
  }

  private class RandRsExecutor extends NoopRsExecutor {
    private final Random rand = new Random();

    public ExecuteProceduresResponse sendRequest(ServerName server, ExecuteProceduresRequest req)
        throws IOException {
      switch (rand.nextInt(5)) {
        case 0: throw new ServerNotRunningYetException("wait on server startup");
        case 1: throw new SocketTimeoutException("simulate socket timeout");
        case 2: throw new RemoteException("java.io.IOException", "unexpected exception");
      }
      return super.sendRequest(server, req);
    }

    @Override
    protected RegionOpeningState execOpenRegion(ServerName server, RegionOpenInfo openReq)
        throws IOException {
      switch (rand.nextInt(6)) {
        case 0:
          return OpenRegionResponse.RegionOpeningState.OPENED;
        case 1:
          sendTransitionReport(server, openReq.getRegion(), TransitionCode.OPENED);
          return OpenRegionResponse.RegionOpeningState.ALREADY_OPENED;
        case 2:
          sendTransitionReport(server, openReq.getRegion(), TransitionCode.FAILED_OPEN);
          return OpenRegionResponse.RegionOpeningState.FAILED_OPENING;
      }
      return null;
    }

    @Override
    protected CloseRegionResponse execCloseRegion(ServerName server, byte[] regionName)
        throws IOException {
      CloseRegionResponse.Builder resp = CloseRegionResponse.newBuilder();
      boolean closed = rand.nextBoolean();
      if (closed) {
        HRegionInfo hri = am.getRegionInfo(regionName);
        sendTransitionReport(server, HRegionInfo.convert(hri), TransitionCode.CLOSED);
      }
      resp.setClosed(closed);
      return resp.build();
    }
  }

  private interface MockRSExecutor {
    ExecuteProceduresResponse sendRequest(ServerName server, ExecuteProceduresRequest req)
        throws IOException;
  }

  private class MockRSProcedureDispatcher extends RSProcedureDispatcher {
    private MockRSExecutor mockRsExec;

    public MockRSProcedureDispatcher(final MasterServices master) {
      super(master);
    }

    public void setMockRsExecutor(final MockRSExecutor mockRsExec) {
      this.mockRsExec = mockRsExec;
    }

    @Override
    protected void remoteDispatch(ServerName serverName, Set<RemoteProcedure> operations) {
      submitTask(new MockRemoteCall(serverName, operations));
    }

    private class MockRemoteCall extends ExecuteProceduresRemoteCall {
      public MockRemoteCall(final ServerName serverName,
          final Set<RemoteProcedure> operations) {
        super(serverName, operations);
      }

      @Override
      protected ExecuteProceduresResponse sendRequest(final ServerName serverName,
          final ExecuteProceduresRequest request) throws IOException {
        return mockRsExec.sendRequest(serverName, request);
      }
    }
  }
}
