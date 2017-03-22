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

import java.io.IOException;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.SortedSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MasterWalManager;
import org.apache.hadoop.hbase.master.MockNoopMasterServices;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.store.NoopProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.security.Superusers;

public class MockMasterServices extends MockNoopMasterServices {
  private final MasterFileSystem fileSystemManager;
  private final MasterWalManager walManager;
  private final AssignmentManager assignmentManager;

  private MasterProcedureEnv procedureEnv;
  private ProcedureExecutor<MasterProcedureEnv> procedureExecutor;
  private ProcedureStore procedureStore;

  private LoadBalancer balancer;
  private ServerManager serverManager;
  // Set of regions on a 'server'. Populated externally. Used in below faking 'cluster'.
  private final NavigableMap<ServerName, SortedSet<byte []>> regionsToRegionServers;

  public MockMasterServices(Configuration conf,
      NavigableMap<ServerName, SortedSet<byte []>> regionsToRegionServers)
  throws IOException {
    super(conf);
    this.regionsToRegionServers = regionsToRegionServers;
    Superusers.initialize(conf);
    this.fileSystemManager = new MasterFileSystem(this);
    this.walManager = new MasterWalManager(this);
    this.assignmentManager = new AssignmentManager(this, new MockRegionStateStore(this)) {
      public boolean isTableEnabled(final TableName tableName) {
        return true;
      }

      public boolean isTableDisabled(final TableName tableName) {
        return false;
      }

      @Override
      protected boolean waitServerReportEvent(ServerName serverName, Procedure proc) {
        // Make a report with current state of the server 'serverName' before we call wait..
        SortedSet<byte []> regions = regionsToRegionServers.get(serverName);
        getAssignmentManager().reportOnlineRegions(serverName, 0,
            regions == null? new HashSet<byte []>(): regions);
        return super.waitServerReportEvent(serverName, proc);
      }
    };
    this.balancer = LoadBalancerFactory.getLoadBalancer(conf);
    this.serverManager = new ServerManager(this);
  }

  public void start(final int numServes, final RSProcedureDispatcher remoteDispatcher)
      throws IOException {
    startProcedureExecutor(remoteDispatcher);
    assignmentManager.start();
    for (int i = 0; i < numServes; ++i) {
      serverManager.regionServerReport(
        ServerName.valueOf("localhost", 100 + i, 1), ServerLoad.EMPTY_SERVERLOAD);
    }
  }

  @Override
  public void stop(String why) {
    stopProcedureExecutor();
    this.assignmentManager.stop();
  }

  private void startProcedureExecutor(final RSProcedureDispatcher remoteDispatcher)
      throws IOException {
    final Configuration conf = getConfiguration();
    final Path logDir = new Path(fileSystemManager.getRootDir(),
        MasterProcedureConstants.MASTER_PROCEDURE_LOGDIR);

    //procedureStore = new WALProcedureStore(conf, fileSystemManager.getFileSystem(), logDir,
    //    new MasterProcedureEnv.WALStoreLeaseRecovery(this));
    procedureStore = new NoopProcedureStore();
    procedureStore.registerListener(new MasterProcedureEnv.MasterProcedureStoreListener(this));

    procedureEnv = new MasterProcedureEnv(this,
       remoteDispatcher != null ? remoteDispatcher : new RSProcedureDispatcher(this));

    procedureExecutor = new ProcedureExecutor(conf, procedureEnv, procedureStore,
        procedureEnv.getProcedureScheduler());

    final int numThreads = conf.getInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS,
        Math.max(Runtime.getRuntime().availableProcessors(),
          MasterProcedureConstants.DEFAULT_MIN_MASTER_PROCEDURE_THREADS));
    final boolean abortOnCorruption = conf.getBoolean(
        MasterProcedureConstants.EXECUTOR_ABORT_ON_CORRUPTION,
        MasterProcedureConstants.DEFAULT_EXECUTOR_ABORT_ON_CORRUPTION);
    procedureStore.start(numThreads);
    procedureExecutor.start(numThreads, abortOnCorruption);
    procedureEnv.getRemoteDispatcher().start();
  }

  private void stopProcedureExecutor() {
    if (procedureEnv != null) {
      procedureEnv.getRemoteDispatcher().stop();
    }

    if (procedureExecutor != null) {
      procedureExecutor.stop();
    }

    if (procedureStore != null) {
      procedureStore.stop(isAborted());
    }
  }

  @Override
  public boolean isInitialized() {
    return true;
  }

  @Override
  public MasterFileSystem getMasterFileSystem() {
    return fileSystemManager;
  }

  @Override
  public MasterWalManager getMasterWalManager() {
    return walManager;
  }

  @Override
  public ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return procedureExecutor;
  }

  @Override
  public LoadBalancer getLoadBalancer() {
    return balancer;
  }

  @Override
  public ServerManager getServerManager() {
    return serverManager;
  }

  @Override
  public AssignmentManager getAssignmentManager() {
    return assignmentManager;
  }

  private static class MockRegionStateStore extends RegionStateStore {
    public MockRegionStateStore(final MasterServices master) {
      super(master);
    }

    public void start() throws IOException {
    }

    public void stop() {
    }

    public void updateRegionLocation(final HRegionInfo regionInfo, final State state,
      final ServerName regionLocation, final ServerName lastHost, final long openSeqNum)
      throws IOException {
    }
  }
}
