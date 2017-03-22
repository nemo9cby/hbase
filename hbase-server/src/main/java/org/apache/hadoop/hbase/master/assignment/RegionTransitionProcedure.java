/**
 *
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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.master.assignment.RegionStates.RegionStateNode;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionTransitionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * Base class for the Assign and Unassign Procedure.
 * There can only be one RegionTransitionProcedure per region running at a time
 * since each procedure takes a lock on the region (see MasterProcedureScheduler).
 *
 * <p>This procedure is asynchronous and responds to external events.
 * The AssignmentManager will notify this procedure when the RS completes
 * the operation and reports the transitioned state
 * (see the Assign and Unassign class for more details).
 */
@InterfaceAudience.Private
public abstract class RegionTransitionProcedure
    extends Procedure<MasterProcedureEnv>
    implements TableProcedureInterface,
      RemoteProcedure<MasterProcedureEnv, ServerName> {
  private static final Log LOG = LogFactory.getLog(RegionTransitionProcedure.class);

  protected final AtomicBoolean aborted = new AtomicBoolean(false);

  private RegionTransitionState transitionState =
      RegionTransitionState.REGION_TRANSITION_QUEUE;
  private RegionStateNode regionNode = null;
  private HRegionInfo regionInfo;
  private boolean hasLock = false;

  public RegionTransitionProcedure() {
    // Required by the Procedure framework to create the procedure on replay
  }

  public RegionTransitionProcedure(final HRegionInfo regionInfo) {
    this.regionInfo = regionInfo;
  }

  public HRegionInfo getRegionInfo() {
    return regionInfo;
  }

  protected void setRegionInfo(final HRegionInfo regionInfo) {
    // Setter is for deserialization.
    this.regionInfo = regionInfo;
  }

  @Override
  public TableName getTableName() {
    return getRegionInfo().getTable();
  }

  public boolean isMeta() {
    return TableName.isMetaTableName(getTableName());
  }

  @Override
  public void toStringClassDetails(final StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" table=");
    sb.append(getTableName());
    sb.append(", region=");
    sb.append(getRegionInfo().getEncodedName());
    if (regionNode != null) {
      sb.append(", server=");
      sb.append(regionNode.getRegionLocation());
    }
  }

  public RegionStateNode getRegionState(final MasterProcedureEnv env) {
    if (regionNode == null) {
      regionNode = env.getAssignmentManager()
        .getRegionStates().getOrCreateRegionNode(getRegionInfo());
    }
    return regionNode;
  }

  protected void setTransitionState(final RegionTransitionState state) {
    this.transitionState = state;
  }

  protected RegionTransitionState getTransitionState() {
    return transitionState;
  }

  protected abstract boolean startTransition(MasterProcedureEnv env, RegionStateNode regionNode)
    throws IOException, ProcedureSuspendedException;
  protected abstract boolean updateTransition(MasterProcedureEnv env, RegionStateNode regionNode)
    throws IOException, ProcedureSuspendedException;
  protected abstract void completeTransition(MasterProcedureEnv env, RegionStateNode regionNode)
    throws IOException, ProcedureSuspendedException;

  protected abstract void reportTransition(MasterProcedureEnv env,
      RegionStateNode regionNode, TransitionCode code, long seqId) throws UnexpectedStateException;

  public abstract RemoteOperation remoteCallBuild(MasterProcedureEnv env, ServerName serverName);
  protected abstract void remoteCallFailed(MasterProcedureEnv env,
      RegionStateNode regionNode, IOException exception);

  @Override
  public void remoteCallCompleted(final MasterProcedureEnv env,
      final ServerName serverName, final RemoteOperation response) {
    // Ignore the response? reportTransition() is the one that count?
  }

  @Override
  public void remoteCallFailed(final MasterProcedureEnv env,
      final ServerName serverName, final IOException exception) {
    final RegionStateNode regionNode = getRegionState(env);
    assert serverName.equals(regionNode.getRegionLocation()); // TODO
    LOG.warn("Remote call failed " + regionNode + ": " + exception.getMessage());
    remoteCallFailed(env, regionNode, exception);
    env.getProcedureScheduler().wakeEvent(regionNode.getProcedureEvent());
  }

  protected void addToRemoteDispatcher(final MasterProcedureEnv env,
      final ServerName targetServer) {
    assert targetServer.equals(getRegionState(env).getRegionLocation()) :
      "targetServer=" + targetServer + " getRegionLocation=" + getRegionState(env).getRegionLocation(); // TODO

    LOG.info("ADD TO REMOTE DISPATCHER " + getRegionState(env) + ": " + targetServer);

    // Add the open region operation to the server dispatch queue.
    // The pending close will be dispatched to the server together with the other
    // pending operation for that server.
    env.getProcedureScheduler().suspendEvent(getRegionState(env).getProcedureEvent());

    // TODO: If the server is gone... go on failure/retry
    env.getRemoteDispatcher().getNode(targetServer).add(this);
  }

  protected void reportTransition(final MasterProcedureEnv env, final ServerName serverName,
      final TransitionCode code, final long seqId) throws UnexpectedStateException {
    final RegionStateNode regionNode = getRegionState(env);
    if (!serverName.equals(regionNode.getRegionLocation())) {
      if (isMeta() && regionNode.getRegionLocation() == null) {
        regionNode.setRegionLocation(serverName);
      } else {
        throw new UnexpectedStateException(String.format(
          "reported unexpected transition state=%s from server=%s on region=%s, expected server=%s",
          code, serverName, regionNode.getRegionInfo(), regionNode.getRegionLocation()));
      }
    }

    reportTransition(env, regionNode, code, seqId);
    env.getProcedureScheduler().wakeEvent(regionNode.getProcedureEvent());
  }

  protected boolean isServerOnline(final MasterProcedureEnv env, final RegionStateNode regionNode) {
    return isServerOnline(env, regionNode.getRegionLocation());
  }

  protected boolean isServerOnline(final MasterProcedureEnv env, final ServerName serverName) {
    return env.getMasterServices().getServerManager().isServerOnline(serverName);
  }

  @Override
  protected Procedure[] execute(final MasterProcedureEnv env) throws ProcedureSuspendedException {
    final AssignmentManager am = env.getAssignmentManager();
    final RegionStateNode regionNode = getRegionState(env);
    LOG.debug("" + transitionState + " " + this);
    if (!am.addRegionInTransition(regionNode, this)) {
      String msg = String.format(
        "There is already another procedure running on this region this=%s owner=%s",
        this, regionNode.getProcedure());
      LOG.warn(msg);
      setAbortFailure(getClass().getSimpleName(), msg);
      return null;
    }
    try {
      boolean retry;
      do {
        retry = false;
        switch (transitionState) {
          case REGION_TRANSITION_QUEUE:
            // 1. push into the AM queue for balancer policy
            if (!startTransition(env, regionNode)) {
              // the operation aborted, check getException()
              am.removeRegionInTransition(getRegionState(env), this);
              return null;
            }
            transitionState = RegionTransitionState.REGION_TRANSITION_DISPATCH;
            if (env.getProcedureScheduler().waitEvent(regionNode.getProcedureEvent(), this)) {
              throw new ProcedureSuspendedException();
            }
            break;

          case REGION_TRANSITION_DISPATCH:
            // 2. send the request to the target server
            if (!updateTransition(env, regionNode)) {
              // The operation aborted, check getException()
              am.removeRegionInTransition(regionNode, this);
              return null;
            }
            if (transitionState != RegionTransitionState.REGION_TRANSITION_DISPATCH) {
              retry = true;
              break;
            }
            if (env.getProcedureScheduler().waitEvent(regionNode.getProcedureEvent(), this)) {
              throw new ProcedureSuspendedException();
            }
            break;

          case REGION_TRANSITION_FINISH:
            // 3. wait assignment response. completion/failure
            completeTransition(env, regionNode);
            am.removeRegionInTransition(regionNode, this);
            return null;
        }
      } while (retry);
    } catch (IOException e) {
      LOG.warn("Retryable error trying to transition: " + regionNode, e);
    }

    return new Procedure[] { this };
  }

  @Override
  protected void rollback(final MasterProcedureEnv env) {
    if (isRollbackSupported(transitionState)) {
      // Nothing done up to this point. abort safely.
      // This should happen when something like disableTable() is triggered.
      env.getAssignmentManager().removeRegionInTransition(regionNode, this);
      return;
    }

    // There is no rollback for assignment unless we cancel the operation by
    // dropping/disabling the table.
    throw new UnsupportedOperationException("unhandled state " + transitionState);
  }

  protected abstract boolean isRollbackSupported(final RegionTransitionState state);

  @Override
  protected boolean abort(final MasterProcedureEnv env) {
    if (isRollbackSupported(transitionState)) {
      aborted.set(true);
      return true;
    }
    return false;
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    // Unless we are assigning meta, wait for meta to be available and loaded.
    if (!isMeta() && (env.waitFailoverCleanup(this) ||
        env.getAssignmentManager().waitMetaInitialized(this, getRegionInfo()))) {
      return LockState.LOCK_EVENT_WAIT;
    }

    // TODO: Revisit this and move it to the executor
    if (env.getProcedureScheduler().waitRegion(this, getRegionInfo())) {
      return LockState.LOCK_EVENT_WAIT;
    }
    this.hasLock = true;
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureScheduler().wakeRegion(this, getRegionInfo());
    hasLock = false;
  }

  protected boolean holdLock(final MasterProcedureEnv env) {
    return true;
  }

  protected boolean hasLock(final MasterProcedureEnv env) {
    return hasLock;
  }

  @Override
  protected boolean shouldWaitClientAck(MasterProcedureEnv env) {
    // The operation is triggered internally on the server
    // the client does not know about this procedure.
    return false;
  }
}
