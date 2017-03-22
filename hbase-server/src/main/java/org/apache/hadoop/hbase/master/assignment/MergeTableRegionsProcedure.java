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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.exceptions.MergeRegionException;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.CatalogJanitor;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineTableProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MergeTableRegionsState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * The procedure to Merge a region in a table.
 */
@InterfaceAudience.Private
public class MergeTableRegionsProcedure
    extends AbstractStateMachineTableProcedure<MergeTableRegionsState> {
  private static final Log LOG = LogFactory.getLog(MergeTableRegionsProcedure.class);

  private Boolean traceEnabled;

  private ServerName regionLocation;
  private String regionsToMergeListFullName;

  private HRegionInfo[] regionsToMerge;
  private HRegionInfo mergedRegion;
  private boolean forcible;

  public MergeTableRegionsProcedure() {
    // Required by the Procedure framework to create the procedure on replay
  }

  public MergeTableRegionsProcedure(final MasterProcedureEnv env,
      final HRegionInfo regionToMergeA, final HRegionInfo regionToMergeB) throws IOException {
    this(env, regionToMergeA, regionToMergeB, false);
  }

  public MergeTableRegionsProcedure(final MasterProcedureEnv env,
      final HRegionInfo regionToMergeA, final HRegionInfo regionToMergeB,
      final boolean forcible) throws MergeRegionException {
    this(env, new HRegionInfo[] {regionToMergeA, regionToMergeB}, forcible);
  }

  public MergeTableRegionsProcedure(final MasterProcedureEnv env,
      final HRegionInfo[] regionsToMerge, final boolean forcible)
      throws MergeRegionException {
    super(env);

    // Check daughter regions and make sure that we have valid daughter regions
    // before doing the real work.
    checkRegionsToMerge(regionsToMerge, forcible);

    // WARN: make sure there is no parent region of the two merging regions in
    // hbase:meta If exists, fixing up daughters would cause daughter regions(we
    // have merged one) online again when we restart master, so we should clear
    // the parent region to prevent the above case
    // Since HBASE-7721, we don't need fix up daughters any more. so here do nothing
    this.regionsToMerge = regionsToMerge;
    this.mergedRegion = createMergedRegionInfo(regionsToMerge);
    this.forcible = forcible;

    this.regionsToMergeListFullName = getRegionsToMergeListFullNameString();
  }

  private static void checkRegionsToMerge(final HRegionInfo[] regionsToMerge,
      final boolean forcible) throws MergeRegionException {
    // For now, we only merge 2 regions.
    // It could be extended to more than 2 regions in the future.
    if (regionsToMerge == null || regionsToMerge.length != 2) {
      throw new MergeRegionException("Expected to merge 2 regions, got: " +
        Arrays.toString(regionsToMerge));
    }

    checkRegionsToMerge(regionsToMerge[0], regionsToMerge[1], forcible);
  }

  private static void checkRegionsToMerge(final HRegionInfo regionToMergeA,
      final HRegionInfo regionToMergeB, final boolean forcible) throws MergeRegionException {
    if (!regionToMergeA.getTable().equals(regionToMergeB.getTable())) {
      throw new MergeRegionException("Can't merge regions from two different tables: " +
        regionToMergeA + ", " + regionToMergeB);
    }

    if (regionToMergeA.getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID ||
        regionToMergeB.getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID) {
      throw new MergeRegionException("Can't merge non-default replicas");
    }

    if (!HRegionInfo.areAdjacent(regionToMergeA, regionToMergeB)) {
      String msg = "Unable to merge not adjacent regions " + regionToMergeA.getShortNameToLog() +
          ", " + regionToMergeB.getShortNameToLog() + " where forcible = " + forcible;
      LOG.warn(msg);
      if (!forcible) {
        throw new MergeRegionException(msg);
      }
    }
  }

  private static HRegionInfo createMergedRegionInfo(final HRegionInfo[] regionsToMerge) {
    return createMergedRegionInfo(regionsToMerge[0], regionsToMerge[1]);
  }

  /**
   * Create merged region info through the specified two regions
   */
  private static HRegionInfo createMergedRegionInfo(final HRegionInfo regionToMergeA,
      final HRegionInfo regionToMergeB) {
    // Choose the smaller as start key
    final byte[] startKey;
    if (regionToMergeA.compareTo(regionToMergeB) <= 0) {
      startKey = regionToMergeA.getStartKey();
    } else {
      startKey = regionToMergeB.getStartKey();
    }

    // Choose the bigger as end key
    final byte[] endKey;
    if (Bytes.equals(regionToMergeA.getEndKey(), HConstants.EMPTY_BYTE_ARRAY)
        || (!Bytes.equals(regionToMergeB.getEndKey(), HConstants.EMPTY_BYTE_ARRAY)
            && Bytes.compareTo(regionToMergeA.getEndKey(), regionToMergeB.getEndKey()) > 0)) {
      endKey = regionToMergeA.getEndKey();
    } else {
      endKey = regionToMergeB.getEndKey();
    }

    // Merged region is sorted between two merging regions in META
    final long rid = getMergedRegionIdTimestamp(regionToMergeA, regionToMergeB);
    return new HRegionInfo(regionToMergeA.getTable(), startKey, endKey, false, rid);
  }

  private static long getMergedRegionIdTimestamp(final HRegionInfo regionToMergeA,
      final HRegionInfo regionToMergeB) {
    long rid = EnvironmentEdgeManager.currentTime();
    // Regionid is timestamp. Merged region's id can't be less than that of
    // merging regions else will insert at wrong location in hbase:meta (See HBASE-710).
    if (rid < regionToMergeA.getRegionId() || rid < regionToMergeB.getRegionId()) {
      LOG.warn("Clock skew; merging regions id are " + regionToMergeA.getRegionId()
          + " and " + regionToMergeB.getRegionId() + ", but current time here is " + rid);
      rid = Math.max(regionToMergeA.getRegionId(), regionToMergeB.getRegionId()) + 1;
    }
    return rid;
  }

  @Override
  protected Flow executeFromState(
      final MasterProcedureEnv env,
      final MergeTableRegionsState state) throws InterruptedException {
    if (isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }
    try {
      switch (state) {
      case MERGE_TABLE_REGIONS_PREPARE:
        prepareMergeRegion(env);
        setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_PRE_MERGE_OPERATION);
        break;
      case MERGE_TABLE_REGIONS_PRE_MERGE_OPERATION:
        preMergeRegions(env);
        setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_SET_MERGING_TABLE_STATE);
        break;
      case MERGE_TABLE_REGIONS_SET_MERGING_TABLE_STATE:
        setRegionStateToMerging(env);
        setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_CLOSE_REGIONS);
        break;
      case MERGE_TABLE_REGIONS_CLOSE_REGIONS:
        addChildProcedure(createUnassignProcedures(env, getRegionReplication(env)));
        setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_CREATE_MERGED_REGION);
        break;
      case MERGE_TABLE_REGIONS_CREATE_MERGED_REGION:
        createMergedRegion(env);
        setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_PRE_MERGE_COMMIT_OPERATION);
        break;
      case MERGE_TABLE_REGIONS_PRE_MERGE_COMMIT_OPERATION:
        preMergeRegionsCommit(env);
        setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_UPDATE_META);
        break;
      case MERGE_TABLE_REGIONS_UPDATE_META:
        updateMetaForMergedRegions(env);
        setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_POST_MERGE_COMMIT_OPERATION);
        break;
      case MERGE_TABLE_REGIONS_POST_MERGE_COMMIT_OPERATION:
        postMergeRegionsCommit(env);
        setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_OPEN_MERGED_REGION);
        break;
      case MERGE_TABLE_REGIONS_OPEN_MERGED_REGION:
        addChildProcedure(createAssignProcedures(env, getRegionReplication(env)));
        setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_POST_OPERATION);
        break;
      case MERGE_TABLE_REGIONS_POST_OPERATION:
        postCompletedMergeRegions(env);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      LOG.warn("Error trying to merge regions " + getRegionsToMergeListFullNameString() +
        " in the table " + getTableName() + " (in state=" + state + ")", e);

      setFailure("master-merge-regions", e);
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(
      final MasterProcedureEnv env,
      final MergeTableRegionsState state) throws IOException, InterruptedException {
    if (isTraceEnabled()) {
      LOG.trace(this + " rollback state=" + state);
    }

    try {
      switch (state) {
      case MERGE_TABLE_REGIONS_POST_OPERATION:
      case MERGE_TABLE_REGIONS_OPEN_MERGED_REGION:
      case MERGE_TABLE_REGIONS_POST_MERGE_COMMIT_OPERATION:
      case MERGE_TABLE_REGIONS_UPDATE_META:
        String msg = this + " We are in the " + state + " state."
            + " It is complicated to rollback the merge operation that region server is working on."
            + " Rollback is not supported and we should let the merge operation to complete";
        LOG.warn(msg);
        // PONR
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      case MERGE_TABLE_REGIONS_PRE_MERGE_COMMIT_OPERATION:
        break;
      case MERGE_TABLE_REGIONS_CREATE_MERGED_REGION:
        cleanupMergedRegion(env);
        break;
      case MERGE_TABLE_REGIONS_CLOSE_REGIONS:
        rollbackCloseRegionsForMerge(env);
        break;
      case MERGE_TABLE_REGIONS_SET_MERGING_TABLE_STATE:
        setRegionStateToRevertMerging(env);
        break;
      case MERGE_TABLE_REGIONS_PRE_MERGE_OPERATION:
        postRollBackMergeRegions(env);
        break;
      case MERGE_TABLE_REGIONS_MOVE_REGION_TO_SAME_RS:
        break; // nothing to rollback
      case MERGE_TABLE_REGIONS_PREPARE:
        break; // nothing to rollback
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (Exception e) {
      // This will be retried. Unless there is a bug in the code,
      // this should be just a "temporary error" (e.g. network down)
      LOG.warn("Failed rollback attempt step " + state + " for merging the regions "
          + getRegionsToMergeListFullNameString() + " in table " + getTableName(), e);
      throw e;
    }
  }

  /*
   * Check whether we are in the state that can be rollback
   */
  @Override
  protected boolean isRollbackSupported(final MergeTableRegionsState state) {
    switch (state) {
    case MERGE_TABLE_REGIONS_POST_OPERATION:
    case MERGE_TABLE_REGIONS_OPEN_MERGED_REGION:
    case MERGE_TABLE_REGIONS_POST_MERGE_COMMIT_OPERATION:
    case MERGE_TABLE_REGIONS_UPDATE_META:
        // It is not safe to rollback if we reach to these states.
        return false;
      default:
        break;
    }
    return true;
  }

  @Override
  protected MergeTableRegionsState getState(final int stateId) {
    return MergeTableRegionsState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final MergeTableRegionsState state) {
    return state.getNumber();
  }

  @Override
  protected MergeTableRegionsState getInitialState() {
    return MergeTableRegionsState.MERGE_TABLE_REGIONS_PREPARE;
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    final MasterProcedureProtos.MergeTableRegionsStateData.Builder mergeTableRegionsMsg =
        MasterProcedureProtos.MergeTableRegionsStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setMergedRegionInfo(HRegionInfo.convert(mergedRegion))
        .setForcible(forcible);
    for (int i = 0; i < regionsToMerge.length; ++i) {
      mergeTableRegionsMsg.addRegionInfo(HRegionInfo.convert(regionsToMerge[i]));
    }
    mergeTableRegionsMsg.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    final MasterProcedureProtos.MergeTableRegionsStateData mergeTableRegionsMsg =
        MasterProcedureProtos.MergeTableRegionsStateData.parseDelimitedFrom(stream);
    setUser(MasterProcedureUtil.toUserInfo(mergeTableRegionsMsg.getUserInfo()));

    assert(mergeTableRegionsMsg.getRegionInfoCount() == 2);
    regionsToMerge = new HRegionInfo[mergeTableRegionsMsg.getRegionInfoCount()];
    for (int i = 0; i < regionsToMerge.length; i++) {
      regionsToMerge[i] = HRegionInfo.convert(mergeTableRegionsMsg.getRegionInfo(i));
    }

    mergedRegion = HRegionInfo.convert(mergeTableRegionsMsg.getMergedRegionInfo());
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append("(table=");
    sb.append(getTableName());
    sb.append(" regions=");
    sb.append(getRegionsToMergeListFullNameString());
    sb.append(" forcible=");
    sb.append(forcible);
    sb.append(")");
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    if (env.waitInitialized(this)) return LockState.LOCK_EVENT_WAIT;

    if (env.getProcedureScheduler().waitRegions(this, getTableName(),
        mergedRegion, regionsToMerge[0], regionsToMerge[1])) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureScheduler().wakeRegions(this, getTableName(),
      mergedRegion, regionsToMerge[0], regionsToMerge[1]);
  }

  @Override
  public TableName getTableName() {
    return mergedRegion.getTable();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.MERGE;
  }

  /**
   * Prepare merge and do some check
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void prepareMergeRegion(final MasterProcedureEnv env) throws IOException {
    // Note: the following logic assumes that we only have 2 regions to merge.  In the future,
    // if we want to extend to more than 2 regions, the code needs to modify a little bit.
    //
    CatalogJanitor catalogJanitor = env.getMasterServices().getCatalogJanitor();
    boolean regionAHasMergeQualifier = !catalogJanitor.cleanMergeQualifier(regionsToMerge[0]);
    if (regionAHasMergeQualifier
        || !catalogJanitor.cleanMergeQualifier(regionsToMerge[1])) {
      String msg = "Skip merging regions " + getRegionsToMergeListFullNameString()
        + ", because region "
        + (regionAHasMergeQualifier ? regionsToMerge[0].getEncodedName() : regionsToMerge[1]
              .getEncodedName()) + " has merge qualifier";
      LOG.warn(msg);
      throw new MergeRegionException(msg);
    }

    RegionStates regionStates = env.getAssignmentManager().getRegionStates();
    RegionState regionStateA = regionStates.getRegionState(regionsToMerge[0].getEncodedName());
    RegionState regionStateB = regionStates.getRegionState(regionsToMerge[1].getEncodedName());
    if (regionStateA == null || regionStateB == null) {
      throw new UnknownRegionException(
        regionStateA == null ?
            regionsToMerge[0].getEncodedName() : regionsToMerge[1].getEncodedName());
    }

    if (!regionStateA.isOpened() || !regionStateB.isOpened()) {
      throw new MergeRegionException(
        "Unable to merge regions not online " + regionStateA + ", " + regionStateB);
    }
  }

  /**
   * Pre merge region action
   * @param env MasterProcedureEnv
   **/
  private void preMergeRegions(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      boolean ret = cpHost.preMergeRegionsAction(regionsToMerge, getUser());
      if (ret) {
        throw new IOException(
          "Coprocessor bypassing regions " + getRegionsToMergeListFullNameString() + " merge.");
      }
    }
  }

  /**
   * Action after rollback a merge table regions action.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void postRollBackMergeRegions(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postRollBackMergeRegionsAction(regionsToMerge, getUser());
    }
  }

  /**
   * Set the region states to MERGING state
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  public void setRegionStateToMerging(final MasterProcedureEnv env) throws IOException {
    //transition.setTransitionCode(TransitionCode.READY_TO_MERGE);
  }

  /**
   * Rollback the region state change
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void setRegionStateToRevertMerging(final MasterProcedureEnv env) throws IOException {
    //transition.setTransitionCode(TransitionCode.MERGE_REVERTED);
  }

  /**
   * Create merged region
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void createMergedRegion(final MasterProcedureEnv env) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tabledir = FSUtils.getTableDir(mfs.getRootDir(), regionsToMerge[0].getTable());
    final FileSystem fs = mfs.getFileSystem();
    HRegionFileSystem regionFs = HRegionFileSystem.openRegionFromFileSystem(
      env.getMasterConfiguration(), fs, tabledir, regionsToMerge[0], false);
    regionFs.createMergesDir();

    mergeStoreFiles(env, regionFs, regionFs.getMergesDir());
    HRegionFileSystem regionFs2 = HRegionFileSystem.openRegionFromFileSystem(
      env.getMasterConfiguration(), fs, tabledir, regionsToMerge[1], false);
    mergeStoreFiles(env, regionFs2, regionFs.getMergesDir());

    regionFs.commitMergedRegion(mergedRegion);
  }

  /**
   * Create reference file(s) of merging regions under the merges directory
   * @param env MasterProcedureEnv
   * @param regionFs region file system
   * @param mergedDir the temp directory of merged region
   * @throws IOException
   */
  private void mergeStoreFiles(
      final MasterProcedureEnv env, final HRegionFileSystem regionFs, final Path mergedDir)
      throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Configuration conf = env.getMasterConfiguration();
    final HTableDescriptor htd = env.getMasterServices().getTableDescriptors().get(getTableName());

    for (String family: regionFs.getFamilies()) {
      final HColumnDescriptor hcd = htd.getFamily(family.getBytes());
      final Collection<StoreFileInfo> storeFiles = regionFs.getStoreFiles(family);

      if (storeFiles != null && storeFiles.size() > 0) {
        final CacheConfig cacheConf = new CacheConfig(conf, hcd);
        for (StoreFileInfo storeFileInfo: storeFiles) {
          // Create reference file(s) of the region in mergedDir
          regionFs.mergeStoreFile(
            mergedRegion,
            family,
            new StoreFile(
              mfs.getFileSystem(), storeFileInfo, conf, cacheConf, hcd.getBloomFilterType()),
            mergedDir);
        }
      }
    }
  }

  /**
   * Clean up merged region
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void cleanupMergedRegion(final MasterProcedureEnv env) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tabledir = FSUtils.getTableDir(mfs.getRootDir(), regionsToMerge[0].getTable());
    final FileSystem fs = mfs.getFileSystem();
    HRegionFileSystem regionFs = HRegionFileSystem.openRegionFromFileSystem(
      env.getMasterConfiguration(), fs, tabledir, regionsToMerge[0], false);
    regionFs.cleanupMergedRegion(mergedRegion);
  }

  /**
   * Rollback close regions
   * @param env MasterProcedureEnv
   **/
  private void rollbackCloseRegionsForMerge(final MasterProcedureEnv env) throws IOException {
    // Check whether the region is closed; if so, open it in the same server
    final int regionReplication = getRegionReplication(env);
    final ServerName serverName = getServerName(env);

    final AssignProcedure[] procs =
        new AssignProcedure[regionsToMerge.length * regionReplication];
    int procsIdx = 0;
    for (int i = 0; i < regionsToMerge.length; ++i) {
      for (int j = 0; j < regionReplication; ++j) {
        final HRegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(regionsToMerge[i], j);
        procs[procsIdx++] = env.getAssignmentManager().createAssignProcedure(hri, serverName);
      }
    }
    env.getMasterServices().getMasterProcedureExecutor().submitProcedures(procs);
  }

  private UnassignProcedure[] createUnassignProcedures(final MasterProcedureEnv env,
      final int regionReplication) {
    final UnassignProcedure[] procs =
        new UnassignProcedure[regionsToMerge.length * regionReplication];
    int procsIdx = 0;
    for (int i = 0; i < regionsToMerge.length; ++i) {
      for (int j = 0; j < regionReplication; ++j) {
        final HRegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(regionsToMerge[i], j);
        procs[procsIdx++] = env.getAssignmentManager().createUnassignProcedure(hri,null,true);
      }
    }
    return procs;
  }

  private AssignProcedure[] createAssignProcedures(final MasterProcedureEnv env,
      final int regionReplication) {
    final ServerName targetServer = getServerName(env);
    final AssignProcedure[] procs = new AssignProcedure[regionReplication];
    for (int i = 0; i < procs.length; ++i) {
      final HRegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(mergedRegion, i);
      procs[i] = env.getAssignmentManager().createAssignProcedure(hri, targetServer);
    }
    return procs;
  }

  private int getRegionReplication(final MasterProcedureEnv env) throws IOException {
    final HTableDescriptor htd = env.getMasterServices().getTableDescriptors().get(getTableName());
    return htd.getRegionReplication();
  }

  /**
   * Post merge region action
   * @param env MasterProcedureEnv
   **/
  private void preMergeRegionsCommit(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      @MetaMutationAnnotation
      final List<Mutation> metaEntries = new ArrayList<Mutation>();
      boolean ret = cpHost.preMergeRegionsCommit(regionsToMerge, metaEntries, getUser());

      if (ret) {
        throw new IOException(
          "Coprocessor bypassing regions " + getRegionsToMergeListFullNameString() + " merge.");
      }
      try {
        for (Mutation p : metaEntries) {
          HRegionInfo.parseRegionName(p.getRow());
        }
      } catch (IOException e) {
        LOG.error("Row key of mutation from coprocessor is not parsable as region name."
          + "Mutations from coprocessor should only be for hbase:meta table.", e);
        throw e;
      }
    }
  }

  /**
   * Add merged region to META and delete original regions.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void updateMetaForMergedRegions(final MasterProcedureEnv env) throws IOException {
    final ServerName serverName = getServerName(env);
    env.getAssignmentManager().markRegionAsMerged(mergedRegion, serverName,
      regionsToMerge[0], regionsToMerge[1]);
  }

  /**
   * Post merge region action
   * @param env MasterProcedureEnv
   **/
  private void postMergeRegionsCommit(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postMergeRegionsCommit(regionsToMerge, mergedRegion, getUser());
    }
  }

  /**
   * Post merge region action
   * @param env MasterProcedureEnv
   **/
  private void postCompletedMergeRegions(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postCompletedMergeRegionsAction(regionsToMerge, mergedRegion, getUser());
    }
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @param env MasterProcedureEnv
   * @return serverName
   */
  private ServerName getServerName(final MasterProcedureEnv env) {
    if (regionLocation == null) {
      regionLocation = env.getAssignmentManager().getRegionStates()
        .getRegionServerOfRegion(regionsToMerge[0]);
    }
    return regionLocation;
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @param fullName whether return only encoded name
   * @return region names in a list
   */
  private String getRegionsToMergeListFullNameString() {
    if (regionsToMergeListFullName == null) {
      final StringBuilder sb = new StringBuilder("[");
      int i = 0;
      while(i < regionsToMerge.length - 1) {
        sb.append(regionsToMerge[i].getRegionNameAsString());
        sb.append(", ");
        i++;
      }
      sb.append(regionsToMerge[i].getRegionNameAsString());
      sb.append("]");
      regionsToMergeListFullName = sb.toString();
    }
    return regionsToMergeListFullName;
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @return traceEnabled
   */
  private Boolean isTraceEnabled() {
    if (traceEnabled == null) {
      traceEnabled = LOG.isTraceEnabled();
    }
    return traceEnabled;
  }
}