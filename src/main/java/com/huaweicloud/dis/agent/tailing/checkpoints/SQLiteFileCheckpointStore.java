package com.huaweicloud.dis.agent.tailing.checkpoints;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.huaweicloud.dis.agent.AgentContext;
import com.huaweicloud.dis.agent.tailing.FileFlow;
import com.huaweicloud.dis.agent.tailing.FileId;
import com.huaweicloud.dis.agent.tailing.TrackedFile;
import lombok.Cleanup;
import lombok.ToString;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Checkpoint store backed by a SQLite database file. This class is thread-safe.
 */
@ToString(exclude = {"agentContext", "connection"})
public class SQLiteFileCheckpointStore implements FileCheckpointStore
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteFileCheckpointStore.class);

    static final String DEFAULT_CHECKPOINTS_DIR = "conf" + File.separator;

    static final String DEFAULT_CHECKPOINTS_FILE =  "since.db";

    private static final int DEFAULT_DB_CONNECTION_TIMEOUT_SECONDS = 5;
    
    private static final int DEFAULT_DB_QUERY_TIMEOUT_SECONDS = 30;
    
    private final Path dbFile;

    private ReentrantLock lock = new ReentrantLock();

    @VisibleForTesting
    final AgentContext agentContext;
    
    @VisibleForTesting
    Connection connection;
    
    private final int dbQueryTimeoutSeconds;
    
    private final int dbConnectionTimeoutSeconds;
    
    public SQLiteFileCheckpointStore(AgentContext agentContext)
    {
        this.agentContext = agentContext;
        Path configCheckPath = agentContext.checkpointFile();
        if (configCheckPath == null)
        {
            // checkpoint file的名称为 agent-name_since.db
            // 为了兼容老版本since.db，此处判断如果checkpoint file不存在，且使用默认的agent name，且since.db存在，则使用since.db即可。
            String dbFileStr = DEFAULT_CHECKPOINTS_DIR + this.agentContext.getAgentName() + "_" + DEFAULT_CHECKPOINTS_FILE;
            if (!Files.exists(Paths.get(dbFileStr)) && AgentContext.DEFAULT_AGENT_NAME.equals(this.agentContext.getAgentName())
                    && Files.exists(Paths.get(DEFAULT_CHECKPOINTS_DIR + DEFAULT_CHECKPOINTS_FILE)))
            {
                // 直接使用conf/since.db文件
                LOGGER.info("Will use compatible checkpoint file : " + DEFAULT_CHECKPOINTS_DIR + DEFAULT_CHECKPOINTS_FILE);
                this.dbFile = Paths.get(DEFAULT_CHECKPOINTS_DIR + DEFAULT_CHECKPOINTS_FILE);
            }
            else
            {
                // 根据NAME来创建since.db，用于多进程区分
                LOGGER.info("Will use default checkpoint file : {}", dbFileStr);
                this.dbFile = Paths.get(dbFileStr);
            }
        }
        else
        {
            // 使用用户指定的文件
            LOGGER.info("Will use custom checkpoint file : {}", configCheckPath.toString());
            this.dbFile = configCheckPath;
        }
        this.dbQueryTimeoutSeconds =
                this.agentContext.readInteger("checkpoints.queryTimeoutSeconds", DEFAULT_DB_QUERY_TIMEOUT_SECONDS);
        this.dbConnectionTimeoutSeconds = this.agentContext.readInteger("checkpoints.connectionTimeoutSeconds",
                DEFAULT_DB_CONNECTION_TIMEOUT_SECONDS);
        connect();
        // Every time we connect, try cleaning up the database
        deleteOldData();
    }
    
    private synchronized boolean isConnected()
    {
        return connection != null;
    }
    
    @Override
    public synchronized void close()
    {
        if (isConnected())
        {
            try
            {
                LOGGER.debug("Closing connection to database {}...", dbFile);
                connection.close();
            }
            catch (SQLException e)
            {
                LOGGER.error("Failed to cleanly close the database {}", dbFile, e);
            }
        }
        connection = null;
    }
    
    private synchronized void connect()
    {
        if (!isConnected())
        {
            try
            {
                Class.forName("org.sqlite.JDBC");
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException("Failed to load SQLite driver.", e);
            }
            
            try
            {
                LOGGER.debug("Connecting to database {}...", dbFile);
                if (!Files.isDirectory(dbFile.getParent()))
                {
                    Files.createDirectories(dbFile.getParent());
                }
                String connectionString = String.format("jdbc:sqlite:%s", dbFile.toString());
                connection = DriverManager.getConnection(connectionString);
                connection.setAutoCommit(false);
            }
            catch (SQLException | IOException e)
            {
                throw new RuntimeException("Failed to create or connect to the checkpoint database.", e);
            }
            try
            {
                @Cleanup
                Statement statement = connection.createStatement();
                statement.executeUpdate(
                    "create table if not exists FILE_CHECKPOINTS(" + "       flow text," + "       path text,"
                        + "       fileId text," + "       lastModifiedTime bigint," + "       size bigint,"
                        + "       offset bigint," + "       headerLength int," + "       headerString text,"
                        + "       lastUpdated datetime," + "       primary key (flow, fileId))");
            }
            catch (SQLException e)
            {
                throw new RuntimeException("Failed to configure the checkpoint database.", e);
            }
        }
    }
    
    protected boolean ensureConnected()
    {
        if (isConnected())
        {
            return true;
        }
        else
        {
            try
            {
                connect();
                return true;
            }
            catch (Exception e)
            {
                LOGGER.error("Failed to open a connection to the checkpoint database.", e);
                return false;
            }
        }
    }
    
    @Override
    public synchronized FileCheckpoint saveCheckpoint(TrackedFile file, long offset)
    {
        Preconditions.checkNotNull(file);
        return createOrUpdateCheckpoint(new FileCheckpoint(file, offset));
    }
    
    @Override
    public synchronized boolean saveCheckpointBatchAfterSend(List<FileCheckpoint> fileCheckpoints)
    {
        return createOrUpdateCheckpointBatchByIncrement(fileCheckpoints);
    }
    
    private FileCheckpoint createOrUpdateCheckpoint(FileCheckpoint cp)
    {
        if (!ensureConnected())
            return null;
        try
        {
            @Cleanup
            PreparedStatement update = connection
                .prepareStatement("update FILE_CHECKPOINTS " + "set path=?, offset=?, lastModifiedTime=?, size=?, "
                    + "headerLength=?, headerString=?, lastUpdated=strftime('%Y-%m-%d %H:%M:%f', 'now') "
                    + "where flow=? and fileId=?");
            update.setString(1, cp.getFile().getPath().toAbsolutePath().toString());
            update.setLong(2, cp.getOffset());
            update.setLong(3, cp.getFile().getLastModifiedTime());
            update.setLong(4, cp.getFile().getSize());
            update.setInt(5, cp.getFile().getHeaderBytesLength());
            // 将文件头部信息生成摘要存放
            update.setString(6,
                cp.getFile().getHeaderBytes() == null ? cp.getFile().getSha256HeaderStr()
                    : DigestUtils.sha256Hex(cp.getFile().getHeaderBytes()));
            update.setString(7, cp.getFile().getFlow().getId());
            update.setString(8, cp.getFile().getId().toString());
            int affected = update.executeUpdate();
            if (affected == 0)
            {
                @Cleanup
                PreparedStatement insert = connection.prepareStatement("insert or ignore into FILE_CHECKPOINTS "
                    + "values(?, ?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%d %H:%M:%f', 'now'))");
                insert.setString(1, cp.getFile().getFlow().getId());
                insert.setString(2, cp.getFile().getPath().toAbsolutePath().toString());
                insert.setString(3, cp.getFile().getId().toString());
                insert.setLong(4, cp.getFile().getLastModifiedTime());
                insert.setLong(5, cp.getFile().getSize());
                insert.setLong(6, cp.getOffset());
                insert.setInt(7, cp.getFile().getHeaderBytesLength());
                insert.setString(8,
                    cp.getFile().getHeaderBytes() == null ? cp.getFile().getSha256HeaderStr()
                        : DigestUtils.sha256Hex(cp.getFile().getHeaderBytes()));
                affected = insert.executeUpdate();
                if (affected == 1)
                {
                    LOGGER.trace("Created new database checkpoint: {}@{}", cp.getFile(), cp.getOffset());
                }
                else
                {
                    // SANITYCHECK: This should never happen since method is synchronized.
                    // TODO: Remove when done debugging.
                    LOGGER.error("Did not update or create checkpoint because of race condition: {}@{}",
                        cp.getFile(),
                        cp.getOffset());
                    throw new RuntimeException(
                        "Race condition detected when setting checkpoint for file: " + cp.getFile().getPath());
                }
            }
            else
            {
                LOGGER.trace("Updated database checkpoint: {}@{}", cp.getFile(), cp.getOffset());
            }
            connection.commit();
            return cp;
        }
        catch (SQLException e)
        {
            LOGGER.error("Failed to create the checkpoint {}@{} in database {}", cp.getFile(), cp.getOffset(), dbFile, e);
            try
            {
                connection.rollback();
            }
            catch (SQLException e2)
            {
                LOGGER.error("Failed to rollback checkpointing transaction: {}@{}", cp.getFile(), cp.getOffset());
                LOGGER.info("Reinitializing connection to database {}", dbFile);
                close();
            }
            return null;
        }
    }
    
    private boolean createOrUpdateCheckpointBatchByIncrement(List<FileCheckpoint> fileCheckpoints)
    {
        if (!ensureConnected())
            return false;
        try
        {
            lock.lock();
            @Cleanup
            PreparedStatement update = connection
                .prepareStatement("update FILE_CHECKPOINTS " + "set path=?, offset=(select max(?, offset) from "
                    + "FILE_CHECKPOINTS where flow=? and fileId=?), lastModifiedTime=?, size=?, "
                    + "headerLength=?, headerString=?, lastUpdated=strftime('%Y-%m-%d %H:%M:%f', 'now') "
                    + "where flow=? and fileId=?");
            
            for (FileCheckpoint cp : fileCheckpoints)
            {
                update.setString(1, cp.getFile().getPath().toAbsolutePath().toString());
                update.setLong(2, cp.getOffset());
                update.setString(3, cp.getFile().getFlow().getId());
                update.setString(4, cp.getFile().getId().toString());
                update.setLong(5, cp.getFile().getLastModifiedTime());
                update.setLong(6, cp.getFile().getSize());
                update.setInt(7, cp.getFile().getHeaderBytesLength());
                update.setString(8,
                    cp.getFile().getHeaderBytes() == null ? cp.getFile().getSha256HeaderStr()
                        : DigestUtils.sha256Hex(cp.getFile().getHeaderBytes()));
                update.setString(9, cp.getFile().getFlow().getId());
                update.setString(10, cp.getFile().getId().toString());
                update.addBatch();
                LOGGER.debug("Update Checkpoint info {}.", cp);
            }
            int[] updateAffects = update.executeBatch();
            
            @Cleanup
            PreparedStatement insert = null;
            List<FileCheckpoint> failedFileCheckpoint = new ArrayList<>();
            for (int i = 0; i < updateAffects.length; i++)
            {
                // 更新失败，则插入记录
                if (updateAffects[i] == 0)
                {
                    FileCheckpoint cp = fileCheckpoints.get(i);

                    // 文件不存在，忽略
                    if (!Files.exists(cp.getFile().getPath()))
                    {
                        continue;
                    }
                    if (insert == null)
                    {
                        insert = connection.prepareStatement("insert or ignore into FILE_CHECKPOINTS "
                            + "values(?, ?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%d %H:%M:%f', 'now'))");
                    }
                    failedFileCheckpoint.add(cp);
                    insert.setString(1, cp.getFile().getFlow().getId());
                    insert.setString(2, cp.getFile().getPath().toAbsolutePath().toString());
                    insert.setString(3, cp.getFile().getId().toString());
                    insert.setLong(4, cp.getFile().getLastModifiedTime());
                    insert.setLong(5, cp.getFile().getSize());
                    insert.setLong(6, cp.getOffset());
                    insert.setInt(7, cp.getFile().getHeaderBytesLength());
                    insert.setString(8, DigestUtils.sha256Hex(cp.getFile().getHeaderBytes()));
                    insert.addBatch();
                }
                else
                {
                    LOGGER.trace("Updated database checkpoint: {}@{}",
                        fileCheckpoints.get(i).getFile(),
                        fileCheckpoints.get(i).getOffset());
                }
            }
            
            if (insert != null)
            {
                int[] insertAffects = insert.executeBatch();
                for (int i = 0; i < insertAffects.length; i++)
                {
                    if (insertAffects[i] == 1)
                    {
                        LOGGER.trace("Created new database checkpoint: {}@{}",
                            failedFileCheckpoint.get(i).getFile(),
                            failedFileCheckpoint.get(i).getOffset());
                    }
                    else
                    {
                        // SANITYCHECK: This should never happen since method is synchronized.
                        // TODO: Remove when done debugging.
                        LOGGER.error("Did not update or create checkpoint because of race condition: {}@{}",
                            failedFileCheckpoint.get(i).getFile(),
                            failedFileCheckpoint.get(i).getOffset());
                        throw new RuntimeException("Race condition detected when setting checkpoint for file: "
                            + failedFileCheckpoint.get(i).getFile().getPath());
                    }
                }
            }
            
            connection.commit();
            return true;
        }
        catch (SQLException e)
        {
            LOGGER.error("Failed to create the checkpoints {} in database {}", fileCheckpoints, dbFile, e);
            try
            {
                connection.rollback();
            }
            catch (SQLException e2)
            {
                LOGGER.error("Failed to rollback checkpointing transaction: {}", fileCheckpoints);
                LOGGER.info("Reinitializing connection to database {}", dbFile);
                close();
            }
            return false;
        }
        finally
        {
            lock.unlock();
        }
    }
    
    @Override
    public long getOffsetForFileID(FileFlow<?> flow, String fileID)
    {
        Preconditions.checkNotNull(flow);
        Preconditions.checkNotNull(fileID);
        if (!ensureConnected())
            return 0;
        try
        {
            @Cleanup
            PreparedStatement statement =
                connection.prepareStatement("select offset " + "from FILE_CHECKPOINTS " + "where flow=? and fileId=?");
            statement.setString(1, flow.getId());
            statement.setString(2, fileID);
            statement.setMaxRows(1);
            @Cleanup
            ResultSet result = statement.executeQuery();
            if (result.next())
            {
                return result.getLong("offset");
            }
            else
                return 0;
        }
        catch (SQLException e)
        {
            LOGGER.error("Failed when getting offset for fileId {} in flow {}", fileID, flow.getId(), e);
            return 0;
        }
    }
    
    @Override
    @Deprecated
    public FileCheckpoint getCheckpointForFlow(FileFlow<?> flow)
    {
        Preconditions.checkNotNull(flow);
        if (!ensureConnected())
            return null;
        try
        {
            @Cleanup
            PreparedStatement statement = connection
                .prepareStatement("select path, fileId, lastModifiedTime, size, offset, headerLength, headerString "
                    + "from FILE_CHECKPOINTS " + "where flow=? " + "order by lastUpdated desc " + "limit 1");
            statement.setString(1, flow.getId());
            statement.setMaxRows(1);
            @Cleanup
            ResultSet result = statement.executeQuery();
            if (result.next())
            {
                TrackedFile file = new TrackedFile(flow, Paths.get(result.getString("path")),
                    new FileId(result.getString("fileId")), result.getLong("lastModifiedTime"), result.getLong("size"),
                    result.getLong("offset"), result.getInt("headerLength"), null, result.getString("headerString"));
                return new FileCheckpoint(file, result.getLong("offset"));
            }
            else
                return null;
        }
        catch (SQLException e)
        {
            LOGGER.error("Failed when getting checkpoint for flow {}", flow.getId(), e);
            return null;
        }
    }
    
    @Override
    public List<Map<String, Object>> dumpCheckpoints()
    {
        if (!ensureConnected())
            return Collections.emptyList();
        try
        {
            @Cleanup
            PreparedStatement statement =
                this.connection.prepareStatement("select * from FILE_CHECKPOINTS order by lastUpdated desc");
            @Cleanup
            ResultSet result = statement.executeQuery();
            List<Map<String, Object>> checkpoints = new ArrayList<>();
            ResultSetMetaData md = result.getMetaData();
            int columns = md.getColumnCount();
            while (result.next())
            {
                Map<String, Object> row = new LinkedHashMap<>(columns);
                for (int i = 1; i <= columns; ++i)
                {
                    row.put(md.getColumnName(i), result.getObject(i));
                }
                checkpoints.add(row);
            }
            return checkpoints;
        }
        catch (SQLException e)
        {
            LOGGER.error("Failed when dumping checkpoints from db.", e);
            return Collections.emptyList();
        }
    }
    
    @VisibleForTesting
    synchronized void deleteOldData()
    {
        if (!ensureConnected())
            return;
        try
        {
            String query = String.format(
                "delete from FILE_CHECKPOINTS " + "where datetime(lastUpdated, '+%d days') < CURRENT_TIMESTAMP",
                agentContext.checkpointTimeToLiveDays());
            @Cleanup
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setQueryTimeout(dbQueryTimeoutSeconds);
            int affectedCount = statement.executeUpdate();
            connection.commit();
            LOGGER.info("Deleted {} old checkpoints.", affectedCount);
        }
        catch (SQLException e)
        {
            try
            {
                connection.rollback();
            }
            catch (SQLException e2)
            {
                LOGGER.error("Failed to rollback cleanup transaction.", e2);
                LOGGER.info("Reinitializing connection to database {}", dbFile);
                close();
            }
            LOGGER.error("Failed to delete old checkpoints.", e);
        }
    }
    
    private boolean updateFileId(TrackedFile trackedFile)
    {
        if (!ensureConnected())
            return false;
        try
        {
            @Cleanup
            PreparedStatement update =
                connection.prepareStatement("update FILE_CHECKPOINTS " + "set fileId=?, lastModifiedTime=? "
                    + "lastUpdated=strftime('%Y-%m-%d %H:%M:%f', 'now') " + "where flow=? and path=?");
            update.setString(1, trackedFile.getId().toString());
            update.setLong(2, trackedFile.getLastModifiedTime());
            update.setString(3, trackedFile.getFlow().getId());
            update.setString(4, trackedFile.getPath().toAbsolutePath().toString());
            int affected = update.executeUpdate();
            
            if (affected == 0)
            {
                return false;
            }
            else
            {
                LOGGER.trace("Updated database fileId: {}@{}", trackedFile, trackedFile.getId().toString());
            }
            connection.commit();
            return true;
        }
        catch (SQLException e)
        {
            LOGGER.error("Failed to update fileId {}@{} in database {}",
                trackedFile,
                trackedFile.getId().toString(),
                dbFile);
            try
            {
                connection.rollback();
            }
            catch (SQLException e2)
            {
                LOGGER.error("Failed to rollback transaction: {}@{}", trackedFile, trackedFile.getId().toString());
                LOGGER.info("Reinitializing connection to database {}", dbFile);
                close();
            }
            return false;
        }
    }
    
    public List<TrackedFile> getAllCheckpointForFlow(FileFlow<?> flow)
    {
        Preconditions.checkNotNull(flow);
        if (!ensureConnected())
        {
            throw new RuntimeException("Failed to connect to checkpoint database.");
        }
        List<TrackedFile> trackedFiles = new ArrayList<>();
        try
        {
            @Cleanup
            PreparedStatement statement = connection
                .prepareStatement("select path, fileId, lastModifiedTime, size, offset, headerLength, headerString "
                    + "from (select * from FILE_CHECKPOINTS where flow=? order by lastModifiedTime asc )"
                    + " group by fileId order by lastModifiedTime desc");
            statement.setString(1, flow.getId());
            @Cleanup
            ResultSet result = statement.executeQuery();
            while (result.next())
            {
                trackedFiles.add(new TrackedFile(flow, Paths.get(result.getString("path")),
                    new FileId(result.getString("fileId")), result.getLong("lastModifiedTime"), result.getLong("size"),
                    result.getLong("offset"), result.getInt("headerLength"), null, result.getString("headerString")));
            }
            return trackedFiles;
        }
        catch (SQLException e)
        {
            LOGGER.error("Failed when getting checkpoint for flow {}", flow.getId(), e);
            throw new RuntimeException("Failed to configure the checkpoint database.", e);
        }
    }
    
    @Override
    public void deleteCheckpointByTrackedFileList(List<TrackedFile> trackedFileList)
    {
        if (trackedFileList == null || trackedFileList.size() == 0)
        {
            return;
        }
        if (!ensureConnected())
        {
            return;
        }
        try
        {
            lock.lock();
            @Cleanup
            PreparedStatement statement = connection
                    .prepareStatement("delete from FILE_CHECKPOINTS where rowid in (select rowid from FILE_CHECKPOINTS "
                            + "where flow=? and fileId=? order by lastModifiedTime asc limit 1)\n");

            for (TrackedFile trackedFile : trackedFileList)
            {
                statement.setString(1, trackedFile.getFlow().getId());
                statement.setString(2, trackedFile.getId().toString());
                statement.addBatch();
                LOGGER.debug("delete checkpoint info {}.", trackedFile);
            }
            statement.executeBatch();
            connection.commit();
            LOGGER.info("Delete {} checkpoints", trackedFileList.size());
        }
        catch (SQLException e)
        {
            try
            {
                connection.rollback();
            }
            catch (SQLException e2)
            {
                LOGGER.error("Failed to rollback cleanup transaction.", e2);
                LOGGER.info("Reinitializing connection to database {}", dbFile);
                close();
            }
        }
        finally
        {
            lock.unlock();
        }
    }
}
