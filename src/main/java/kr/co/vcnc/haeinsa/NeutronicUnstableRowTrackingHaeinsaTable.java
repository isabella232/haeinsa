package kr.co.vcnc.haeinsa;

import static kr.co.vcnc.haeinsa.HaeinsaConstants.LOCK_FAMILY;
import static kr.co.vcnc.haeinsa.HaeinsaConstants.LOCK_QUALIFIER;
import com.google.common.base.Throwables;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 * A decorator which allows us to track unstable rows in a separate
 * HBase table.
 *
 * Using this decorator guarantees that if a row is unstable,
 * it will have an accompanying row inside the UnstableLocksTable.
 * However, note that if a row exists in the UnstableRowsTable that is
 * not a guarantee that the row in the original table is unstable.
 *
 * The row key inside the UnstableLocksTable will be ORIGINAL_TABLE_NAME#ORIGINAL_ROW_KEY.
 */
public class NeutronicUnstableRowTrackingHaeinsaTable extends ForwardingHaeinsaTable {
  private static final Logger LOG = LoggerFactory.getLogger(NeutronicUnstableRowTrackingHaeinsaTable.class);
  private static final String DELIEMETER = "#";
  private final HTableInterface unstableRowsTable;
  private final byte[] rowPrefix;
  private final int rowPrefixSize;

  public NeutronicUnstableRowTrackingHaeinsaTable(HaeinsaTableIface haeinsaTable, HTableInterface unstableRowsTable) {
    super(haeinsaTable);
    rowPrefix = (Bytes.toString(delegate().getTableName()) + DELIEMETER).getBytes();
    rowPrefixSize = rowPrefix.length;
    this.unstableRowsTable = unstableRowsTable;
  }

  @Override
  public void prewrite(HaeinsaRowTransaction rowState, byte[] row, boolean isPrimary) throws IOException {
    byte[] unstableRowKey = getUnstableRowKey(row);
    long preWriteTimestamp = rowState.getCurrent().getPrewriteTimestamp();
    unstableRowsTable.put(new Put(unstableRowKey).addColumn(LOCK_FAMILY, LOCK_QUALIFIER, Bytes.toBytes(preWriteTimestamp)));
    delegate().prewrite(rowState, row, isPrimary);
  }

  @Override
  public void makeStable(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException {
    delegate().makeStable(rowTxState, row);

    try {
      byte[] unstableRowKey = getUnstableRowKey(row);
      long preWriteTimestamp = rowTxState.getCurrent().getPrewriteTimestamp();
      Delete delete = new Delete(unstableRowKey).addFamily(LOCK_FAMILY);
      unstableRowsTable.checkAndDelete(row, LOCK_FAMILY, LOCK_QUALIFIER, Bytes.toBytes(preWriteTimestamp), delete);
    } catch (Exception e) {
      LOG.warn("Failed to cleanup unstable locks table row {} due to {}", row, Throwables.getStackTraceAsString(e));
    }
  }

  @Override
  public void close() throws IOException {
    unstableRowsTable.close();
    delegate().close();
  }

  private byte[] getUnstableRowKey(byte[] origRowKey) {
    byte[] unstableRowKey = new byte[rowPrefixSize + origRowKey.length];
    System.arraycopy(rowPrefix, 0, unstableRowKey, 0, rowPrefixSize);
    System.arraycopy(origRowKey, 0, unstableRowKey, rowPrefixSize, origRowKey.length);
    return unstableRowKey;
  }
}
