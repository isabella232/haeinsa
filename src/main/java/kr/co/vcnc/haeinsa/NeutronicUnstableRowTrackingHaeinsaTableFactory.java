package kr.co.vcnc.haeinsa;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;

public class NeutronicUnstableRowTrackingHaeinsaTableFactory implements HaeinsaTableIfaceFactory {
  public static final String UNSTABLE_ROWS_TRACKING_TABLE_NAME = "neutronic.haeinsa.unstable.rows.tracking.table.name";
  public static final String UNSTABLE_ROWS_TRACKING_TABLE_NAME_DEFAULT = "unstable_rows";
  private final HTableInterfaceFactory tableInterfaceFactory;
  private final List<String> tablesToTrackUnstableRows;

  public NeutronicUnstableRowTrackingHaeinsaTableFactory(List<String> tablesToTrackUnstableRows) {
    this(new HTableFactory(), tablesToTrackUnstableRows);
  }

  public NeutronicUnstableRowTrackingHaeinsaTableFactory(HTableInterfaceFactory tableInterfaceFactory, List<String> tablesToTrackUnstableRows) {
    this.tableInterfaceFactory = tableInterfaceFactory;
    this.tablesToTrackUnstableRows = tablesToTrackUnstableRows;
  }

  @Override
  public HaeinsaTableIface createHaeinsaTableIface(Configuration config, byte[] tableName) {
    if (tablesToTrackUnstableRows.contains(new String(tableName))) {
      String unstableRowsTableName = config.get(UNSTABLE_ROWS_TRACKING_TABLE_NAME, UNSTABLE_ROWS_TRACKING_TABLE_NAME_DEFAULT);
      HTableInterface unstableRowTable = tableInterfaceFactory.createHTableInterface(config, unstableRowsTableName.getBytes());
      return new NeutronicUnstableRowTrackingHaeinsaTable(new HaeinsaTable(tableInterfaceFactory.createHTableInterface(config, tableName)), unstableRowTable);
    } else {
      return new HaeinsaTable(tableInterfaceFactory.createHTableInterface(config, tableName));
    }
  }

  @Override
  public void releaseHaeinsaTableIface(HaeinsaTableIface table) throws IOException {
    table.close();
  }
}