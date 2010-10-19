include "../../common/thrift/datatype.thrift"

namespace cpp org.cloudata.cloudatamaster
namespace java org.cloudata.core.master.thrift.generated
namespace python cloudatamaster
namespace php cloudatamaster

service CloudataMasterService {
  void createTable(1:TableSchemaThrift table, 2:string[] endRowKeys) throws CException;
  string dropTable(1:string tableName) throws CException;
  TableSchemaThrift[] listTables() throws CException;
  TabletInfoThrift[] getTablets(1: string tableName) throws CException;
  TabletInfoThrift addTablet(1:string tableName, 2:string startRowKey, 3:string endRowKey) throws CException;
  void addColumn(1:string tableName, 2:string addedColumnName) throws CException;
  void test();
  TabletServerInfoT[] getTabletServerInfos();
  void addUser(1:string userId) throws CException;
  void removeUser(1:string userId) throws CException;
  void addTablePermission(1:string tableName, 2:string userId, 3:string readWrite) throws CException;
  void removeTablePermission(1:string tableName, 2:string userId) throws CException;
  void startBalancer() throws CException;
  boolean assignTablet(1:TabletInfoThrift tabletInfo) throws CException;
  void endTabletAssignment(1:TabletInfoThrift tabletInfo, 2:boolean created) throws CException;
  boolean checkServer();
  void errorTabletAssignment(1:string hostName, 2:TabletInfoThrift tabletInfo);
  void reportTabletSplited(1:TabletInfoThrift targetTablet, 2:TabletInfoThrift[] splitedTablets);
  void errorTableDrop(1:string taskId, 2:string hostName, 3:string tableName, 4:string message) throws CException;
  void endTableDrop(1:string taskId, 2:string hostName, 3:string tableName) throws CException;
  void heartbeatTS(1:string hostName, 2:int tabletNum) throws CException;
  void heartbeatCS(1:string hostName, 2:ServerMonitorInfoThrift serverMonitorInfo) throws CException;
  void doRebalacing(1:string tabletServerHostName, 2:TabletInfoThrift tabletInfo, 3:boolean end) throws CException;
}
