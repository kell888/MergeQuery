using System;
using System.Collections.Generic;
using System.Text;
using System.Configuration;
using System.Data.Common;
using System.Data;
using System.Data.SqlClient;
using System.IO;

namespace MergeQueryUtil
{
    /// <summary>
    /// 数据库操作类
    /// </summary>
    public class SqlHelper
    {
        private string connStr;
        private string providerName;
        private string connStringConfigName = "MonitoringSystem";
        private DbConnection conn;
        /// <summary>
        /// 数据库连接字符串的配置名称，为空则默认为【MonitoringSystem】
        /// </summary>
        /// <param name="connStringConfigName"></param>
        public SqlHelper(string connStringConfigName)
        {
            if (!string.IsNullOrEmpty(connStringConfigName))
            {
                SetConnConfig(connStringConfigName);
            }
            else
            {
                connStr = ConfigurationManager.ConnectionStrings[this.connStringConfigName].ConnectionString;
                providerName = ConfigurationManager.ConnectionStrings[this.connStringConfigName].ProviderName;
            }
        }
        ///// <summary>
        ///// 根据新的连接字符串更改当前的数据库，更改后会使得Conn重新构造！
        ///// </summary>
        ///// <param name="connString"></param>
        ///// <returns></returns>
        //public void ChangeDatabase(string connString)
        //{
        //    conn = null;
        //    connStr = connString;
        //}
        /// <summary>
        /// 当前数据库是否能连接上
        /// </summary>
        public bool CanConnect
        {
            get
            {
                bool isClosed = Conn.State == ConnectionState.Closed;
                try
                {
                    if (isClosed)
                        Conn.Open();
                    return Conn.State == ConnectionState.Open;
                }
                catch
                {
                    return false;
                }
                finally
                {
                    if (isClosed)
                        Conn.Close();
                }
            }
        }
        /// <summary>
        /// 当前数据库是否已经打开
        /// </summary>
        public bool IsOpened
        {
            get
            {
                return Conn.State == ConnectionState.Open;
            }
        }
        /// <summary>
        /// 设置数据库连接字符串的配置名称，默认为【MonitoringSystem】，如果要赋的值与当前值相同则立即返回
        /// </summary>
        /// <param name="connStringConfigName"></param>
        public void SetConnConfig(string connStringConfigName)
        {
            if (connStringConfigName.Equals(this.connStringConfigName, StringComparison.InvariantCultureIgnoreCase))
                return;

            ConnectionStringConfigName = connStringConfigName;
        }
        /// <summary>
        /// 设置或获取数据库连接字符串的配置名称，设置时会使得Conn重新构造！
        /// </summary>
        public string ConnectionStringConfigName
        {
            get
            {
                if (ConfigurationManager.ConnectionStrings != null && ConfigurationManager.ConnectionStrings.Count > 0)
                {
                    bool find = false;
                    for (int i = 0; i < ConfigurationManager.ConnectionStrings.Count; i++)
                    {
                        if (ConfigurationManager.ConnectionStrings[i].Name.Equals(connStringConfigName, StringComparison.InvariantCultureIgnoreCase))
                        {
                            find = true;
                            break;
                        }
                    }
                    if (!find)
                    {
                        connStringConfigName = ConfigurationManager.ConnectionStrings[0].Name;//找不到时取第一个连接串
                    }
                    return connStringConfigName;
                }
                else
                {
                    throw new Exception("应用程序配置中缺少ConnectionStrings节！请配置好再运行程序");
                }
            }
            set
            {
                conn = null;
                connStringConfigName = value;
                connStr = ConfigurationManager.ConnectionStrings[connStringConfigName].ConnectionString;
                providerName = ConfigurationManager.ConnectionStrings[connStringConfigName].ProviderName;
            }
        }
        /// <summary>
        /// 获取数据库连接对象
        /// </summary>
        public DbConnection Conn
        {
            get
            {
                if (conn == null)
                {
                    DbProviderFactory factory = DbProviderFactories.GetFactory(providerName);
                    conn = factory.CreateConnection();
                    conn.ConnectionString = connStr;
                    return conn;
                }
                return conn;
            }
        }

        //准备Command
        private void PrepareCommand(DbCommand cmd, string cmdText, CommandType type, DbParameter[] param)
        {
            cmd.CommandText = cmdText;
            cmd.CommandType = type;
            if (param != null && param.Length > 0)
            {
                cmd.Parameters.Clear();
                cmd.Parameters.AddRange(param);
            }
            string cmdTimeout = ConfigurationManager.AppSettings["cmdTimeout"];
            if (!string.IsNullOrEmpty(cmdTimeout))
            {
                int RET;
                if (int.TryParse(cmdTimeout, out RET))
                {
                    cmd.CommandTimeout = RET;
                }
            }
        }

        /// <summary>
        /// 执行查询操作
        /// </summary>
        /// <param name="cmdText">命令文本</param>
        /// <param name="type">命令类型</param>
        /// <param name="param">文本参数数组</param>
        /// <returns></returns>
        public DbDataReader ExecuteQueryReader(string cmdText, CommandType type, DbParameter[] param)
        {
            DbCommand cmd = new SqlCommand("", new SqlConnection(connStr));
            PrepareCommand(cmd, cmdText, type, param);
            if (cmd.Connection.State != ConnectionState.Open)
                cmd.Connection.Open();
            return cmd.ExecuteReader(CommandBehavior.CloseConnection);
        }

        /// <summary>
        /// 执行查询操作
        /// </summary>
        /// <param name="sql">命令文本</param>
        /// <returns></returns>
        public DbDataReader ExecuteQueryReader(string sql)
        {
            DbCommand cmd = new SqlCommand("", new SqlConnection(connStr));
            PrepareCommand(cmd, sql, CommandType.Text, null);
            if (cmd.Connection.State != ConnectionState.Open)
                cmd.Connection.Open();
            return cmd.ExecuteReader(CommandBehavior.CloseConnection);
        }

        /// <summary>
        /// 执行查询操作
        /// </summary>
        /// <param name="cmdText">SQL命令</param>
        /// <param name="type"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public DataTable ExecuteQueryDataTable(string cmdText, CommandType type, DbParameter[] param)
        {
            DataTable dt = new DataTable();
            try
            {
                DbCommand cmd = Conn.CreateCommand();
                PrepareCommand(cmd, cmdText, type, param);
                if (Conn.State != ConnectionState.Open)
                    Conn.Open();
                DbDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                dt.Load(reader);
                reader.Close();
            }
            catch (Exception e) { }
            finally
            {

            }
            return dt;
        }

        /// <summary>
        /// 执行查询操作
        /// </summary>
        /// <param name="sql">SQL命令</param>
        /// <returns></returns>
        public DataTable ExecuteQueryDataTable(string sql)
        {
            DataTable dt = new DataTable();
                SqlCommand cmd = new SqlCommand(sql, new SqlConnection(connStr));
            try
            {
                DbProviderFactory factory = DbProviderFactories.GetFactory(providerName);
                DbDataAdapter adapter = factory.CreateDataAdapter();
                adapter.SelectCommand = cmd;
                PrepareCommand(cmd, cmd.CommandText, CommandType.Text, null);
                if (cmd.Connection.State != ConnectionState.Open)
                    cmd.Connection.Open();
                adapter.Fill(dt);
            }
            catch (Exception e) { }
            finally
            {
                cmd.Connection.Close();
            }
            return dt;
        }

        /// <summary>
        /// 执行查询操作
        /// </summary>
        /// <param name="sql">SQL命令</param>
        /// <returns></returns>
        public DbDataAdapter GetDataAdapter(string sql)
        {
            try
            {
                DbProviderFactory factory = DbProviderFactories.GetFactory(providerName);
                DbDataAdapter adapter = factory.CreateDataAdapter();
                SqlCommand cmd = new SqlCommand(sql, new SqlConnection(connStr));
                adapter.SelectCommand = cmd;
                PrepareCommand(cmd, cmd.CommandText, CommandType.Text, null);
                if (cmd.Connection.State != ConnectionState.Open)
                    cmd.Connection.Open();
                return adapter;
            }
            catch (Exception e) { }
            finally
            {

            }
            return null;
        }

        /// <summary>
        /// 执行查询操作
        /// </summary>
        /// <param name="cmd">命令对象</param>
        /// <param name="type">命令类型</param>
        /// <param name="param">文本参数数组</param>
        /// <returns></returns>
        public DataTable ExecuteQueryDataTable(DbCommand cmd, CommandType type, DbParameter[] param)
        {
            DataTable dt = new DataTable();
            try
            {
                DbProviderFactory factory = DbProviderFactories.GetFactory(providerName);
                DbDataAdapter adapter = factory.CreateDataAdapter();
                adapter.SelectCommand = cmd;
                PrepareCommand(cmd, cmd.CommandText, type, param);
                if (cmd.Connection.State != ConnectionState.Open)
                    cmd.Connection.Open();
                adapter.Fill(dt);
            }
            catch (Exception e) { }
            finally
            {
                cmd.Connection.Close();
            }
            return dt;
        }

        /// <summary>
        /// 执行查询操作返回第一行第一列对象
        /// </summary>
        /// <param name="cmdText"></param>
        /// <param name="type"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public object ExecuteScalar(string cmdText, CommandType type, DbParameter[] param)
        {
            object obj = null;
            using (DbCommand cmd = new SqlCommand("", new SqlConnection(connStr)))
            {
                try
                {
                    PrepareCommand(cmd, cmdText, type, param);
                    if (cmd.Connection.State != ConnectionState.Open)
                        cmd.Connection.Open();
                    obj = cmd.ExecuteScalar();
                }
                catch (Exception e) { }
                finally
                {
                    cmd.Connection.Close();
                }
            }
            return obj;
        }

        /// <summary>
        /// 执行查询操作返回第一行第一列对象
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public object ExecuteScalar(string sql)
        {
            object obj = null;
            using (DbCommand cmd = new SqlCommand("", new SqlConnection(connStr)))
            {
                try
                {
                    PrepareCommand(cmd, sql, CommandType.Text, null);
                    if (cmd.Connection.State != ConnectionState.Open)
                        cmd.Connection.Open();
                    obj = cmd.ExecuteScalar();
                }
                catch (Exception e) { }
                finally
                {
                    cmd.Connection.Close();
                }
            }
            return obj;
        }

        /// <summary>
        /// 执行增删改操作，返回受影响行数
        /// </summary>
        /// <param name="cmdText"></param>
        /// <param name="type"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public int ExecuteNonQuery(string cmdText, CommandType type, DbParameter[] param)
        {
            int i = 0;
            using (DbCommand cmd = new SqlCommand("", new SqlConnection(connStr)))
            {
                try
                {
                    PrepareCommand(cmd, cmdText, type, param);
                    if (cmd.Connection.State != ConnectionState.Open)
                        cmd.Connection.Open();
                    i = cmd.ExecuteNonQuery();
                    cmd.Parameters.Clear();
                }
                catch (Exception e) { }
                finally
                {
                    cmd.Connection.Close();
                }
            }
            return i;
        }

        /// <summary>
        /// 执行增删改操作，返回受影响行数
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public int ExecuteNonQuery(string sql)
        {
            int i = 0;
            using (DbCommand cmd = new SqlCommand("", new SqlConnection(connStr)))
            {
                try
                {
                    PrepareCommand(cmd, sql, CommandType.Text, null);
                    if (cmd.Connection.State != ConnectionState.Open)
                        cmd.Connection.Open();
                    i = cmd.ExecuteNonQuery();
                }
                catch (Exception e) { }
                finally
                {
                    cmd.Connection.Close();
                }
            }
            return i;
        }
        /// <summary>
        /// SQL数据库备份
        /// </summary>
        /// <param name="filename">要备份到的文件全路径</param>
        /// <param name="connStrBuilder">连接字符串构造器</param>
        /// <param name="Backup_PercentComplete">进度</param>
        /// <param name="oBackup">数据库备份服务对象</param>
        /// <param name="remark">备份备注</param>
        public bool SQLDbBackup(string filename, SqlConnectionStringBuilder connStrBuilder, SQLDMO.BackupSink_PercentCompleteEventHandler Backup_PercentComplete, out SQLDMO.Backup oBackup, string remark)
        {
            string ServerIP = connStrBuilder.DataSource;
            string LoginUserName = connStrBuilder.UserID;
            string LoginPass = connStrBuilder.Password;
            string DBName = connStrBuilder.InitialCatalog;
            FileInfo fi = new FileInfo(filename);
            string DBFile = fi.Name.Substring(0, fi.Name.Length - fi.Extension.Length);
            oBackup = new SQLDMO.BackupClass();
            SQLDMO.SQLServer oSQLServer = new SQLDMO.SQLServerClass();
            try
            {
                oSQLServer.LoginSecure = false;
                oSQLServer.Connect(ServerIP, LoginUserName, LoginPass);
                oBackup.Action = SQLDMO.SQLDMO_BACKUP_TYPE.SQLDMOBackup_Database;
                oBackup.PercentComplete += Backup_PercentComplete;
                oBackup.Database = DBName;
                oBackup.Files = @"" + string.Format("[{0}]", filename) + "";
                oBackup.BackupSetName = DBFile;
                oBackup.BackupSetDescription = "备份集" + DBFile;
                oBackup.Initialize = true;
                oBackup.SQLBackup(oSQLServer);
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
            finally
            {
                oSQLServer.DisConnect();
            }
        }
        /// <summary>
        /// 取消指定备份服务的备份进程
        /// </summary>
        /// <param name="oBackup"></param>
        /// <param name="Backup_PercentComplete"></param>
        public void CancelDbBackup(SQLDMO.Backup oBackup, SQLDMO.BackupSink_PercentCompleteEventHandler Backup_PercentComplete)
        {
            try
            {
                oBackup.Abort();
                oBackup.PercentComplete -= Backup_PercentComplete;
            }
            catch (Exception e)
            {
                
            }
        }
        /// <summary>
        /// SQL恢复数据库
        /// </summary>
        /// <param name="filename">备份集文件的全路径</param>
        /// <param name="connStrBuilder">连接字符串构造器</param>
        /// <param name="Restore_PercentComplete">进度</param>
        /// <param name="oRestore">数据库还原服务对象</param>
        public bool SQLDbRestore(string filename, SqlConnectionStringBuilder connStrBuilder, SQLDMO.RestoreSink_PercentCompleteEventHandler Restore_PercentComplete, out SQLDMO.Restore oRestore)
        {
            string ServerIP = connStrBuilder.DataSource;
            string LoginUserName = connStrBuilder.UserID;
            string LoginPass = connStrBuilder.Password;
            string DBName = connStrBuilder.InitialCatalog;
            oRestore = new SQLDMO.RestoreClass();
            SQLDMO.SQLServer oSQLServer = new SQLDMO.SQLServerClass();
            try
            {
                oSQLServer.LoginSecure = false;
                oSQLServer.Connect(ServerIP, LoginUserName, LoginPass);
                //因为“数据库正在使用，所以无法获得对数据库的独占访问权。”不一定是由于其他进程的占用造成，还有其他的原因，所以要脱机再联机会比较保险...
                KillProcess(DBName);
                OffAndOnLine(DBName);
                oRestore.Action = SQLDMO.SQLDMO_RESTORE_TYPE.SQLDMORestore_Database;
                oRestore.PercentComplete += Restore_PercentComplete;
                oRestore.Action = SQLDMO.SQLDMO_RESTORE_TYPE.SQLDMORestore_Database;
                oRestore.Database = DBName;
                oRestore.Files = @"" + string.Format("[{0}]", filename) + "";
                oRestore.FileNumber = 1;
                oRestore.ReplaceDatabase = true;
                oRestore.SQLRestore(oSQLServer);
                return true;
            }
            catch (Exception e)
            {
                
            }
            finally
            {
                oSQLServer.DisConnect();
            }
            return false;
        }
        /// <summary>
        /// 取消指定还原服务的还原进程
        /// </summary>
        /// <param name="oRestore"></param>
        /// <param name="Restore_PercentComplete"></param>
        public void CancelDbRestore(SQLDMO.Restore oRestore, SQLDMO.RestoreSink_PercentCompleteEventHandler Restore_PercentComplete)
        {
            try
            {
                oRestore.Abort();
                oRestore.PercentComplete -= Restore_PercentComplete;
            }
            catch (Exception e)
            {
                
            }
        }
        /// <summary>
        /// 联机指定数据库
        /// </summary>
        /// <param name="DBName"></param>
        public void OnLine(string DBName)
        {
            string DB = DBName;
            if (string.IsNullOrEmpty(DB))
                DB = Conn.Database;
            string sql = "alter database " + DB + " set online with ROLLBACK IMMEDIATE";
            ExecuteNonQuery(sql);
        }
        /// <summary>
        /// 脱机指定数据库
        /// </summary>
        /// <param name="DBName"></param>
        public void OffLine(string DBName)
        {
            string DB = DBName;
            if (string.IsNullOrEmpty(DB))
                DB = Conn.Database;
            string sql = "alter database " + DB + " set offline with ROLLBACK IMMEDIATE";
            ExecuteNonQuery(sql);
        }
        /// <summary>
        /// 脱机再联机指定数据库
        /// </summary>
        /// <param name="DBName"></param>
        public void OffAndOnLine(string DBName)
        {
            string DB = DBName;
            if (string.IsNullOrEmpty(DB))
                DB = Conn.Database;
            string sql = "alter database " + DBName + " set offline with ROLLBACK IMMEDIATE;alter database " + DB + " set online with ROLLBACK IMMEDIATE";
            ExecuteNonQuery(sql);
        }
        /// <summary>
        /// 杀死指定数据库的进程
        /// </summary>
        /// <param name="DbName"></param>
        public void KillProcess(string DbName)
        {
            string DB = DbName;
            if (string.IsNullOrEmpty(DB))
                DB = Conn.Database;
            string sql = "select spid from sys.sysprocesses where dbid=(select dbid from master..sysdatabases where name = '" + DB + "')";
            object obj = ExecuteScalar(sql);
            if (obj != null && obj != DBNull.Value)
            {
                ExecuteNonQuery("kill " + obj.ToString());
            }
        }
        /// <summary>
        /// 收缩数据库
        /// </summary>
        /// <param name="DbName"></param>
        /// <returns></returns>
        public bool ShrinkDB(string DbName)
        {
            string DB = DbName;
            if (string.IsNullOrEmpty(DB))
                DB = Conn.Database;
            try
            {
                string sql = "DBCC SHRINKDATABASE('" + DB + "')";
                ExecuteNonQuery(sql);
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }
        /// <summary>
        /// 是否存在指定的数据库对象
        /// </summary>
        /// <param name="tablename"></param>
        /// <returns></returns>
        public bool ExistsTable(string tablename)
        {
            string sql = "select 1 from sysobjects where id=object_id('" + tablename + "')";
            DataTable dt = ExecuteQueryDataTable(sql);
            if (dt.Rows.Count > 0)
            {
                return true;
            }
            return false;
        }
        /// <summary>
        /// 是否存在指定的数据列
        /// </summary>
        /// <param name="tablename"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public bool ExistsColumn(string tablename, string columnName)
        {
            string sql = "select 1 from syscolumns where id=object_id('" + tablename + "') and name='" + columnName + "'";
            DataTable dt = ExecuteQueryDataTable(sql);
            if (dt.Rows.Count > 0)
            {
                return true;
            }
            return false;
        }
    }
}