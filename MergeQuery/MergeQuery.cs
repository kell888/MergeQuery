/*****************************************************
 * 使用前提：只针对后缀名是_log的表或者视图，并且_log后面紧跟YYYYMM的年月格式（只按月存储数据）或者紧跟YYYYMMDD的年月日格式（只按日存储数据），XXX_DayLog方法则执行按日存储的表或者视图，注意：_log表只能放在tablenames的第一个位置
 * 默认：数据库连接的默认配置名称为【MonitoringSystem】，如果不是，则要给各个静态方法的缺省参数connConfig赋值
 * 作者：Kell
 * 时间：2013-09-17
*****************************************************/
using System;
using System.Collections.Generic;
using System.Web;
using System.Data;
using System.Data.SqlClient;
using System.Data.Common;
using System.Configuration;

namespace MergeQueryUtil
{
    /// <summary>
    /// 动态表的共用类库
    /// </summary>
    public static class MergeCommon
    {
        /// <summary>
        /// 根据指定的时间获取年月
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public static string GetYYYYMM(DateTime time)
        {
            return time.Year.ToString().PadLeft(4, '0') + time.Month.ToString().PadLeft(2, '0');
        }
        /// <summary>
        /// 根据指定的时间获取年月日
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public static string GetYYYYMMDD(DateTime time)
        {
            return time.Year.ToString().PadLeft(4, '0') + time.Month.ToString().PadLeft(2, '0') + time.Day.ToString().PadLeft(2, '0');
        }
        /// <summary>
        /// 获取指定表的结构
        /// </summary>
        /// <param name="tablename"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static DataTable GetTableSchama(string tablename, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string sql = "select * from " + tablename + " where 1=2";
            DataTable dt = SqlHelper.ExecuteQueryDataTable(sql);
            return dt;
        }
        /// <summary>
        /// 获取指定表的结构
        /// </summary>
        /// <param name="tablename"></param>
        /// <param name="yyyyMMORyyyyMMdd"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static DataTable GetTableSchama(string tablename, string yyyyMMORyyyyMMdd, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string sql = "select * from " + tablename + yyyyMMORyyyyMMdd + " where 1=2";
            DataTable dt = SqlHelper.ExecuteQueryDataTable(sql);
            return dt;
        }
        /// <summary>
        /// 获取指定表的记录中最早时间和最晚时间
        /// </summary>
        /// <param name="tablename"></param>
        /// <param name="timeField"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="connConfig"></param>
        public static void GetHistoryTime(string tablename, string timeField, out DateTime start, out DateTime end, string connConfig)
        {
            if (tablename.ToLower().StartsWith("dbo."))
                tablename = tablename.Substring(4);
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            start = DateTime.Now;
            end = DateTime.Now;
            string TB = tablename;
            DbDataReader reader = SqlHelper.ExecuteQueryReader("select name from sys.objects where name like '" + tablename + "%' order by name asc");
            while (reader.Read())
            {
                TB = reader.GetString(0);
                string sql = "select min(" + timeField + ") from " + TB;
                DataTable dt = SqlHelper.ExecuteQueryDataTable(sql);
                if (dt.Rows.Count > 0)
                {
                    if (dt.Rows[0][0] != DBNull.Value)
                    {
                        start = Convert.ToDateTime(dt.Rows[0][0]);
                        break;
                    }
                }
            }
            reader.Close();
            reader = SqlHelper.ExecuteQueryReader("select name from sys.objects where name like '" + tablename + "%' order by name desc");
            while (reader.Read())
            {
                TB = reader.GetString(0);
                string sql = "select max(" + timeField + ") from " + TB;
                DataTable dt = SqlHelper.ExecuteQueryDataTable(sql);
                if (dt.Rows.Count > 0)
                {
                    if (dt.Rows[0][0] != DBNull.Value)
                    {
                        end = Convert.ToDateTime(dt.Rows[0][0]).AddSeconds(1);//加上1秒，以便能精确查询到数据
                        break;
                    }
                }
            }
            reader.Close();
        }
        /// <summary>
        /// 获取指定表记录中的最早时间
        /// </summary>
        /// <param name="tablename"></param>
        /// <param name="timeField"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static DateTime GetStartTime(string tablename, string timeField, string connConfig)
        {
            if (tablename.ToLower().StartsWith("dbo."))
                tablename = tablename.Substring(4);
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DateTime start = DateTime.Now;
            string TB = tablename;
            DbDataReader reader = SqlHelper.ExecuteQueryReader("select name from sys.objects where name like '" + tablename + "%' order by name asc");
            while (reader.Read())
            {
                TB = reader.GetString(0);
                string sql = "select min(" + timeField + ") from " + TB;
                DataTable dt = SqlHelper.ExecuteQueryDataTable(sql);
                if (dt.Rows.Count > 0)
                {
                    if (dt.Rows[0][0] != DBNull.Value)
                    {
                        start = Convert.ToDateTime(dt.Rows[0][0]);
                        break;
                    }
                }
            }
            reader.Close();
            return start;
        }
        /// <summary>
        /// 获取指定表记录中的最晚时间
        /// </summary>
        /// <param name="tablename"></param>
        /// <param name="timeField"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static DateTime GetEndTime(string tablename, string timeField, string connConfig)
        {
            if (tablename.ToLower().StartsWith("dbo."))
                tablename = tablename.Substring(4);
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DateTime end = DateTime.Now;
            string TB = tablename;
            DbDataReader reader = SqlHelper.ExecuteQueryReader("select name from sys.objects where name like '" + tablename + "%' order by name desc");
            while (reader.Read())
            {
                TB = reader.GetString(0);
                string sql = "select max(" + timeField + ") from " + TB;
                DataTable dt = SqlHelper.ExecuteQueryDataTable(sql);
                if (dt.Rows.Count > 0)
                {
                    if (dt.Rows[0][0] != DBNull.Value)
                    {
                        end = Convert.ToDateTime(dt.Rows[0][0]).AddSeconds(1);//加上1秒，以便能精确查询到数据
                        break;
                    }
                }
            }
            reader.Close();
            return end;
        }
        /// <summary>
        /// 判断指定的表在当前数据库中是否存在
        /// </summary>
        /// <param name="tablename"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static bool ExistsTable(string tablename, string connConfig)
        {
            if (tablename.ToLower().StartsWith("dbo."))
                tablename = tablename.Substring(4);
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string sql = "select 1 from sysobjects where id=object_id('" + tablename + "')";// and objectproperty(id, N'IsUserTable')=1";
            DataTable dt = SqlHelper.ExecuteQueryDataTable(sql);
            if (dt.Rows.Count > 0)
            {
                return true;
            }
            return false;
        }
    }

    /// <summary>
    /// 生成查询动态表的SQL脚本类库，注意：order by语句不能放在where参数中，而应该静态函数执行完毕后，在外面结构尾部加上
    /// </summary>
    public static class MergeSQLQuery
    {
        /// <summary>
        /// 获取查询月表数据的SQL语句
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="where"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static string GetQuerySQL(string tablenames, string selectFields, string timeField, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DateTime start;
            DateTime end;
            MergeCommon.GetHistoryTime(tablenames.Split(',')[0].Trim(), timeField, out start, out end, connConfig);
            int yearOffset = end.Year - start.Year;
            if (yearOffset > 0)
            {
                return MultiYear(tablenames, selectFields, timeField, start, end, where, connConfig);
            }
            else
            {
                if (end.Month - start.Month > 0)
                {
                    return SingleYear(tablenames, selectFields, timeField, start, end, where, connConfig);
                }
                else
                {
                    return SingleMonth(tablenames, selectFields, timeField, start.Year, start.Month, start.Day, end.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
                }
            }
        }
        /// <summary>
        /// 获取查询日表数据的SQL语句
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="where"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static string GetQuerySQL_DayLog(string tablenames, string selectFields, string timeField, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DateTime start;
            DateTime end;
            MergeCommon.GetHistoryTime(tablenames.Split(',')[0].Trim(), timeField, out start, out end, connConfig);
            int yearOffset = end.Year - start.Year;
            if (yearOffset > 0)
            {
                return MultiYear(tablenames, selectFields, timeField, start, end, where, connConfig);
            }
            else
            {
                if (end.Month - start.Month > 0)
                {
                    return SingleYear(tablenames, selectFields, timeField, start, end, where, connConfig);
                }
                else
                {
                    if (end.Day - start.Day > 0)
                    {
                        return SingleMonth(tablenames, selectFields, timeField, start.Year, start.Month, start.Day, end.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
                    }
                    else
                    {
                        return SingleDay(tablenames, selectFields, timeField, start.Year, start.Month, start.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
                    }
                }
            }
        }
        /// <summary>
        /// 获取查询月表数据的SQL语句
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="time"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static string GetQuerySQLAt(string tablenames, string selectFields, string timeField, DateTime time, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMM = MergeCommon.GetYYYYMM(time);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return "";
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return "";
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMM + ".");
            string sql = "select " + selectFields + " from " + tables + " where " + t + " = '" + time.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMM + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMM + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMM + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            if (!string.IsNullOrEmpty(orderby))
            {
                orderby = orderby.Trim().ToLower();
                if (orderby.Contains("_log "))
                    orderby = orderby.Replace("_log ", "_log" + yyyyMM + " ");
                if (orderby.Contains("_log."))
                    orderby = orderby.Replace("_log.", "_log" + yyyyMM + ".");
                if (orderby.Contains("_log)"))
                    orderby = orderby.Replace("_log)", "_log" + yyyyMM + ")");
                if (!orderby.StartsWith("order by "))
                    orderby = "order by " + orderby;
                sql += " " + orderby;
            }
            return sql;
        }
        /// <summary>
        /// 获取查询日表数据的SQL语句
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="time"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static string GetQuerySQLAt_DayLog(string tablenames, string selectFields, string timeField, DateTime time, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMMdd = MergeCommon.GetYYYYMMDD(time);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return "";
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return "";
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMMdd + ".");
            string sql = "select " + selectFields + " from " + tables + " where " + t + " = '" + time.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMMdd + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMMdd + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMMdd + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            if (!string.IsNullOrEmpty(orderby))
            {
                orderby = orderby.Trim().ToLower();
                if (orderby.Contains("_log "))
                    orderby = orderby.Replace("_log ", "_log" + yyyyMMdd + " ");
                if (orderby.Contains("_log."))
                    orderby = orderby.Replace("_log.", "_log" + yyyyMMdd + ".");
                if (orderby.Contains("_log)"))
                    orderby = orderby.Replace("_log)", "_log" + yyyyMMdd + ")");
                if (!orderby.StartsWith("order by "))
                    orderby = "order by " + orderby;
                sql += " " + orderby;
            }
            return sql;
        }
        /// <summary>
        /// 获取查询月表数据的SQL语句
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="where"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static string GetQuerySQLRange(string tablenames, string selectFields, string timeField, DateTime start, DateTime end, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            int yearOffset = end.Year - start.Year;
            if (yearOffset > 0)
            {
                return MultiYear(tablenames, selectFields, timeField, start, end, where, connConfig);
            }
            else
            {
                if (end.Month - start.Month > 0)
                {
                    return SingleYear(tablenames, selectFields, timeField, start, end, where, connConfig);
                }
                else
                {
                    return SingleMonth(tablenames, selectFields, timeField, start.Year, start.Month, start.Day, end.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
                }
            }
        }
        /// <summary>
        /// 获取查询日表数据的SQL语句
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="where"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static string GetQuerySQLRange_DayLog(string tablenames, string selectFields, string timeField, DateTime start, DateTime end, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            int yearOffset = end.Year - start.Year;
            if (yearOffset > 0)
            {
                return MultiYear(tablenames, selectFields, timeField, start, end, where, connConfig);
            }
            else
            {
                if (end.Month - start.Month > 0)
                {
                    return SingleYear(tablenames, selectFields, timeField, start, end, where, connConfig);
                }
                else
                {
                    if (end.Day - start.Day > 0)
                    {
                        return SingleMonth(tablenames, selectFields, timeField, start.Year, start.Month, start.Day, end.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
                    }
                    else
                    {
                        return SingleDay(tablenames, selectFields, timeField, start.Year, start.Month, start.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
                    }
                }
            }
        }
        private static string GetSQL(string tablenames, string selectFields, string timeField, string yyyyMM, string s, string e, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return "";
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return "";
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMM + ".");
            string sql = "select " + selectFields + " from " + tables + " where " + t + " >= '" + s + "' and " + t + " < '" + e + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMM + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMM + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMM + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            return sql;
        }
        private static string GetSQL_DayLog(string tablenames, string selectFields, string timeField, string yyyyMMdd, string s, string e, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return "";
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return "";
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMMdd + ".");
            string sql = "select " + selectFields + " from " + tables + " where " + t + " >= '" + s + "' and " + t + " < '" + e + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMMdd + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMMdd + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMMdd + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            return sql;
        }
        private static string MultiYear(string tablenames, string selectFields, string timeField, DateTime start, DateTime end, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string dt = "";
            int startYear = start.Year;
            int endYear = end.Year;
            string dt1 = SingleYear(tablenames, selectFields, timeField, start, new DateTime(start.Year, 12, 31, 23, 59, 59), where, connConfig);
            dt += dt1;
            for (int i = startYear + 1; i < endYear; i++)
            {
                string d = SingleYear(tablenames, selectFields, timeField, new DateTime(i, 1, 1, 0, 0, 0), new DateTime(i, 12, 31, 23, 59, 59), where, connConfig);
                if (!string.IsNullOrEmpty(d))
                {
                    if (string.IsNullOrEmpty(dt))
                        dt = d;
                    else
                        dt += " UNION ALL " + d;
                }
            }
            string dt2 = SingleYear(tablenames, selectFields, timeField, new DateTime(end.Year, 1, 1, 0, 0, 0), end, where, connConfig);
            if (!string.IsNullOrEmpty(dt2))
            {
                if (string.IsNullOrEmpty(dt))
                    dt = dt2;
                else
                    dt += " UNION ALL " + dt2;
            }
            return dt;
        }
        private static string SingleYear(string tablenames, string selectFields, string timeField, DateTime start, DateTime end, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string dt = "";
            int startMonth = start.Month;
            int endMonth = end.Month;
            int year = start.Year;
            string dt1 = SingleMonth(tablenames, selectFields, timeField, year, start.Month, start.Day, DateTime.DaysInMonth(year, start.Month), start.Hour + ":" + start.Minute + ":" + start.Second, "23:59:59", where, connConfig);
            dt += dt1;
            for (int i = startMonth + 1; i < endMonth; i++)
            {
                string d = SingleMonth(tablenames, selectFields, timeField, year, i, 1, DateTime.DaysInMonth(year, i), "00:00:00", "23:59:59", where, connConfig);
                if (!string.IsNullOrEmpty(d))
                {
                    if (string.IsNullOrEmpty(dt))
                        dt = d;
                    else
                        dt += " UNION ALL " + d;
                }
            }
            string dt2 = SingleMonth(tablenames, selectFields, timeField, year, end.Month, 1, end.Day, "00:00:00", end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
            if (!string.IsNullOrEmpty(dt2))
            {
                if (string.IsNullOrEmpty(dt))
                    dt = dt2;
                else
                    dt += " UNION ALL " + dt2;
            }
            return dt;
        }
        private static string SingleMonth(string tablenames, string selectFields, string timeField, int year, int month, int startDay, int endDay, string startTime, string endTime, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMM = year.ToString().PadLeft(4, '0') + month.ToString().PadLeft(2, '0');
            string s = yyyyMM.Substring(0, 4) + "-" + yyyyMM.Substring(4, 2) + "-" + startDay.ToString().PadLeft(2, '0') + " " + startTime;
            string e = yyyyMM.Substring(0, 4) + "-" + yyyyMM.Substring(4, 2) + "-" + endDay.ToString().PadLeft(2, '0') + " " + endTime;
            return GetSQL(tablenames, selectFields, timeField, yyyyMM, s, Convert.ToDateTime(e).AddSeconds(1).ToString(), where, connConfig);
        }
        private static string SingleDay(string tablenames, string selectFields, string timeField, int year, int month, int day, string startTime, string endTime, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMMdd = year.ToString().PadLeft(4, '0') + month.ToString().PadLeft(2, '0') + day.ToString().PadLeft(2, '0');
            string s = yyyyMMdd.Substring(0, 4) + "-" + yyyyMMdd.Substring(4, 2) + "-" + yyyyMMdd.Substring(6, 2) + " " + startTime;
            string e = yyyyMMdd.Substring(0, 4) + "-" + yyyyMMdd.Substring(4, 2) + "-" + yyyyMMdd.Substring(6, 2) + " " + endTime;
            return GetSQL_DayLog(tablenames, selectFields, timeField, yyyyMMdd, s, Convert.ToDateTime(e).AddSeconds(1).ToString(), where, connConfig);
        }
    }

    /// <summary>
    /// 生成更新动态表的SQL脚本类库
    /// </summary>
    public static class MergeSQLUpdate
    {
        /// <summary>
        /// 获取更新月表数据的SQL语句
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="updateFields"></param>
        /// <param name="timeField"></param>
        /// <param name="where"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static string GetUpdateSQL(string tablenames, string updateFields, string timeField, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DateTime start;
            DateTime end;
            MergeCommon.GetHistoryTime(tablenames.Split(',')[0].Trim(), timeField, out start, out end, connConfig);
            int yearOffset = end.Year - start.Year;
            if (yearOffset > 0)
            {
                return MultiYear(tablenames, updateFields, timeField, start, end, where, connConfig);
            }
            else
            {
                if (end.Month - start.Month > 0)
                {
                    return SingleYear(tablenames, updateFields, timeField, start, end, where, connConfig);
                }
                else
                {
                    return SingleMonth(tablenames, updateFields, timeField, start.Year, start.Month, start.Day, end.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
                }
            }
        }
        /// <summary>
        /// 获取更新日表数据的SQL语句
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="updateFields"></param>
        /// <param name="timeField"></param>
        /// <param name="where"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static string GetUpdateSQL_DayLog(string tablenames, string updateFields, string timeField, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DateTime start;
            DateTime end;
            MergeCommon.GetHistoryTime(tablenames.Split(',')[0].Trim(), timeField, out start, out end, connConfig);
            int yearOffset = end.Year - start.Year;
            if (yearOffset > 0)
            {
                return MultiYear(tablenames, updateFields, timeField, start, end, where, connConfig);
            }
            else
            {
                if (end.Month - start.Month > 0)
                {
                    return SingleYear(tablenames, updateFields, timeField, start, end, where, connConfig);
                }
                else
                {
                    if (end.Day - start.Day > 0)
                    {
                        return SingleMonth(tablenames, updateFields, timeField, start.Year, start.Month, start.Day, end.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
                    }
                    else
                    {
                        return SingleDay(tablenames, updateFields, timeField, start.Year, start.Month, start.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
                    }
                }
            }
        }
        /// <summary>
        /// 获取更新月表数据的SQL语句
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="updateFields"></param>
        /// <param name="timeField"></param>
        /// <param name="time"></param>
        /// <param name="where"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static string GetUpdateSQL(string tablenames, string updateFields, string timeField, DateTime time, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMM = MergeCommon.GetYYYYMM(time);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return "";
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return "";
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMM + ".");
            string sql = "update " + tables + " set " + updateFields + " where " + t + " = '" + time.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMM + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMM + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMM + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            return sql;
        }
        /// <summary>
        /// 获取更新日表数据的SQL语句
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="updateFields"></param>
        /// <param name="timeField"></param>
        /// <param name="time"></param>
        /// <param name="where"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static string GetUpdateSQL_DayLog(string tablenames, string updateFields, string timeField, DateTime time, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMMdd = MergeCommon.GetYYYYMMDD(time);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return "";
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return "";
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMMdd + ".");
            string sql = "update " + tables + " set " + updateFields + " where " + t + " = '" + time.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMMdd + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMMdd + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMMdd + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            return sql;
        }
        /// <summary>
        /// 获取更新月表数据的SQL语句
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="updateFields"></param>
        /// <param name="timeField"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="where"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static string GetUpdateSQL(string tablenames, string updateFields, string timeField, DateTime start, DateTime end, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            int yearOffset = end.Year - start.Year;
            if (yearOffset > 0)
            {
                return MultiYear(tablenames, updateFields, timeField, start, end, where, connConfig);
            }
            else
            {
                if (end.Month - start.Month > 0)
                {
                    return SingleYear(tablenames, updateFields, timeField, start, end, where, connConfig);
                }
                else
                {
                    if (end.Day - start.Day > 0)
                    {
                        return SingleMonth(tablenames, updateFields, timeField, start.Year, start.Month, start.Day, end.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
                    }
                    else
                    {
                        return SingleDay(tablenames, updateFields, timeField, start.Year, start.Month, start.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
                    }
                }
            }
        }
        private static string GetSQL(string tablenames, string updateFields, string timeField, string yyyyMM, string s, string e, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return "";
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return "";
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMM + ".");
            string sql = "update " + tables + " set " + updateFields + " where " + t + " >= '" + s + "' and " + t + " < '" + e + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMM + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMM + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMM + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            return sql;
        }
        private static string GetSQL_DayLog(string tablenames, string updateFields, string timeField, string yyyyMMdd, string s, string e, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return "";
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return "";
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMMdd + ".");
            string sql = "update " + tables + " set " + updateFields + " where " + t + " >= '" + s + "' and " + t + " < '" + e + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMMdd + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMMdd + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMMdd + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            return sql;
        }
        private static string MultiYear(string tablenames, string updateFields, string timeField, DateTime start, DateTime end, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string dt = "";
            int startYear = start.Year;
            int endYear = end.Year;
            string dt1 = SingleYear(tablenames, updateFields, timeField, start, new DateTime(start.Year, 12, 31, 23, 59, 59), where, connConfig);
            dt += dt1;
            for (int i = startYear + 1; i < endYear; i++)
            {
                string d = SingleYear(tablenames, updateFields, timeField, new DateTime(i, 1, 1, 0, 0, 0), new DateTime(i, 12, 31, 23, 59, 59), where, connConfig);
                if (!string.IsNullOrEmpty(d))
                {
                    if (string.IsNullOrEmpty(dt))
                        dt = d;
                    else
                        dt += " UNION ALL " + d;
                }
            }
            string dt2 = SingleYear(tablenames, updateFields, timeField, new DateTime(end.Year, 1, 1, 0, 0, 0), end, where, connConfig);
            if (!string.IsNullOrEmpty(dt2))
            {
                if (string.IsNullOrEmpty(dt))
                    dt = dt2;
                else
                    dt += " UNION ALL " + dt2;
            }
            return dt;
        }
        private static string SingleYear(string tablenames, string updateFields, string timeField, DateTime start, DateTime end, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string dt = "";
            int startMonth = start.Month;
            int endMonth = end.Month;
            int year = start.Year;
            string dt1 = SingleMonth(tablenames, updateFields, timeField, year, start.Month, start.Day, DateTime.DaysInMonth(year, start.Month), start.Hour + ":" + start.Minute + ":" + start.Second, "23:59:59", where, connConfig);
            dt += dt1;
            for (int i = startMonth + 1; i < endMonth; i++)
            {
                string d = SingleMonth(tablenames, updateFields, timeField, year, i, 1, DateTime.DaysInMonth(year, i), "00:00:00", "23:59:59", where, connConfig);
                if (!string.IsNullOrEmpty(d))
                {
                    if (string.IsNullOrEmpty(dt))
                        dt = d;
                    else
                        dt += " UNION ALL " + d;
                }
            }
            string dt2 = SingleMonth(tablenames, updateFields, timeField, year, end.Month, 1, end.Day, "00:00:00", end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
            if (!string.IsNullOrEmpty(dt2))
            {
                if (string.IsNullOrEmpty(dt))
                    dt = dt2;
                else
                    dt += " UNION ALL " + dt2;
            }
            return dt;
        }
        private static string SingleMonth(string tablenames, string updateFields, string timeField, int year, int month, int startDay, int endDay, string startTime, string endTime, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMM = year.ToString().PadLeft(4, '0') + month.ToString().PadLeft(2, '0');
            string s = yyyyMM.Substring(0, 4) + "-" + yyyyMM.Substring(4, 2) + "-" + startDay.ToString().PadLeft(2, '0') + " " + startTime;
            string e = yyyyMM.Substring(0, 4) + "-" + yyyyMM.Substring(4, 2) + "-" + endDay.ToString().PadLeft(2, '0') + " " + endTime;
            return GetSQL(tablenames, updateFields, timeField, yyyyMM, s, Convert.ToDateTime(e).AddSeconds(1).ToString(), where, connConfig);
        }
        private static string SingleDay(string tablenames, string updateFields, string timeField, int year, int month, int day, string startTime, string endTime, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMMdd = year.ToString().PadLeft(4, '0') + month.ToString().PadLeft(2, '0') + day.ToString().PadLeft(2, '0');
            string s = yyyyMMdd.Substring(0, 4) + "-" + yyyyMMdd.Substring(4, 2) + "-" + yyyyMMdd.Substring(6, 2) + " " + startTime;
            string e = yyyyMMdd.Substring(0, 4) + "-" + yyyyMMdd.Substring(4, 2) + "-" + yyyyMMdd.Substring(6, 2) + " " + endTime;
            return GetSQL_DayLog(tablenames, updateFields, timeField, yyyyMMdd, s, e, where, connConfig);
        }
    }

    /// <summary>
    /// 查询合并动态表的结果集类库
    /// </summary>
    public static class MergeQuery
    {
        /// <summary>
        /// 获取有史以来的所有数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static DataTable GetData(string tablenames, string selectFields, string timeField, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DataTable dt = new DataTable("NULL");
            DateTime start;
            DateTime end;
            MergeCommon.GetHistoryTime(tablenames.Split(',')[0].Trim(), timeField, out start, out end, connConfig);
            int yearOffset = end.Year - start.Year;
            if (yearOffset > 0)
            {
                dt = MultiYear(tablenames, selectFields, timeField, start, end, where, orderby, connConfig);
            }
            else
            {
                if (end.Month - start.Month > 0)
                {
                    dt = SingleYear(tablenames, selectFields, timeField, start, end, where, orderby, connConfig);
                }
                else
                {
                    dt = SingleMonth(tablenames, selectFields, timeField, start.Year, start.Month, start.Day, end.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, orderby, connConfig);
                }
            }
            return dt;
        }
        /// <summary>
        /// 获取有史以来的所有数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static DataTable GetData_DayLog(string tablenames, string selectFields, string timeField, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DataTable dt = new DataTable("NULL");
            DateTime start;
            DateTime end;
            MergeCommon.GetHistoryTime(tablenames.Split(',')[0].Trim(), timeField, out start, out end, connConfig);
            int yearOffset = end.Year - start.Year;
            if (yearOffset > 0)
            {
                dt = MultiYear(tablenames, selectFields, timeField, start, end, where, orderby, connConfig);
            }
            else
            {
                if (end.Month - start.Month > 0)
                {
                    dt = SingleYear(tablenames, selectFields, timeField, start, end, where, orderby, connConfig);
                }
                else
                {
                    if (end.Day - start.Day > 0)
                    {
                        dt = SingleMonth(tablenames, selectFields, timeField, start.Year, start.Month, start.Day, end.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, orderby, connConfig);
                    }
                    else
                    {
                        dt = SingleDay(tablenames, selectFields, timeField, start.Year, start.Month, start.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, orderby, connConfig);
                    }
                }
            }
            return dt;
        }
        /// <summary>
        /// 获取在指定时间段内的所有数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static DataTable GetDataRange(string tablenames, string selectFields, string timeField, DateTime start, DateTime end, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DataTable dt = new DataTable("NULL");
            int yearOffset = end.Year - start.Year;
            if (yearOffset > 0)
            {
                dt = MultiYear(tablenames, selectFields, timeField, start, end, where, orderby, connConfig);
            }
            else
            {
                if (end.Month - start.Month > 0)
                {
                    dt = SingleYear(tablenames, selectFields, timeField, start, end, where, orderby, connConfig);
                }
                else
                {
                    dt = SingleMonth(tablenames, selectFields, timeField, start.Year, start.Month, start.Day, end.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, orderby, connConfig);
                }
            }
            return dt;
        }
        /// <summary>
        /// 获取在指定时间段内的所有数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static DataTable GetDataRange_DayLog(string tablenames, string selectFields, string timeField, DateTime start, DateTime end, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DataTable dt = new DataTable("NULL");
            int yearOffset = end.Year - start.Year;
            if (yearOffset > 0)
            {
                dt = MultiYear(tablenames, selectFields, timeField, start, end, where, orderby, connConfig);
            }
            else
            {
                if (end.Month - start.Month > 0)
                {
                    dt = SingleYear(tablenames, selectFields, timeField, start, end, where, orderby, connConfig);
                }
                else
                {
                    if (end.Day - start.Day > 0)
                    {
                        dt = SingleMonth(tablenames, selectFields, timeField, start.Year, start.Month, start.Day, end.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, orderby, connConfig);
                    }
                    else
                    {
                        dt = SingleDay(tablenames, selectFields, timeField, start.Year, start.Month, start.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, orderby, connConfig);
                    }
                }
            }
            return dt;
        }
        /// <summary>
        /// 获取从指定时间开始之后的所有数据(log表一定得放在tablenames的第一位)
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="start"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static DataTable GetDataAfter(string tablenames, string selectFields, string timeField, DateTime start, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DateTime end = MergeCommon.GetEndTime(tablenames.Split(',')[0].Trim(), timeField, connConfig);
            return GetDataRange(tablenames, selectFields, timeField, start, end, where, orderby, connConfig);
        }
        /// <summary>
        /// 获取从指定时间开始之后的所有数据(log表一定得放在tablenames的第一位)
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="start"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static DataTable GetDataAfter_DayLog(string tablenames, string selectFields, string timeField, DateTime start, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DateTime end = MergeCommon.GetEndTime(tablenames.Split(',')[0].Trim(), timeField, connConfig);
            return GetDataRange_DayLog(tablenames, selectFields, timeField, start, end, where, orderby, connConfig);
        }
        /// <summary>
        /// 获取在指定的时间之前所有数据(log表一定得放在tablenames的第一位)
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="end"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static DataTable GetDataBefore(string tablenames, string selectFields, string timeField, DateTime end, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DateTime start = MergeCommon.GetStartTime(tablenames.Split(',')[0].Trim(), timeField, connConfig);
            return GetDataRange(tablenames, selectFields, timeField, start, end, where, orderby, connConfig);
        }
        /// <summary>
        /// 获取在指定的时间之前所有数据(log表一定得放在tablenames的第一位)
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="end"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static DataTable GetDataBefore_DayLog(string tablenames, string selectFields, string timeField, DateTime end, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DateTime start = MergeCommon.GetStartTime(tablenames.Split(',')[0].Trim(), timeField, connConfig);
            return GetDataRange_DayLog(tablenames, selectFields, timeField, start, end, where, orderby, connConfig);
        }
        /// <summary>
        /// 获取当月的数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static DataTable GetDataCurrentMonth(string tablenames, string selectFields, string timeField, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            return GetDataRange(tablenames, selectFields, timeField, new DateTime(DateTime.Now.Year, DateTime.Now.Month, 1, 0, 0, 0), new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.DaysInMonth(DateTime.Now.Year, DateTime.Now.Month), 23, 59, 59), where, orderby, connConfig);
        }
        /// <summary>
        /// 获取当月的数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static DataTable GetDataCurrentMonth_DayLog(string tablenames, string selectFields, string timeField, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            return GetDataRange_DayLog(tablenames, selectFields, timeField, new DateTime(DateTime.Now.Year, DateTime.Now.Month, 1, 0, 0, 0), new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.DaysInMonth(DateTime.Now.Year, DateTime.Now.Month), 23, 59, 59), where, orderby, connConfig);
        }
        private static DataTable MultiYear(string tablenames, string selectFields, string timeField, DateTime start, DateTime end, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DataTable dt = new DataTable("NULL");
            int startYear = start.Year;
            int endYear = end.Year;
            DataTable dt1 = SingleYear(tablenames, selectFields, timeField, start, new DateTime(start.Year, 12, 31, 23, 59, 59), where, orderby, connConfig);
            if (dt1.TableName != "NULL")
            {
                dt = dt1.Clone();
                if (dt1.Rows.Count > 0)
                    dt.Merge(dt1);
            }
            for (int i = startYear + 1; i < endYear; i++)
            {
                DataTable d = SingleYear(tablenames, selectFields, timeField, new DateTime(i, 1, 1, 0, 0, 0), new DateTime(i, 12, 31, 23, 59, 59), where, orderby, connConfig);
                if (d.TableName != "NULL")
                {
                    dt = d.Clone();
                    if (d.Rows.Count > 0)
                        dt.Merge(d);
                }
            }
            DataTable dt2 = SingleYear(tablenames, selectFields, timeField, new DateTime(end.Year, 1, 1, 0, 0, 0), end, where, orderby, connConfig);
            if (dt2.TableName != "NULL")
            {
                dt = dt2.Clone();
                if (dt2.Rows.Count > 0)
                    dt.Merge(dt2);
            }
            return dt;
        }
        private static DataTable SingleYear(string tablenames, string selectFields, string timeField, DateTime start, DateTime end, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DataTable dt = new DataTable("NULL");
            int startMonth = start.Month;
            int endMonth = end.Month;
            int year = start.Year;
            DataTable dt1 = SingleMonth(tablenames, selectFields, timeField, year, start.Month, start.Day, DateTime.DaysInMonth(year, start.Month), start.Hour + ":" + start.Minute + ":" + start.Second, "23:59:59", where, orderby, connConfig);
            if (dt1.TableName != "NULL")
            {
                dt = dt1.Clone();
                if (dt1.Rows.Count > 0)
                    dt.Merge(dt1);
            }
            for (int i = startMonth + 1; i < endMonth; i++)
            {
                DataTable d = SingleMonth(tablenames, selectFields, timeField, year, i, 1, DateTime.DaysInMonth(year, i), "00:00:00", "23:59:59", where, orderby, connConfig);

                if (d.TableName != "NULL")
                {
                    dt = d.Clone();
                    if (d.Rows.Count > 0)
                        dt.Merge(d);
                }
            }
            DataTable dt2 = SingleMonth(tablenames, selectFields, timeField, year, end.Month, 1, end.Day, "00:00:00", end.Hour + ":" + end.Minute + ":" + end.Second, where, orderby, connConfig);
            if (dt2.TableName != "NULL")
            {
                dt = dt2.Clone();
                if (dt2.Rows.Count > 0)
                    dt.Merge(dt2);
            }
            return dt;
        }
        private static DataTable SingleMonth(string tablenames, string selectFields, string timeField, int year, int month, int startDay, int endDay, string startTime, string endTime, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMM = year.ToString().PadLeft(4, '0') + month.ToString().PadLeft(2, '0');
            string s = yyyyMM.Substring(0, 4) + "-" + yyyyMM.Substring(4, 2) + "-" + startDay.ToString().PadLeft(2, '0') + " " + startTime;
            string e = yyyyMM.Substring(0, 4) + "-" + yyyyMM.Substring(4, 2) + "-" + endDay.ToString().PadLeft(2, '0') + " " + endTime;
            return GetTable(tablenames, selectFields, timeField, yyyyMM, s, Convert.ToDateTime(e).AddSeconds(1).ToString(), where, orderby, connConfig);
        }
        private static DataTable SingleDay(string tablenames, string selectFields, string timeField, int year, int month, int day, string startTime, string endTime, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMMdd = year.ToString().PadLeft(4, '0') + month.ToString().PadLeft(2, '0') + day.ToString().PadLeft(2, '0');
            string s = yyyyMMdd.Substring(0, 4) + "-" + yyyyMMdd.Substring(4, 2) + "-" + day.ToString().PadLeft(2, '0') + " " + startTime;
            string e = yyyyMMdd.Substring(0, 4) + "-" + yyyyMMdd.Substring(4, 2) + "-" + day.ToString().PadLeft(2, '0') + " " + endTime;
            return GetTable_DayLog(tablenames, selectFields, timeField, yyyyMMdd, s, Convert.ToDateTime(e).AddSeconds(1).ToString(), where, orderby, connConfig);
        }
        private static DataTable GetTable(string tablenames, string selectFields, string timeField, string yyyyMM, string s, string e, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return new DataTable("NULL");
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return new DataTable("NULL");
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMM + ".");
            string sql = "select " + selectFields + " from " + tables + " where " + t + " >= '" + s + "' and " + t + " < '" + e + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMM + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMM + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMM + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            if (!string.IsNullOrEmpty(orderby))
            {
                orderby = orderby.Trim().ToLower();
                if (orderby.Contains("_log "))
                    orderby = orderby.Replace("_log ", "_log" + yyyyMM + " ");
                if (orderby.Contains("_log."))
                    orderby = orderby.Replace("_log.", "_log" + yyyyMM + ".");
                if (orderby.Contains("_log)"))
                    orderby = orderby.Replace("_log)", "_log" + yyyyMM + ")");
                if (!orderby.StartsWith("order by "))
                    orderby = "order by " + orderby;
                sql += " " + orderby;
            }
            DataTable dt = SqlHelper.ExecuteQueryDataTable(sql);
            return dt;
        }
        private static DataTable GetTable_DayLog(string tablenames, string selectFields, string timeField, string yyyyMMdd, string s, string e, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return new DataTable("NULL");
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return new DataTable("NULL");
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMMdd + ".");
            string sql = "select " + selectFields + " from " + tables + " where " + t + " >= '" + s + "' and " + t + " < '" + e + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMMdd + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMMdd + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMMdd + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            if (!string.IsNullOrEmpty(orderby))
            {
                orderby = orderby.Trim().ToLower();
                if (orderby.Contains("_log "))
                    orderby = orderby.Replace("_log ", "_log" + yyyyMMdd + " ");
                if (orderby.Contains("_log."))
                    orderby = orderby.Replace("_log.", "_log" + yyyyMMdd + ".");
                if (orderby.Contains("_log)"))
                    orderby = orderby.Replace("_log)", "_log" + yyyyMMdd + ")");
                if (!orderby.StartsWith("order by "))
                    orderby = "order by " + orderby;
                sql += " " + orderby;
            }
            DataTable dt = SqlHelper.ExecuteQueryDataTable(sql);
            return dt;
        }
        /// <summary>
        /// 获取指定时间的数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="time"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static DataTable GetDataAt(string tablenames, string selectFields, string timeField, DateTime time, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMM = MergeCommon.GetYYYYMM(time);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return new DataTable("NULL");
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return new DataTable("NULL");
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMM + ".");
            string sql = "select " + selectFields + " from " + tables + " where " + t + " = '" + time.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMM + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMM + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMM + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            if (!string.IsNullOrEmpty(orderby))
            {
                orderby = orderby.Trim().ToLower();
                if (orderby.Contains("_log "))
                    orderby = orderby.Replace("_log ", "_log" + yyyyMM + " ");
                if (orderby.Contains("_log."))
                    orderby = orderby.Replace("_log.", "_log" + yyyyMM + ".");
                if (orderby.Contains("_log)"))
                    orderby = orderby.Replace("_log)", "_log" + yyyyMM + ")");
                if (!orderby.StartsWith("order by "))
                    orderby = "order by " + orderby;
                sql += " " + orderby;
            }
            DataTable dt = SqlHelper.ExecuteQueryDataTable(sql);
            return dt;
        }
        /// <summary>
        /// 获取指定时间的数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="time"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static DataTable GetDataAt_DayLog(string tablenames, string selectFields, string timeField, DateTime time, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMMdd = MergeCommon.GetYYYYMMDD(time);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return new DataTable("NULL");
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return new DataTable("NULL");
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMMdd + ".");
            string sql = "select " + selectFields + " from " + tables + " where " + t + " = '" + time.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMMdd + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMMdd + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMMdd + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            if (!string.IsNullOrEmpty(orderby))
            {
                orderby = orderby.Trim().ToLower();
                if (orderby.Contains("_log "))
                    orderby = orderby.Replace("_log ", "_log" + yyyyMMdd + " ");
                if (orderby.Contains("_log."))
                    orderby = orderby.Replace("_log.", "_log" + yyyyMMdd + ".");
                if (orderby.Contains("_log)"))
                    orderby = orderby.Replace("_log)", "_log" + yyyyMMdd + ")");
                if (!orderby.StartsWith("order by "))
                    orderby = "order by " + orderby;
                sql += " " + orderby;
            }
            DataTable dt = SqlHelper.ExecuteQueryDataTable(sql);
            return dt;
        }
        /// <summary>
        /// 获取指定时间的数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="time"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static SqlDataAdapter GetAdapterAt(string tablenames, string selectFields, string timeField, DateTime time, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMM = MergeCommon.GetYYYYMM(time);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return new SqlDataAdapter();
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return new SqlDataAdapter();
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMM + ".");
            string sql = "select " + selectFields + " from " + tables + " where " + t + " = '" + time.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMM + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMM + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMM + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            if (!string.IsNullOrEmpty(orderby))
            {
                orderby = orderby.Trim().ToLower();
                if (orderby.Contains("_log "))
                    orderby = orderby.Replace("_log ", "_log" + yyyyMM + " ");
                if (orderby.Contains("_log."))
                    orderby = orderby.Replace("_log.", "_log" + yyyyMM + ".");
                if (orderby.Contains("_log)"))
                    orderby = orderby.Replace("_log)", "_log" + yyyyMM + ")");
                if (!orderby.StartsWith("order by "))
                    orderby = "order by " + orderby;
                sql += " " + orderby;
            }
            SqlDataAdapter adp = (SqlDataAdapter)SqlHelper.GetDataAdapter(sql);
            return adp;
        }
        /// <summary>
        /// 获取指定时间的数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="time"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static SqlDataAdapter GetAdapterAt_DayLog(string tablenames, string selectFields, string timeField, DateTime time, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMMdd = MergeCommon.GetYYYYMMDD(time);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return new SqlDataAdapter();
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return new SqlDataAdapter();
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMMdd + ".");
            string sql = "select " + selectFields + " from " + tables + " where " + t + " = '" + time.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMMdd + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMMdd + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMMdd + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            if (!string.IsNullOrEmpty(orderby))
            {
                orderby = orderby.Trim().ToLower();
                if (orderby.Contains("_log "))
                    orderby = orderby.Replace("_log ", "_log" + yyyyMMdd + " ");
                if (orderby.Contains("_log."))
                    orderby = orderby.Replace("_log.", "_log" + yyyyMMdd + ".");
                if (orderby.Contains("_log)"))
                    orderby = orderby.Replace("_log)", "_log" + yyyyMMdd + ")");
                if (!orderby.StartsWith("order by "))
                    orderby = "order by " + orderby;
                sql += " " + orderby;
            }
            SqlDataAdapter adp = (SqlDataAdapter)SqlHelper.GetDataAdapter(sql);
            return adp;
        }
        /// <summary>
        /// 获取指定月份内某个时间段的数据(不能够跨月份！)
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static SqlDataAdapter GetAdapter(string tablenames, string selectFields, string timeField, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DateTime theMonthStart;
            DateTime theMonthEnd;
            MergeCommon.GetHistoryTime(tablenames.Split(',')[0].Trim(), timeField, out theMonthStart, out theMonthEnd, connConfig);
            string yyyyMM = MergeCommon.GetYYYYMM(theMonthStart);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return new SqlDataAdapter();
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return new SqlDataAdapter();
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMM + ".");
            string sql = "select " + selectFields + " from " + tables + " where " + t + " >= '" + theMonthStart.ToString("yyyy-MM-dd HH:mm:ss.fff") + "' and " + t + " < '" + theMonthEnd.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMM + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMM + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMM + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            if (!string.IsNullOrEmpty(orderby))
            {
                orderby = orderby.Trim().ToLower();
                if (orderby.Contains("_log "))
                    orderby = orderby.Replace("_log ", "_log" + yyyyMM + " ");
                if (orderby.Contains("_log."))
                    orderby = orderby.Replace("_log.", "_log" + yyyyMM + ".");
                if (orderby.Contains("_log)"))
                    orderby = orderby.Replace("_log)", "_log" + yyyyMM + ")");
                if (!orderby.StartsWith("order by "))
                    orderby = "order by " + orderby;
                sql += " " + orderby;
            }
            SqlDataAdapter adp = (SqlDataAdapter)SqlHelper.GetDataAdapter(sql);
            return adp;
        }
        /// <summary>
        /// 获取指定月份内某个时间段的数据(不能够跨月份！)
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static SqlDataAdapter GetAdapter_DayLog(string tablenames, string selectFields, string timeField, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DateTime theMonthStart;
            DateTime theMonthEnd;
            MergeCommon.GetHistoryTime(tablenames.Split(',')[0].Trim(), timeField, out theMonthStart, out theMonthEnd, connConfig);
            string yyyyMMdd = MergeCommon.GetYYYYMMDD(theMonthStart);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return new SqlDataAdapter();
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return new SqlDataAdapter();
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMMdd + ".");
            string sql = "select " + selectFields + " from " + tables + " where " + t + " >= '" + theMonthStart.ToString("yyyy-MM-dd HH:mm:ss.fff") + "' and " + t + " < '" + theMonthEnd.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMMdd + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMMdd + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMMdd + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            if (!string.IsNullOrEmpty(orderby))
            {
                orderby = orderby.Trim().ToLower();
                if (orderby.Contains("_log "))
                    orderby = orderby.Replace("_log ", "_log" + yyyyMMdd + " ");
                if (orderby.Contains("_log."))
                    orderby = orderby.Replace("_log.", "_log" + yyyyMMdd + ".");
                if (orderby.Contains("_log)"))
                    orderby = orderby.Replace("_log)", "_log" + yyyyMMdd + ")");
                if (!orderby.StartsWith("order by "))
                    orderby = "order by " + orderby;
                sql += " " + orderby;
            }
            SqlDataAdapter adp = (SqlDataAdapter)SqlHelper.GetDataAdapter(sql);
            return adp;
        }
        /// <summary>
        /// 获取指定月份内某个时间段的数据(不能够跨月份！)
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="theMonthStart"></param>
        /// <param name="theMonthEnd"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static SqlDataAdapter GetAdapterRange(string tablenames, string selectFields, string timeField, DateTime theMonthStart, DateTime theMonthEnd, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMM = MergeCommon.GetYYYYMM(theMonthStart);
            theMonthEnd = theMonthEnd.AddSeconds(1);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return new SqlDataAdapter();
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return new SqlDataAdapter();
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMM + ".");
            string sql = "select " + selectFields + " from " + tables + " where " + t + " >= '" + theMonthStart.ToString("yyyy-MM-dd HH:mm:ss.fff") + "' and " + t + " < '" + theMonthEnd.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMM + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMM + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMM + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            if (!string.IsNullOrEmpty(orderby))
            {
                orderby = orderby.Trim().ToLower();
                if (orderby.Contains("_log "))
                    orderby = orderby.Replace("_log ", "_log" + yyyyMM + " ");
                if (orderby.Contains("_log."))
                    orderby = orderby.Replace("_log.", "_log" + yyyyMM + ".");
                if (orderby.Contains("_log)"))
                    orderby = orderby.Replace("_log)", "_log" + yyyyMM + ")");
                if (!orderby.StartsWith("order by "))
                    orderby = "order by " + orderby;
                sql += " " + orderby;
            }
            SqlDataAdapter adp = (SqlDataAdapter)SqlHelper.GetDataAdapter(sql);
            return adp;
        }
        /// <summary>
        /// 获取指定月份内某个时间段的数据(不能够跨月份！)
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="theMonthStart"></param>
        /// <param name="theMonthEnd"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static SqlDataAdapter GetAdapterRange_DayLog(string tablenames, string selectFields, string timeField, DateTime theMonthStart, DateTime theMonthEnd, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMMdd = MergeCommon.GetYYYYMMDD(theMonthStart);
            theMonthEnd = theMonthEnd.AddSeconds(1);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return new SqlDataAdapter();
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return new SqlDataAdapter();
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMMdd + ".");
            string sql = "select " + selectFields + " from " + tables + " where " + t + " >= '" + theMonthStart.ToString("yyyy-MM-dd HH:mm:ss.fff") + "' and " + t + " < '" + theMonthEnd.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMMdd + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMMdd + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMMdd + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            if (!string.IsNullOrEmpty(orderby))
            {
                orderby = orderby.Trim().ToLower();
                if (orderby.Contains("_log "))
                    orderby = orderby.Replace("_log ", "_log" + yyyyMMdd + " ");
                if (orderby.Contains("_log."))
                    orderby = orderby.Replace("_log.", "_log" + yyyyMMdd + ".");
                if (orderby.Contains("_log)"))
                    orderby = orderby.Replace("_log)", "_log" + yyyyMMdd + ")");
                if (!orderby.StartsWith("order by "))
                    orderby = "order by " + orderby;
                sql += " " + orderby;
            }
            SqlDataAdapter adp = (SqlDataAdapter)SqlHelper.GetDataAdapter(sql);
            return adp;
        }
        /// <summary>
        /// 获取当月的数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static SqlDataAdapter GetAdapterCurrentMonth(string tablenames, string selectFields, string timeField, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            return GetAdapterRange(tablenames, selectFields, timeField, new DateTime(DateTime.Now.Year, DateTime.Now.Month, 1, 0, 0, 0), new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.DaysInMonth(DateTime.Now.Year, DateTime.Now.Month), 23, 59, 59).AddSeconds(1), where, orderby, connConfig);
        }
        /// <summary>
        /// 获取当月的数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="selectFields"></param>
        /// <param name="timeField"></param>
        /// <param name="where"></param>
        /// <param name="orderby"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static SqlDataAdapter GetAdapterCurrentMonth_DayLog(string tablenames, string selectFields, string timeField, string where, string orderby, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            return GetAdapterRange_DayLog(tablenames, selectFields, timeField, new DateTime(DateTime.Now.Year, DateTime.Now.Month, 1, 0, 0, 0), new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.DaysInMonth(DateTime.Now.Year, DateTime.Now.Month), 23, 59, 59), where, orderby, connConfig);
        }
    }

    /// <summary>
    /// 更新动态表的记录集类库
    /// </summary>
    public static class MergeUpdate
    {
        /// <summary>
        /// 更新月表数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="updateFields"></param>
        /// <param name="timeField"></param>
        /// <param name="where"></param>
        /// <param name="connConfig"></param>
        public static void UpdateData(string tablenames, string updateFields, string timeField, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DateTime start;
            DateTime end;
            MergeCommon.GetHistoryTime(tablenames.Split(',')[0].Trim(), timeField, out start, out end, connConfig);
            int yearOffset = end.Year - start.Year;
            if (yearOffset > 0)
            {
                MultiYear(tablenames, updateFields, timeField, start, end, where, connConfig);
            }
            else
            {
                if (end.Month - start.Month > 0)
                {
                    SingleYear(tablenames, updateFields, timeField, start, end, where, connConfig);
                }
                else
                {
                    SingleMonth(tablenames, updateFields, timeField, start.Year, start.Month, start.Day, end.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
                }
            }
        }
        /// <summary>
        /// 更新日表数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="updateFields"></param>
        /// <param name="timeField"></param>
        /// <param name="where"></param>
        /// <param name="connConfig"></param>
        public static void UpdateData_DayLog(string tablenames, string updateFields, string timeField, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            DateTime start;
            DateTime end;
            MergeCommon.GetHistoryTime(tablenames.Split(',')[0].Trim(), timeField, out start, out end, connConfig);
            int yearOffset = end.Year - start.Year;
            if (yearOffset > 0)
            {
                MultiYear(tablenames, updateFields, timeField, start, end, where, connConfig);
            }
            else
            {
                if (end.Month - start.Month > 0)
                {
                    SingleYear(tablenames, updateFields, timeField, start, end, where, connConfig);
                }
                else
                {
                    if (end.Day - start.Day > 0)
                    {
                        SingleMonth(tablenames, updateFields, timeField, start.Year, start.Month, start.Day, end.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
                    }
                    else
                    {
                        SingleDay(tablenames, updateFields, timeField, start.Year, start.Month, start.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
                    }
                }
            }
        }
        /// <summary>
        /// 更新月表数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="updateFields"></param>
        /// <param name="timeField"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="where"></param>
        /// <param name="connConfig"></param>
        public static void UpdateData(string tablenames, string updateFields, string timeField, DateTime start, DateTime end, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            int yearOffset = end.Year - start.Year;
            if (yearOffset > 0)
            {
                MultiYear(tablenames, updateFields, timeField, start, end, where, connConfig);
            }
            else
            {
                if (end.Month - start.Month > 0)
                {
                    SingleYear(tablenames, updateFields, timeField, start, end, where, connConfig);
                }
                else
                {
                    SingleMonth(tablenames, updateFields, timeField, start.Year, start.Month, start.Day, end.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
                }
            }
        }
        /// <summary>
        /// 更新日表数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="updateFields"></param>
        /// <param name="timeField"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="where"></param>
        /// <param name="connConfig"></param>
        public static void UpdateData_DayLog(string tablenames, string updateFields, string timeField, DateTime start, DateTime end, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            int yearOffset = end.Year - start.Year;
            if (yearOffset > 0)
            {
                MultiYear(tablenames, updateFields, timeField, start, end, where, connConfig);
            }
            else
            {
                if (end.Month - start.Month > 0)
                {
                    SingleYear(tablenames, updateFields, timeField, start, end, where, connConfig);
                }
                else
                {
                    if (end.Day - start.Day > 0)
                    {
                        SingleMonth(tablenames, updateFields, timeField, start.Year, start.Month, start.Day, end.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
                    }
                    else
                    {
                        SingleDay(tablenames, updateFields, timeField, start.Year, start.Month, start.Day, start.Hour + ":" + start.Minute + ":" + start.Second, end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
                    }
                }
            }
        }
        private static void MultiYear(string tablenames, string selectFields, string timeField, DateTime start, DateTime end, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            int startYear = start.Year;
            int endYear = end.Year;
            SingleYear(tablenames, selectFields, timeField, start, new DateTime(start.Year, 12, 31, 23, 59, 59), where, connConfig);
            for (int i = startYear + 1; i < endYear; i++)
            {
                SingleYear(tablenames, selectFields, timeField, new DateTime(i, 1, 1, 0, 0, 0), new DateTime(i, 12, 31, 23, 59, 59), where, connConfig);
            }
            SingleYear(tablenames, selectFields, timeField, new DateTime(end.Year, 1, 1, 0, 0, 0), end, where, connConfig);
        }
        private static void SingleYear(string tablenames, string selectFields, string timeField, DateTime start, DateTime end, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            int startMonth = start.Month;
            int endMonth = end.Month;
            int year = start.Year;
            bool flag = SingleMonth(tablenames, selectFields, timeField, year, start.Month, start.Day, DateTime.DaysInMonth(year, start.Month), start.Hour + ":" + start.Minute + ":" + start.Second, "23:59:59", where, connConfig);
            for (int i = startMonth + 1; i < endMonth; i++)
            {
                flag = SingleMonth(tablenames, selectFields, timeField, year, i, 1, DateTime.DaysInMonth(year, i), "00:00:00", "23:59:59", where, connConfig);
            }
            flag = SingleMonth(tablenames, selectFields, timeField, year, end.Month, 1, end.Day, "00:00:00", end.Hour + ":" + end.Minute + ":" + end.Second, where, connConfig);
        }
        private static bool SingleMonth(string tablenames, string selectFields, string timeField, int year, int month, int startDay, int endDay, string startTime, string endTime, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMM = year.ToString().PadLeft(4, '0') + month.ToString().PadLeft(2, '0');
            string s = yyyyMM.Substring(0, 4) + "-" + yyyyMM.Substring(4, 2) + "-" + startDay.ToString().PadLeft(2, '0') + " " + startTime;
            string e = yyyyMM.Substring(0, 4) + "-" + yyyyMM.Substring(4, 2) + "-" + endDay.ToString().PadLeft(2, '0') + " " + endTime;
            return Update(tablenames, selectFields, timeField, yyyyMM, s, Convert.ToDateTime(e).AddSeconds(1).ToString(), where, connConfig);
        }
        private static bool SingleDay(string tablenames, string selectFields, string timeField, int year, int month, int day, string startTime, string endTime, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMMdd = year.ToString().PadLeft(4, '0') + month.ToString().PadLeft(2, '0') + day.ToString().PadLeft(2, '0');
            string s = yyyyMMdd.Substring(0, 4) + "-" + yyyyMMdd.Substring(4, 2) + "-" + yyyyMMdd.Substring(6, 2) + " " + startTime;
            string e = yyyyMMdd.Substring(0, 4) + "-" + yyyyMMdd.Substring(4, 2) + "-" + yyyyMMdd.Substring(6, 2) + " " + endTime;
            return Update_DayLog(tablenames, selectFields, timeField, yyyyMMdd, s, Convert.ToDateTime(e).AddSeconds(1).ToString(), where, connConfig);
        }
        private static bool Update(string tablenames, string updateFields, string timeField, string yyyyMM, string s, string e, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return false;
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return false;
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMM + ".");
            string sql = "update " + tables + " set " + updateFields + "  where " + t + " >= '" + s + "' and " + t + " < '" + e + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMM + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMM + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMM + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            return SqlHelper.ExecuteNonQuery(sql) > 0;
        }
        private static bool Update_DayLog(string tablenames, string updateFields, string timeField, string yyyyMMdd, string s, string e, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return false;
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return false;
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMMdd + ".");
            string sql = "update " + tables + " set " + updateFields + "  where " + t + " >= '" + s + "' and " + t + " < '" + e + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMMdd + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMMdd + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMMdd + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            return SqlHelper.ExecuteNonQuery(sql) > 0;
        }
        /// <summary>
        /// 更新月表数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="updateFields"></param>
        /// <param name="timeField"></param>
        /// <param name="time"></param>
        /// <param name="where"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static bool UpdateData(string tablenames, string updateFields, string timeField, DateTime time, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMM = MergeCommon.GetYYYYMM(time);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return false;
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return false;
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMM + ".");
            string sql = "update " + tables + " set " + updateFields + " where " + t + " = '" + time.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMM + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMM + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMM + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            return SqlHelper.ExecuteNonQuery(sql) > 0;
        }
        /// <summary>
        /// 更新日表数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="updateFields"></param>
        /// <param name="timeField"></param>
        /// <param name="time"></param>
        /// <param name="where"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static bool UpdateData_DayLog(string tablenames, string updateFields, string timeField, DateTime time, string where, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string yyyyMMdd = MergeCommon.GetYYYYMMDD(time);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return false;
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return false;
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string t = timeField.Trim().ToLower();
            if (t.Contains("_log."))
                t = t.Replace("_log.", "_log" + yyyyMMdd + ".");
            string sql = "update " + tables + " set " + updateFields + " where " + t + " = '" + time.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
            if (!string.IsNullOrEmpty(where))
            {
                string w = where.Trim().ToLower();
                if (w.Contains("_log "))
                    w = w.Replace("_log ", "_log" + yyyyMMdd + " ");
                if (w.Contains("_log."))
                    w = w.Replace("_log.", "_log" + yyyyMMdd + ".");
                if (w.Contains("_log)"))
                    w = w.Replace("_log)", "_log" + yyyyMMdd + ")");
                if (w.StartsWith("and"))
                    sql += " " + w;
                else
                    sql += " and " + w;
            }
            return SqlHelper.ExecuteNonQuery(sql) > 0;
        }
        /// <summary>
        /// 更新月表数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="updateFields"></param>
        /// <param name="idField"></param>
        /// <param name="id"></param>
        /// <param name="yyyyMM"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static bool UpdateData(string tablenames, string updateFields, string idField, string id, string yyyyMM, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return false;
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMM;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return false;
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string sql = "update " + tables + " set " + updateFields + " where " + idField + " = " + id;
            return SqlHelper.ExecuteNonQuery(sql) > 0;
        }
        /// <summary>
        /// 更新日表数据
        /// </summary>
        /// <param name="tablenames"></param>
        /// <param name="updateFields"></param>
        /// <param name="idField"></param>
        /// <param name="id"></param>
        /// <param name="yyyyMMdd"></param>
        /// <param name="connConfig"></param>
        /// <returns></returns>
        public static bool UpdateData_DayLog(string tablenames, string updateFields, string idField, string id, string yyyyMMdd, string connConfig)
        {
            SqlHelper SqlHelper = new SqlHelper(connConfig);
            string tables = "";
            string[] ts = tablenames.Split(',');
            foreach (string table in ts)
            {
                if (tables == "")
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += dynTB;
                        else
                            return false;
                    }
                    else
                        tables += table.Trim();
                }
                else
                {
                    if (table.Trim().ToLower().EndsWith("_log"))
                    {
                        string dynTB = table.Trim() + yyyyMMdd;
                        if (MergeCommon.ExistsTable(dynTB, connConfig))
                            tables += "," + dynTB;
                        else
                            return false;
                    }
                    else
                        tables += "," + table.Trim();
                }
            }
            string sql = "update " + tables + " set " + updateFields + " where " + idField + " = " + id;
            return SqlHelper.ExecuteNonQuery(sql) > 0;
        }
    }
}