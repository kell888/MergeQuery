using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Web;

namespace MergeQueryUtil
{
    /// <summary>
    /// 日志静态类
    /// </summary>
    public static class Logs
    {
        static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();
        /// <summary>
        /// 写入错误级别的信息
        /// </summary>
        /// <param name="msg"></param>
        public static void Error(string msg)
        {
            try
            {
                logger.Error(string.Format("[{0}]:{1}" + Environment.NewLine, DateTime.Now, msg));
            }
            catch { }
        }
        /// <summary>
        /// 写入跟踪级别的信息
        /// </summary>
        /// <param name="msg"></param>
        public static void Trace(string msg)
        {
            try
            {
                logger.Trace(string.Format("[{0}]:{1}" + Environment.NewLine, DateTime.Now, msg));
            }
            catch { }
        }
        /// <summary>
        /// 写入信息级别的信息
        /// </summary>
        /// <param name="msg"></param>
        public static void Info(string msg)
        {
            try
            {
                logger.Info(string.Format("[{0}]:{1}" + Environment.NewLine, DateTime.Now, msg));
            }
            catch { }
        }
        /// <summary>
        /// 初始化日志类(一般在应用程序启动时调用本方法)
        /// </summary>
        public static void Init()
        {
            try
            {
                logger.Info(string.Format("[{0}]:{1}" + Environment.NewLine, DateTime.Now, "应用程序启动中..."));
            }
            catch { }
        }
    }
}