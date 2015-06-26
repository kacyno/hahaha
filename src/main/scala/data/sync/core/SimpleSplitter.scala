package data.sync.core

import java.util.Properties
import data.sync.common.ClientMessages.DBInfo
import data.sync.common.ClusterMessages.TaskInfo
import data.sync.common.{DBUtils, DBSource}
import org.apache.commons.lang.StringUtils
import scala.collection.mutable
import scala.collection.JavaConversions._
/**
 * Created by hesiyuan on 15/6/24.
 */
object SimpleSplitter extends Splitter {
  override def split(jobId: String, dbinfos: Array[DBInfo], num: Int, dir: String): java.util.Set[TaskInfo] = {
    var set = new java.util.HashSet[TaskInfo]()
    val tableNum = dbinfos.foldLeft(0)((r, d) => r + d.tables.size)
    var i = 0;

    if (tableNum >= num) {
      //表比期望任务数还多，则以表的个数做为任务数
      for (db <- dbinfos) {
        for (table <- db.tables)
          set += TaskInfo(jobId + "_task_" + i, jobId, db.sql.format(table), db.ip, db.port, db.user, db.pwd, db.db, table, dir + "tmp/")
        i += 1
      }
    } else {
      /*
       *表的数目小于任务数，需要分表
       * 1) 根据用户提供的主键找出最大与最小id
       * 2) 根据每个表的预估大小（max-min)与期望任务数结合对表进行任务切分
       */
      val tableInfo = mutable.Map[String, (Long, Long)]()
      var sql = "select min(%s),max(%s) from %s"

      for (db <- dbinfos) {
        DBSource.register(this.getClass, db.ip, db.port, db.db, createProperties("utf-8", db.ip, db.port, db.db, db.user, db.pwd))
        for (table <- db.tables) {
          val conn = DBSource.getConnection(this.getClass, db.ip, db.port, db.db)
          try {
            val rs = DBUtils.query(conn, sql.format(db.indexFiled, db.indexFiled, table))
            var min = 0l;
            var max = 0l;
            if (rs.next()) {
              min = rs.getLong(1);
              max = rs.getLong(2);
            }
            tableInfo(table) = (min, max)
          }finally{
            conn.close()
          }
        }
      }
      val totalNum = tableInfo.foldLeft(0l)((n, e) => n + e._2._2 - e._2._1)
      val perNum = totalNum / num;
      for (db <- dbinfos) {
        for (table <- db.tables) {
          val info = tableInfo(table)
          var p = (info._2 - info._1) / perNum
          if (!(p > 0 && ((info._2 - info._1) % perNum) < (perNum / 3)))
            p += 1
          for (j <- 1l to p) {
            var cond = " %s>=%d and %s<%d"
            if (db.sql.toLowerCase.indexOf("where") == -1) {
              cond = " where " + cond
            }else
              cond = " and " + cond
            if (j == p)
              set += TaskInfo(jobId + "_task_" + i, jobId, db.sql.format(table) + cond.format(db.indexFiled, info._1 + perNum * (j - 1), db.indexFiled, info._2+1), db.ip, db.port, db.user, db.pwd, db.db, table, dir + "tmp/")
            else
              set += TaskInfo(jobId + "_task_" + i, jobId, db.sql.format(table) + cond.format(db.indexFiled, info._1 + perNum * (j - 1), db.indexFiled, info._1 + perNum * j), db.ip, db.port, db.user, db.pwd, db.db, table, dir + "tmp/")
            i += 1
          }
        }
      }

    }
    set
  }

  private def createProperties(encode: String, ip: String, port: String, dbname: String, user: String, pwd: String): Properties = {
    val p: Properties = new Properties
    var encodeDetail: String = ""
    if (!StringUtils.isBlank(encode)) {
      encodeDetail = "useUnicode=true&characterEncoding=" + encode + "&"
    }
    val url: String = "jdbc:mysql://" + ip + ":" + port + "/" + dbname + "?" + encodeDetail + "yearIsDateType=false&zeroDateTimeBehavior=convertToNull" + "&defaultFetchSize=" + String.valueOf(Integer.MIN_VALUE)
    p.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    p.setProperty("url", url)
    p.setProperty("username", user)
    p.setProperty("password", pwd)
    p.setProperty("maxActive", "10")
    p.setProperty("initialSize", "10")
    p.setProperty("maxIdle", "1")
    p.setProperty("maxWait", "1000")
    p.setProperty("defaultReadOnly", "true")
    p.setProperty("testOnBorrow", "true")
    p.setProperty("validationQuery", "select 1 from dual")
    return p
  }
}
