package com.luxin.mysql.protocol

import java.io.{DataInputStream, DataOutputStream, EOFException, IOException}
import java.net.{InetSocketAddress, ServerSocket, Socket, SocketException}
import java.sql.SQLException
import java.util
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, ExecutorService, Executors, SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import java.util.regex.Pattern
import java.util.{Properties, ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}

import com.luxin.hydrogen.util.SnowflakeIdWorker
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, NullType, ShortType, TimestampType, VarcharType}

import scala.collection.JavaConverters._

/**
 *1. 支持 mysql命令行执行如果安装了mysql客户端，可以通过<b> mysql -u root -h IP地址 -P 端口号 -D 数据库</b> 来执行
 *2. 支持jdbc jdbc版本 5.1.40--5.1.49可用
 * @param context [[SparkSession]]
 */
class  MysqlProtocolServer(context:SparkSession) extends Logging {
    /**
     * key:connection_id+"-"+WorkerRunner.hashCode
     */
  val sessionToWorker = new ConcurrentHashMap[String,SparkSession]()
  def startServer():Unit={
      Thread.currentThread().setName("mysql-protocol-server-acceptor")
    logInfo("starting mysql protocol server...")
    val port =  context.sparkContext.getConf.getInt("spark.mysql.protocol.port",5077)
    val maxThread =  context.sparkContext.getConf.getInt("spark.mysql.protocol.worker.max",200)
    val minThread =  context.sparkContext.getConf.getInt("spark.mysql.protocol.worker.min",6)
    val keepAlive =  context.sparkContext.getConf.getInt("spark.mysql.protocol.worker.keepAlive",10)
    val executorQueue = new SynchronousQueue[Runnable]
    val executorService:ExecutorService = new ThreadPoolExecutor(minThread,maxThread,keepAlive,TimeUnit.SECONDS,executorQueue)

    var server:ServerSocket = null
    try {
      server = new ServerSocket()
      val ia = new InetSocketAddress(port)
      server.bind(ia)
    } catch  {
      case e:Throwable =>
        logError("start server error.",e)
        return;
    }
    logInfo("bind port end.")

    while(true){
       var accept:Socket = null
      try {
        accept = server.accept()
        logInfo("one client connect to server. address:"+accept.getInetAddress.getHostName+" port:"+accept.getPort)
      } catch  {
        case e:Throwable =>
          logError("accept connection error.",e)
      }
      if(MysqlProtocolConstants.CONNECTION_ID.get>Int.MaxValue/2)MysqlProtocolConstants.CONNECTION_ID.set(10)
      val newSession = context.newSession()
      val wr = new WorkerRunner(accept,newSession,MysqlProtocolConstants.CONNECTION_ID.getAndIncrement(),this)
      sessionToWorker.put(wr.sessionKey,newSession)

      try{
        executorService.submit(wr)
      }catch {
        case _ =>
          sessionToWorker.remove(wr.sessionKey)
          wr.close()
      }

    }
  }
}
class WorkerRunner(val client: Socket, context: SparkSession,val connection_id:Int,server:MysqlProtocolServer) extends Logging with Runnable{
  import WorkerRunner._
  val sessionKey = connection_id+"-"+this.hashCode()
  private var seq:Byte = 0
  private val setCommandVariables = new JHashMap[String,String]()
  private var  currentSql:String = _
  private var  running = true
  private var  inIO:DataInputStream = _
  private var  outIO:DataOutputStream = _
  private var continueHandshake = true
  private var currentUser:String = _
  private var  password:String = _
  private var  currentDatabase:String = _
  private var clientCapability:Int = 0
  private var maxPacketSize:Int = 0


  override def run(): Unit = {
    Thread.currentThread().setName("mysql-protocol-worker-"+sessionKey)
    try{
      inIO = new DataInputStream(client.getInputStream)
      outIO = new DataOutputStream(client.getOutputStream)
    }catch{
      case ex:IOException=>
        try {
          client.close()
        } catch  {
          case e:IOException=>logError("close connection error.",e);
        }
        logError("get inIO and outIO error,exit now.",ex)
      return
    }

    logInfo("a client is connected:" + client.isConnected + " host:" + client.getInetAddress.getHostName + " port:" + client.getPort)
    handleHandshake(client)
    if (!continueHandshake) return

    while ( running) {
      try {
        val buffer = readPacket()
        handleCommand(buffer)
      } catch {
        case e: SocketException =>
          logError("encounter a socket Exception.", e)
          running=false
        case e:EOFException =>
          logError("encounter a socket Exception.", e)
          running=false
        case e: SQLException =>
          try{
            incrementSeqID()
            writeErrPacket(0x04, "0", "00004", e.getMessage, seq)
          }
        case e: IOException =>
          try {
            incrementSeqID()
            writeErrPacket(0x04, "0", "00004", e.getMessage, seq)
          }
          catch {
            case ex: Throwable =>
              logError("after error on loop read packet, try to write a ErrPacket. failed.", ex)
              incrementSeqID()
              writeErrPacket(0x04, "0", "00004", e.getMessage, seq)
          }
      }
    }
    server.sessionToWorker.remove(sessionKey)
    logInfo(s"WorkerRunner exit ,connection id:$connection_id")
    try{
      client.close()
    }catch {
      case _ =>
    }
  }

  @throws[IOException]
  @throws[SQLException]
  def handleCommand(buffer: Buffer): Unit = {
    val b = buffer.readByte
    val command = buffer.readRestString
    logDebug("COM_QUERY:" + b + "--:" + command)
    b match {
      //01 COM_QUIT 连接关闭
      case 0x01 =>
        this.running = false
      //COM_INIT_DB
      case 0x02 =>
        try{
          context.sql(USE+command.trim).collect()
          incrementSeqID()
          sendOkPacket("init database success!", 0, 0, 0x0002, -1, seq)
        }catch{
          case e =>
            incrementSeqID()
            writeErrPacket(0x04, "0", "00004", e.getMessage, seq)
        }
        //get the column definitions of a table
      case 0x04 =>
        incrementSeqID()
        this.writeColumnDefinePacket(defaultTableName, defaultTableName, defaultTableName, defaultTableName, "default", "default",
          seq, 0xfe, 0x1f)
        incrementSeqID()
        this.writeEOFPacket(seq, 0, 0x0002,clientCapability)
      //03 COM_QUERY special for us
      case 0x03 =>
        handleCommandQuery(command)
      //TODO 暂时只遇到 01 03，以后其他命令在完成下面的命令
      case _ =>
        incrementSeqID()
        writeErrPacket(1047, "0", "01047", "unsupport command", seq)
    }
  }
  def handleHandshake(client: Socket): Unit = {
    try {
      sendInitialHandShake(connection_id)
      val handshakeRespnse = readPacket()
      clientCapability = handshakeRespnse.readInt
      maxPacketSize = handshakeRespnse.readLongInt
      this.currentUser = handshakeRespnse.readString
      if((clientCapability & MysqlProtocolConstants.CLIENT_CONNECT_WITH_DB)!=0){
        this.password = handshakeRespnse.readString
        this.currentDatabase =handshakeRespnse.readString()
      }else{
        this.password = handshakeRespnse.readRestString
      }
      logInfo("user:" + currentUser + " password:" + password + "pgm" + " database:" + currentDatabase)
      //String restStr,int affectRows,int lastInsertId,int statusFlag,int warnings,byte seqid
      //use the user specify database
      if(currentDatabase!=null && !defaultSchema.equals(currentDatabase)){
          context.sql(USE+currentDatabase).collect()
      }
      incrementSeqID()
      sendOkPacket("authentication succeed!", 0, 0, 0x0002, -1, seq)
    } catch {
      case e: Throwable =>
        continueHandshake = false
        incrementSeqID()
        writeErrPacket(0x04, "0", "00004", e.getMessage, seq)
        logError("hand shake error,will close the conn.", e)
        try client.close()
        catch {
          case ex: IOException =>
            logError("close the conn error after encounter the problem.", ex)
        }
    }
  }

  @throws[SQLException]
  @throws[IOException]
  def sendInitialHandShake(connctionId: Int): Unit = {
    val buffer = new Buffer(20)
    //1              [0a] protocol version
    buffer.writeByte(MysqlProtocolConstants.PROTOCOL)
    // string[NUL]    server version
    buffer.writeString(MysqlProtocolConstants.SERVER_VERSION)
    // 4              connection id
    buffer.writeLong(connctionId)
    //  string[8]      auth-plugin-data-part-1
    buffer.writeBytesNoNull(MysqlProtocolConstants.AUTH_PLUGIN_DATA.getBytes, 0, 8)
    // 1              [00] filler
    buffer.writeByte(0x00)
    // 2              capability flags (lower 2 bytes)
    buffer.writeByte(0x08)
    buffer.writeByte(0x00)
    //  if more data in the packet:
    // 1              character set
    buffer.writeByte(255)
    //2              status flags
    buffer.writeInt(0x02)
    buffer.writeInt(0x00)
    // 2              capability flags (upper 2 bytes)
    buffer.writeInt(0x00)
    buffer.writeInt(0x00)
    // if capabilities & CLIENT_PLUGIN_AUTH {
    //     1              length of auth-plugin-data
    // } else {
    //    1              [00]
    buffer.writeByte(0x00)
    //}
    // string[10]     reserved (all [00])
    buffer.writeBytesNoNull(new Array[Byte](10))
    buffer.writeHeaderLength(buffer.getPosition - Buffer.HEADER_LENGTH)
    buffer.writeSeq(0x00)
    client.getOutputStream.write(buffer.getByteBuffer, 0, buffer.getPosition)
    client.getOutputStream.flush()
  }

  /**
   * @throws IOException IO
   * @return the packet read from client
   */
  @throws[IOException]
  def readPacket(): Buffer = {
    val b = new Array[Byte](3)
    inIO.readFully(b)
    val bodyLen = Buffer.getInt3(b)
    seq = inIO.readByte
    val command = new Array[Byte](bodyLen)
    inIO.readFully(command)
    val buffer = new Buffer(command)
    buffer
  }


  @throws[SQLException]
  @throws[IOException]
  def sendOkPacket(restStr: String, affectRows: Int, lastInsertId: Int, statusFlag: Int, warnings: Int, seqid: Byte): Unit = {
    //send ok packet
    val buffer = new Buffer(30)
    //int<1>	header	[00] or [fe] the OK packet header
    buffer.writeByte(OK_PACKET_HEADER)
    //int<lenenc>	affected_rows	affected rows
    buffer.writeInt(affectRows)
    // int<lenenc>	last_insert_id	last insert-id
    buffer.writeInt(lastInsertId)
    if((clientCapability & MysqlProtocolConstants.CLIENT_PROTOCOL_41) != 0 ){
      // int<2>	status_flags	Status Flags
      buffer.writeInt(statusFlag)
      //int<2>	warnings	number of warnings
      buffer.writeInt(warnings)
    }else{
      // int<2>	status_flags	Status Flags
      buffer.writeInt(statusFlag)
    }

    if((clientCapability & MysqlProtocolConstants.CLIENT_SESSION_TRACK) != 0 ){
      val defaultInfo = "default info!".getBytes
      buffer.writeFieldLength(defaultInfo.length)
      buffer.writeBytesNoNull(defaultInfo)
      if((statusFlag & MysqlProtocolConstants.SERVER_SESSION_STATE_CHANGED) != 0){
        val changeInfo ="track state change".getBytes()
        buffer.writeFieldLength(changeInfo.length+1)
        buffer.writeByte(MysqlProtocolConstants.SESSION_TRACK_STATE_CHANGE)
        buffer.writeBytesNoNull(changeInfo)
      }
    }else{
      // string<EOF>	info	human readable status information
      if (restStr != null) {
        val bs = restStr.getBytes
        buffer.writeFieldLength(bs.length)
        buffer.writeBytesNoNull(bs)
      }
    }

    buffer.writeHeaderLength(buffer.getPosition - Buffer.HEADER_LENGTH)
    buffer.writeSeq(seqid)
    outIO.write(buffer.getByteBuffer, 0, buffer.getPosition)
    outIO.flush()
  }


  /**
   *
   * @param errorCode     length 2
   * @param sqlStatMarker length 1 "0"
   * @param sqlState      length 5 "00001":未执行，"00002":执行中 ,"00003"：完成 ,"00004":执行出错 MysqlErrorNumbers.java
   * @param errorMsg      错误信息
   */
  @throws[SQLException]
  @throws[IOException]
  def writeErrPacket(errorCode: Int, sqlStatMarker: String, sqlState: String, errorMsg: String, seqid: Byte): Unit = {
    val buffer = new Buffer(16)
    buffer.writeByte(ERR_PACKET_HEADER)
    buffer.writeInt(errorCode)
    buffer.writeString(sqlStatMarker)
    buffer.writeString(sqlState)
    buffer.writeString(errorMsg)
    buffer.writeSeq(seqid)
    buffer.writeHeaderLength(buffer.getPosition - Buffer.HEADER_LENGTH)
    outIO.write(buffer.getByteBuffer, 0, buffer.getPosition)
    outIO.flush()
  }

  //下面是 select id,name from test; columndefine320
  @throws[IOException]
  @throws[SQLException]
  def writeColumnCountPacket(count: Int, seq: Byte): Unit = {
    val buffer = new Buffer(20)
    buffer.writeFieldLength(count)
    buffer.writeHeaderLength(buffer.getPosition - Buffer.HEADER_LENGTH)
    buffer.writeSeq(seq)
    outIO.write(buffer.getByteBuffer, 0, buffer.getPosition)
    outIO.flush()
  }

  // ColumnDefinition41
  /**
   *
   * @param name      列名
   * @param org_name  原始列名
   * @param table     表名
   * @param org_table 原始表名
   * @param catalog database catalog
   * @param schema  schema in the database
   * @param seq       packet 序列
   * @param tp
   *                  MYSQL_TYPE_DECIMAL		0x00
   *                  MYSQL_TYPE_TINY			0x01
   *                  MYSQL_TYPE_SHORT		0x02
   *                  MYSQL_TYPE_LONG			0x03
   *                  MYSQL_TYPE_FLOAT		0x04
   *                  MYSQL_TYPE_DOUBLE		0x05
   *                  MYSQL_TYPE_NULL			0x06
   *                  MYSQL_TYPE_TIMESTAMP	0x07
   *                  MYSQL_TYPE_LONGLONG		0x08
   *                  MYSQL_TYPE_INT24		0x09
   *                  MYSQL_TYPE_DATE			0x0a
   *                  MYSQL_TYPE_TIME			0x0b
   *                  MYSQL_TYPE_DATETIME		0x0c
   *                  MYSQL_TYPE_YEAR			0x0d
   *                  MYSQL_TYPE_NEWDATE [a]	0x0e
   *                  MYSQL_TYPE_VARCHAR		0x0f
   *                  MYSQL_TYPE_BIT			0x10
   *                  MYSQL_TYPE_TIMESTAMP2 [a]	0x11
   *                  MYSQL_TYPE_DATETIME2 [a]	0x12
   *                  MYSQL_TYPE_TIME2 [a]	0x13
   *                  MYSQL_TYPE_NEWDECIMAL	0xf6
   *                  MYSQL_TYPE_ENUM			0xf7
   *                  MYSQL_TYPE_SET			0xf8
   *                  MYSQL_TYPE_TINY_BLOB	0xf9
   *                  MYSQL_TYPE_MEDIUM_BLOB	0xfa
   *                  MYSQL_TYPE_LONG_BLOB	0xfb
   *                  MYSQL_TYPE_BLOB	0xfc
   *                  MYSQL_TYPE_VAR_STRING	0xfd
   *                  MYSQL_TYPE_STRING		0xfe
   *                  MYSQL_TYPE_GEOMETRY		0xff
   * @param decimals
   *                  0x00 for integers and static strings
   *                  0x1f for dynamic strings, double, float
   *                  0x00 to 0x51 for decimals
   */
  @throws[SQLException]
  @throws[IOException]
  def writeColumnDefinePacket(name: String, org_name: String, table: String, org_table: String, catalog: String, schema: String,
                              seq: Byte, tp: Int, decimals: Int): Unit = {
    //--
    var bs = name.getBytes
    val buffer = new Buffer(30)
    //lenenc_str     catalog
    //var bs = catalog.getBytes
    buffer.writeFieldLength(bs.length)
    buffer.writeBytesNoNull(bs)
    //lenenc_str     schema
    //bs = schema.getBytes
    buffer.writeFieldLength(bs.length)
    buffer.writeBytesNoNull(bs)
    //lenenc_str     table
    //bs = table.getBytes
    buffer.writeFieldLength(bs.length)
    buffer.writeBytesNoNull(bs)
    //lenenc_str     org_table
    //bs = name.getBytes
    buffer.writeFieldLength(bs.length)
    buffer.writeBytesNoNull(bs)
    //lenenc_str     name
    //bs = org_table.getBytes
    buffer.writeFieldLength(bs.length)
    buffer.writeBytesNoNull(bs)
    //lenenc_str     org_name
    //bs = org_name.getBytes
    buffer.writeFieldLength(bs.length)
    buffer.writeBytesNoNull(bs)
    // lenenc_int     length of fixed-length fields [0c]
    buffer.writeFieldLength(0x0c)
    // 2              character set
    buffer.writeInt(192)
    // 4              column length
    buffer.writeLong(1024 * 1024)
    // 1              type
    buffer.writeByte(tp)
    // 2              flags
    buffer.writeInt(0x03)
    // 1              decimals
    buffer.writeByte(0x1f)
    // 2              filler [00] [00]
    buffer.writeByte(0x00)
    buffer.writeByte(0x00)
    buffer.writeHeaderLength(buffer.getPosition - Buffer.HEADER_LENGTH)
    buffer.writeSeq(seq)
    outIO.write(buffer.getByteBuffer, 0, buffer.getPosition)
    outIO.flush()

    /**
     * if command was COM_FIELD_LIST {
     * lenenc_int     length of default-values
     * string[$len]   default values
     * }
     */
  }

  /**
   *
   * @param seq         序列号
   * @param warningRows CLIENT_PROTOCOL_41时需要改参数 ，我们使用5.7.x的版本 则客户端即可兼容CLIENT_PROTOCOL_41
   * @param statusFlag
   *                    SERVER_STATUS_IN_TRANS	0x0001	a transaction is active
   *                    SERVER_STATUS_AUTOCOMMIT	0x0002	auto-commit is enabled
   *                    SERVER_MORE_RESULTS_EXISTS	0x0008
   *                    SERVER_STATUS_NO_GOOD_INDEX_USED	0x0010
   *                    SERVER_STATUS_NO_INDEX_USED	0x0020
   *                    SERVER_STATUS_CURSOR_EXISTS	0x0040	Used by Binary Protocol Resultset to signal that COM_STMT_FETCH must
   *                                                        be used to fetch the row-data.
   *                    SERVER_STATUS_LAST_ROW_SENT	0x0080
   *                    SERVER_STATUS_DB_DROPPED	0x0100
   *                    SERVER_STATUS_NO_BACKSLASH_ESCAPES	0x0200
   *                    SERVER_STATUS_METADATA_CHANGED	0x0400
   *                    SERVER_QUERY_WAS_SLOW	0x0800
   *                    SERVER_PS_OUT_PARAMS	0x1000
   *                    SERVER_STATUS_IN_TRANS_READONLY	0x2000	in a read-only transaction
   *                    SERVER_SESSION_STATE_CHANGED	0x4000	connection state information has changed
   */
  @throws[IOException]
  @throws[SQLException]
  def writeEOFPacket(seq: Byte, warningRows: Int, statusFlag: Int,capability:Int): Unit = {
    val buffer = new Buffer(10)
    buffer.writeByte(0xfe)
    if((capability & MysqlProtocolConstants.CLIENT_PROTOCOL_41)!=0){
      buffer.writeInt(warningRows)
      buffer.writeInt(statusFlag)
    }
    buffer.writeHeaderLength(buffer.getPosition - Buffer.HEADER_LENGTH)
    buffer.writeSeq(seq)
    outIO.write(buffer.getByteBuffer, 0, buffer.getPosition)
    outIO.flush()
  }

  @throws[SQLException]
  @throws[IOException]
  def writeResultSetRow(values: JList[String], seq: Byte): Unit = {
    val buffer: Buffer = new Buffer(30)

    for (str <- values.asScala) {
      if (str != null) {
        val bs: Array[Byte] = str.getBytes
        buffer.writeFieldLength(bs.length)
        buffer.writeBytesNoNull(bs)
      }
      else buffer.writeByte(0xfb)
    }
    buffer.writeHeaderLength(buffer.getPosition - Buffer.HEADER_LENGTH)
    buffer.writeSeq(seq)
    outIO.write(buffer.getByteBuffer, 0, buffer.getPosition)
  }

  /**
   * 应答序列加 1
   */
  def incrementSeqID(): Unit ={
    seq = (seq+1).toByte
  }

  /**
   *
   * @param sql 传达的sql
   */
  @throws[IOException]
  @throws[SQLException]
  def handleCommandQuery(sql: String): Unit = {
    currentSql = sql
    //set variable value的response时 EOF
    val trimSql = sql.trim.toLowerCase
    if (trimSql.startsWith("set")){
      logInfo("process set command begin")
      val compile = Pattern.compile("([^ ]+)[ ]+([^ |^=]+)([ ]+|[ ]*?=[ ]*?)([^ |^=]+)")
      val matcher = compile.matcher(sql.trim)
      if (matcher.matches) {
        this.setCommandVariables.put(matcher.group(2), matcher.group(4))
        incrementSeqID()
        this.writeColumnCountPacket(1, seq)
        incrementSeqID()
        this.writeColumnDefinePacket("message", "message", "message", "message",
          "system", "system", seq, 0xfe, 0x1f)
        incrementSeqID()
        this.writeEOFPacket(seq, 0, 0x0002,clientCapability)
        this.writeResultSetRow(util.Arrays.asList("success"), seq)
        incrementSeqID()
        this.writeEOFPacket(seq, 0, 0x0002,clientCapability)
        logInfo("process set command end")
      }else{
        handleCommonStatement(sql)
      }

    } else if(trimSql.indexOf("select")>=0 && trimSql.lastIndexOf("@@")>trimSql.lastIndexOf("from")){
      logInfo("process select variables command begin")
      val variables = trimSql.split(",")
      val columns = new JArrayList[String]()
      val columnAs = new JHashMap[String,String]()
      for(vab <- variables){
        val patter1 = Pattern.compile("@@[^ ]+[ ]+as[ ]+[^ ]+")
        val patter2 = Pattern.compile("@@[^ ]+")

        var matcher = patter1.matcher(vab)
        if(matcher.find()){
          val group = matcher.group()
          val keyColumns = group.split("as")
          val key = keyColumns(0).trim
          columns.add(key)
          columnAs.put(key,keyColumns(1).trim)
        }else{
          matcher = patter2.matcher(vab)
          if(matcher.find()){
            val group = matcher.group().trim
            columns.add(group)
            columnAs.put(group,group.replaceAll("@@",""))
          }
        }

      }
      val tableName = "system"
      incrementSeqID()
      this.writeColumnCountPacket(columns.size, seq)
      for(org <- columns.asScala){
        incrementSeqID()
        this.writeColumnDefinePacket(columnAs.get(org), org, tableName, tableName, "default", "default",
          seq, 0xfe, 0x1f)
      }
      incrementSeqID()
      this.writeEOFPacket(seq, 0, 0x0002,clientCapability)
      incrementSeqID()
      writeResultSetRow(getValuesForKey(columns.asScala.map(org=>org.replaceAll("@@session[.]","")).asJava,
        version_variables_key_values), seq)
      incrementSeqID()
      this.writeEOFPacket(seq, 0, 0x0002,clientCapability)
      logInfo("process select variables command end")

    }else if(trimSql.indexOf("show variables")>=0){
      // jdbc lower than 5.1.40
      val patter = Pattern.compile("(variable_name[ ]*=[ ]*[^ ]+)")
      val matcher = patter.matcher(trimSql)
      var count = 0
      while(matcher.find()){
        count+=1
      }
      if(count>0){
        incrementSeqID()
        this.writeColumnCountPacket(2, seq)
        val tableName = "system"
        incrementSeqID()
        this.writeColumnDefinePacket("key", "key", tableName, tableName, "default",
          "default", seq, 0xfe, 0x1f)
        incrementSeqID()
        this.writeColumnDefinePacket("value", "value", tableName, tableName, "default",
          "default", seq, 0xfe, 0x1f)
        incrementSeqID()
        this.writeEOFPacket(seq, 0, 0x0002,clientCapability)
        for((key,value) <- version_variables_key_values.asScala){
          incrementSeqID()
          writeResultSetRow(util.Arrays.asList(key,value), seq)
        }
        incrementSeqID()
        this.writeEOFPacket(seq, 0, 0x0002,clientCapability)
      }else{
        handleCommonStatement(sql)
      }
    }else if(trimSql.indexOf("show collation")>=0){
      //Collation	Charset	Id	Default	Compiled	Sortlen
      incrementSeqID()
      this.writeColumnCountPacket(6, seq)
      incrementSeqID()
      this.writeColumnDefinePacket("Collation", "Collation", "system", "system",
        "default", "default", seq, 0xfe, 0x1f)
      incrementSeqID()
      this.writeColumnDefinePacket("Charset", "Charset", "system", "system",
        "default", "default", seq, 0xfe, 0x1f)
      incrementSeqID()
      this.writeColumnDefinePacket("Id", "Id", "system", "system", "default",
        "default", seq, 0xfe, 0x1f)
      incrementSeqID()
      this.writeColumnDefinePacket("Default", "Default", "system", "system",
        "default", "default", seq, 0xfe, 0x1f)
      incrementSeqID()
      this.writeColumnDefinePacket("Compiled", "Compiled", "system", "system",
        "default", "default", seq, 0xfe, 0x1f)
      incrementSeqID()
      this.writeColumnDefinePacket("Sortlen", "Sortlen", "system", "system",
        "default", "default", seq, 0xfe, 0x1f)
      incrementSeqID()
      this.writeEOFPacket(seq, 0, 0x0002,clientCapability)
      incrementSeqID()
      this.writeEOFPacket(seq, 0, 0x0002,clientCapability)

    }else if(trimSql.equals("select version()")){
      incrementSeqID()
      this.writeColumnCountPacket(1, seq)
      incrementSeqID()
      this.writeColumnDefinePacket("version", "version", "system", "system",
        "default", "default", seq, 0xfe, 0x1f)
      incrementSeqID()
      this.writeEOFPacket(seq, 0, 0x0002,clientCapability)
      incrementSeqID()
      this.writeResultSetRow(util.Arrays.asList(MysqlProtocolConstants.SERVER_VERSION), seq)
      incrementSeqID()
      this.writeEOFPacket(seq, 0, 0x0002,clientCapability)
      println("process command:" + sql + " type:" + 0x3)
    }else {
      handleCommonStatement(sql)
    }

  }


  /**
   *
   * @param sql:查询sql
   */
  @throws[IOException]
  @throws[SQLException]
  def handleCommonStatement( sql:String): Unit ={
    //select
    try{
      val statementId = SnowflakeIdWorker.getInstance().nextId().toString
      val sparkContext = context.sparkContext
      sparkContext.setJobGroup(statementId,sql)
      sparkContext.setCallSite(statementId)

      val dataset = context.sql(sql)

      val schema = dataset.schema
      val fields = schema.fields
      incrementSeqID()
      this.writeColumnCountPacket(fields.length, seq)
      logDebug("write column count packet . count:" + fields.length)

      for(field <- fields){
        incrementSeqID()
        this.writeColumnDefinePacket(field.name, field.name, defaultTableName, defaultTableName, defaultCatalog,
          defaultSchema, seq, convertType(field.dataType), 0x1f)
        logDebug("write columns define packet. field name:" + field.name)
      }
      incrementSeqID()
      this.writeEOFPacket(seq, 0, 0x0002,clientCapability)
      val newRow = new JArrayList[String](fields.length)
      val rows = dataset.collect()
      for (row <- rows) {

        logDebug("begin send one row." + row.toString)
        for(i <- fields.indices) {
          val column = row.get(i)
          if(column == null){
            logDebug("column is null value:" + column)
            newRow.add(column.asInstanceOf[Null])
          }else{
            newRow.add(column.toString)
          }
        }
        incrementSeqID()
        this.writeResultSetRow(newRow, seq)
        newRow.clear()
        logDebug("end send one row.")
      }
        incrementSeqID()
        this.writeEOFPacket(seq, 0, 0x0002,clientCapability)
    }catch{
        case sqlException:SQLException =>
          throw  sqlException
        case ioException:IOException =>
          throw ioException
        case ex =>
          incrementSeqID()
          writeErrPacket(0x04, "0", "00004", ex.getMessage, seq)
          logError(s"execute command :$sql error.",ex)
    }
  }

  def convertType(dataType: DataType): Int = {
    //TODO 可能数据类型需要更精细化的分类
    dataType match {
      case _: DecimalType =>
        /**DECIMAL*/
        0x00
      case _: ByteType =>
        /** TINY */
        0x01
      case _: ShortType =>
        /** SHORT */
        0x02
      case _: IntegerType =>
        /** LONG */
        0x03
      case _: FloatType =>
        /** FLOAT */
        0x04
      case _: DoubleType =>
        /** DOUBLE */
        0x05
      case _: NullType =>
        /** NULL */
        0x06
      case _: TimestampType =>
        /** TIMESTAMP */
        0x07
      case _: LongType =>
        /** LONGLONG */
        0x08
      case _: DateType =>
        /** DATE */
        0x0a
      case _: VarcharType =>
        /** VARCHAR */
        0x0f
      case _ =>
        // default string
        0xfe
      //case  IntegerType /**INT24*/: return		0x09;
      //            case  /**TIME*/	: return		0x0b;
      //            case  /**DATETIME*/	: return	0x0c;
      //            case  /**YEAR*/	: return		0x0d;
      //            case  /**NEWDATE*/ 	: return	0x0e;
      //case /**BIT*/	: return		0x10;
      //case  /**TIMESTAMP2*/ : return  0x11;
      //case  /**DATETIME2*/ : return   0x12;
      //case  /**TIME2*/ 	: return	0x13;
      //case  /**NEWDECIMAL*/: return	0xf6;
      //case  /**ENUM*/	: return		0xf7;
      //case  /**SET*/: return		0xf8;
      //case  /**TINY_BLOB*/: return	0xf9;
      //case  /**MEDIUM_BLOB*/: return	0xfa;
      //case  /**LONG_BLOB*/: return	0xfb;
      //case  /**BLOB*/	: return		0xfc;
      //case  /**VAR_STRING*/: return	0xfd;
      //case  ArrayType /**STRING*/	: return	0xfe;
      //case  /**GEOMETRY*/	: return	0xff;
    }
  }
  def getValuesForKey(keys: JList[String], values: JMap[String, String]): JList[String] = {
    val vacs = new JArrayList[String]
    for (key <- keys.asScala) {
      val s = values.get(key)
      vacs.add(s)
    }
    vacs
  }

  def close(): Unit ={
    running = false
    try{
      client.close()
    }catch{
      case _ =>
    }
  }
}

object WorkerRunner{
  val OK_PACKET_HEADER = 0x00
  val ERR_PACKET_HEADER = 0xff
  val EOF_PACKET_HEADER = 0xff
  val version_variables_key_values = new JHashMap[String, String]
  val version_variables_keys = new JArrayList[String]
  val USE ="USE "
  private def loadProperties():Unit ={
    try{
      val stream = Thread.currentThread().getContextClassLoader
        .getResourceAsStream("com/luxin/mysql/protocol/mysql_server_default.properties")
      val proper = new Properties()
      proper.load(stream)
      //proper.foreach(kv =>version_variables_key_values.put(kv._1,kv._2))
      proper.asScala.foreach(kv =>version_variables_key_values.put(kv._1,kv._2))
    }catch{
      case e :Exception=> e.printStackTrace()
    }
  }
  loadProperties()

  val defaultTableName = "default"
  val defaultCatalog = "default"
  val defaultSchema = "default"
}
