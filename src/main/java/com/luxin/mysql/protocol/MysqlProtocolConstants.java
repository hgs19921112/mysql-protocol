package com.luxin.mysql.protocol;

import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

public class MysqlProtocolConstants {
    public static final AtomicInteger CONNECTION_ID = new AtomicInteger(10);
    public static final byte PROTOCOL = 0x0a;
    public static final String SERVER_VERSION = "5.7.17";
    public static final String AUTH_PLUGIN_DATA = "mysql_old_password";
    public static final int CAPABILITY_FLAGS = 00;
    public static final String CHARACTER_SET = System.getProperty("file.encoding");
    public static final String TIME_ZONE = TimeZone.getDefault().getDisplayName();
    //data type
    public static final int MYSQL_TYPE_DECIMAL = 0x00;
    public static final int MYSQL_TYPE_TINY = 0x01;
    public static final int MYSQL_TYPE_SHORT = 0x02;
    public static final int MYSQL_TYPE_LONG = 0x03;
    public static final int MYSQL_TYPE_FLOAT = 0x04;
    public static final int MYSQL_TYPE_DOUBLE = 0x05;
    public static final int MYSQL_TYPE_NULL = 0x06;
    public static final int MYSQL_TYPE_TIMESTAMP = 0x07;
    public static final int MYSQL_TYPE_LONGLONG = 0x08;
    public static final int MYSQL_TYPE_INT24 = 0x09;
    public static final int MYSQL_TYPE_DATE = 0x0a;
    public static final int MYSQL_TYPE_TIME = 0x0b;
    public static final int MYSQL_TYPE_DATETIME = 0x0c;
    public static final int MYSQL_TYPE_YEAR = 0x0d;
    public static final int MYSQL_TYPE_NEWDATE = 0x0e;
    public static final int MYSQL_TYPE_VARCHAR = 0x0f;
    public static final int MYSQL_TYPE_BIT = 0x10;
    public static final int MYSQL_TYPE_TIMESTAMP2 = 0x11;
    public static final int MYSQL_TYPE_DATETIME2 = 0x12;
    public static final int MYSQL_TYPE_TIME2 = 0x13;
    public static final int MYSQL_TYPE_NEWDECIMAL = 0xf6;
    public static final int MYSQL_TYPE_ENUM = 0xf7;
    public static final int MYSQL_TYPE_SET = 0xf8;
    public static final int MYSQL_TYPE_TINY_BLOB = 0xf9;
    public static final int MYSQL_TYPE_MEDIUM_BLOB = 0xfa;
    public static final int MYSQL_TYPE_LONG_BLOB = 0xfb;
    public static final int MYSQL_TYPE_BLOB = 0xfc;
    public static final int MYSQL_TYPE_VAR_STRING = 0xfd;
    public static final int MYSQL_TYPE_STRING = 0xfe;
    public static final int MYSQL_TYPE_GEOMETRY = 0xff;

    //capability
    public static final int CLIENT_LONG_PASSWORD = 0x00000001; /* new more secure passwords */
    public static final int CLIENT_FOUND_ROWS = 0x00000002;
    public static final int CLIENT_LONG_FLAG = 0x00000004; /* Get all column flags */
    public static final int CLIENT_CONNECT_WITH_DB = 0x00000008;
    public static final int CLIENT_COMPRESS = 0x00000020; /* Can use compression protcol */
    public static final int CLIENT_LOCAL_FILES = 0x00000080; /* Can use LOAD DATA LOCAL */
    public static final int CLIENT_PROTOCOL_41 = 0x00000200; // for > 4.1.1
    public static final int CLIENT_INTERACTIVE = 0x00000400;
    public static final int CLIENT_SSL = 0x00000800;
    public static final int CLIENT_TRANSACTIONS = 0x00002000; // Client knows about transactions
    public static final int CLIENT_RESERVED = 0x00004000; // for 4.1.0 only
    public static final int CLIENT_SECURE_CONNECTION = 0x00008000;
    public static final int CLIENT_MULTI_STATEMENTS = 0x00010000; // Enable/disable multiquery support
    public static final int CLIENT_MULTI_RESULTS = 0x00020000; // Enable/disable multi-results
    public static final int CLIENT_PLUGIN_AUTH = 0x00080000;
    public static final int CLIENT_CONNECT_ATTRS = 0x00100000;
    public static final int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;
    public static final int CLIENT_CAN_HANDLE_EXPIRED_PASSWORD = 0x00400000;
    public static final int CLIENT_SESSION_TRACK = 0x00800000;
    public static final int CLIENT_DEPRECATE_EOF = 0x01000000;

    // status flags
    public static final int SERVER_STATUS_IN_TRANS= 0x0001;//	a transaction is active
    public static final int SERVER_STATUS_AUTOCOMMIT = 0x0002;//	auto-commit is enabled
    public static final int SERVER_MORE_RESULTS_EXISTS = 0x0008;
    public static final int SERVER_STATUS_NO_GOOD_INDEX_USED = 0x0010;
    public static final int SERVER_STATUS_NO_INDEX_USED = 0x0020;
    public static final int SERVER_STATUS_CURSOR_EXISTS = 0x0040;//	Used by Binary Protocol Resultset to signal that COM_STMT_FETCH must be used to fetch the row-data.
    public static final int SERVER_STATUS_LAST_ROW_SENT = 0x0080;
    public static final int SERVER_STATUS_DB_DROPPED = 0x0100;
    public static final int SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200;
    public static final int SERVER_STATUS_METADATA_CHANGED = 0x0400;
    public static final int SERVER_QUERY_WAS_SLOW = 0x0800;
    public static final int SERVER_PS_OUT_PARAMS = 0x1000;
    public static final int SERVER_STATUS_IN_TRANS_READONLY	= 0x2000;//	in a read-only transaction
    public static final int SERVER_SESSION_STATE_CHANGED = 0x4000;//	connection state information has changed
    // state change data type
    public static final int SESSION_TRACK_SYSTEM_VARIABLES = 0x00;//	one or more system variables changed. See also: session_track_system_variables
    public static final int SESSION_TRACK_SCHEMA = 0x01;//	schema changed. See also: session_track_schema
    public static final int SESSION_TRACK_STATE_CHANGE = 0x02;//	"track state change" changed. See also: session_track_state_change
    public static final int SESSION_TRACK_GTIDS = 0x03;//	"track GTIDs" changed. See also: session_track_gtids
}