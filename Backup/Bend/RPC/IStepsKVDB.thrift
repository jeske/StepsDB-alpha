

namespace java org.steps.stepsdb.thrift
namespace csharp Bend.Stepsdb.Thrift

struct RecordKey {
  1: required list<RecordKeyPart> key_parts
}

enum RecordKeyPartType {
  I64 = 1,
  STRING = 2,
  BINARY = 3,
  RECORDKEY = 4,
  MINKEY = 5,
  MAXKEY = 6,
  ANYKEY = 7
}

struct RecordKeyPart {
  1: required RecordKeyPartType value_type,
  2: optional i64 value_i64,
  3: optional string value_string,
  4: optional binary value_binary,
  5: optional RecordKey value_recordkey
}

enum RecordUpdateType {
  DELETION_TOMBSTONE = 0,
  FULL = 1
}

struct RecordUpdate {
  1: required RecordUpdateType update_type,
  2: optional binary update_data
}

enum GetValueResult {
  PRESENT = 0,
  MISSING = 1
}

struct Record {
  1: required RecordKey key,
  2: required RecordUpdate data
}

struct GetValueResult {
  1: required GetValueResult result;
  2: optional Record record;
}

struct RecordList {
  1: required list<Record> list;
}

struct Cursor {
  1: required binary opaque_cursor_id
}

struct ScanResult {
  1: required Cursor cursor,
  2: required RecordList records;
}

exception InvalidRequestException {
  1: required string error_reason
}

exception TimedOutException {
}

exception AuthorizationException {
    1: required string error_reason
}


struct DBNSConfig {    
    2: optional double memcache_target_ratio,    
    3: optional double compaction_priority,	
	4: optional string comment
}

struct DBNSStats {    
    2: required double qps_read,
	3: required double qps_write,
	4: required double throughput_mb_read,
	5: required double throughput_mb_write,
	6: required double memcache_size_mb,
}

struct DBNSInfo {
    1: required DBNSConfig,
	2: required DBNSStats	
}

service IStepsKVDB {
  // admin-related

  // void login()
  
  // DB Namspace (like a Column Family)

  // listDbNamespaces()
  // useDbNamespace(1: RecordKey key)
  // createDbNamespace(1: RecordKey key)
  // configDbNamespace(1: RecordKey key, 2: DBNSConfig config)
  // DBNSInfo getDbNamespaceInfo(
  
  // single record modification
  void setValue(1: RecordKey key, 2: RecordUpdate data),
  void deleteValue(1: RecordKey key),
  
  // multi-record modification
  void setValueMulti(1: list<Record> records),
  void deleteMulti(1: list<RecordKey>),

  // single row exact-key fetch
  RecordResult getValue(1: RecordKey ),  

  // multi-row cursor scan operations
  ScanResult scanForward(1: RecordKey low_key, 2: RecordKey high_key,3: i32 count),
  ScanResult scanBackward(1: RecordKey low_key, 2: RecordKey high_key,3: i32 count),
  RecordList continueScan(1: Cursor cursor, 2:i32 count)
  void disposeCursor(1: Cursor cursor)  
}
