/**

@mainpage Tkrzw: a set of implementations of DBM

@section Introduction

DBM (Database Manager) is a concept of libraries to store an associative array on a permanent storage.  In other words, DBM allows an application program to store key-value pairs in a file and reuse them later.  Each of keys and values is a string or a sequence of bytes.  The key of each record must be unique within the database and a value is associated to it.  You can retrieve a stored record with its key very quickly.  Thanks to simple structure of DBM, its performance can be extremely high.

Tkrzw is a C++ library implementing DBM with various algorithms.  It features high degrees of performance, concurrency, scalability and durability.  The following classes are the most important.

@li tkrzw::DBM -- Database manager interface.
@li tkrzw::HashDBM -- File database manager implementation based on hash table.
@li tkrzw::TreeDBM -- File database manager implementation based on B+ tree.
@li tkrzw::SkipDBM -- File database manager implementation based on skip list.
@li tkrzw::TinyDBM -- On-memory database manager implementation based on hash table.
@li tkrzw::BabyDBM -- On-memory database manager implementation based on B+ tree.
@li tkrzw::CacheDBM -- On-memory database manager implementations with LRU deletion.
@li tkrzw::StdHashDBM -- On-memory hash database manager implementation using std::unordered_map.
@li tkrzw::StdTreeDBM -- On-memory tree database manager implementation using std::map.
@li tkrzw::PolyDBM -- Polymorphic database manager adapter for all DBM classes.
@li tkrzw::ShardDBM -- Sharding database manager adapter based on PolyDBM.
@li tkrzw::AsyncDBM -- Asynchronous database manager adapter for any databases.
@li tkrzw::FileIndex -- File secondary index implementation with TreeDBM.
@li tkrzw::MemIndex -- On-memory secondary index implementation with BabyDBM.
@li tkrzw::StdIndex -- On-memory secondary index implementation with std::map.
@li tkrzw_langc.h -- The C interface of PolyDBM and ShardDBM.

All database classes share the same interface so that applications can use any of them with the common API.  All classes are thread-safe so that multiple threads can access the same database simultaneously.  Basically, you can store records with the "Set" method, retrieve records with the "Get" method, and remove records with the "Remove" method.  Iterator is also supported to retrieve each and every record in the database.  See the <a href="http://dbmx.net/tkrzw/">homepage</a> for details.

@code
#include "tkrzw_dbm_hash.h"

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Creates the database manager.
  HashDBM dbm;

  // Opens a new database.
  dbm.Open("casket.tkh", true);

  // Stores records.
  dbm.Set("foo", "hop");
  dbm.Set("bar", "step");
  dbm.Set("baz", "jump");

  // Retrieves records.
  std::cout << dbm.GetSimple("foo", "*") << std::endl;
  std::cout << dbm.GetSimple("bar", "*") << std::endl;
  std::cout << dbm.GetSimple("baz", "*") << std::endl;
  std::cout << dbm.GetSimple("outlier", "*") << std::endl;

  // Traverses records.
  std::unique_ptr<DBM::Iterator> iter = dbm.MakeIterator();
  iter->First();
  std::string key, value;
  while (iter->Get(&key, &value) == Status::SUCCESS) {
    std::cout << key << ":" << value << std::endl;
    iter->Next();
  }

  // Closes the database.
  dbm.Close();

  return 0;
}
@endcode

*/

/**
 * Common namespace of Tkrzw.
 */
namespace tkrzw {}

/**
 * @file tkrzw_lib_common.h Common library features.
 * @file tkrzw_str_util.h String utilities.
 * @file tkrzw_hash_util.h Hash utilities.
 * @file tkrzw_time_util.h Time utilities.
 * @file tkrzw_thread_util.h Threading utilities.
 * @file tkrzw_logger.h Logger interface and implementations.
 * @file tkrzw_compress.h Data compression functions.
 * @file tkrzw_containers.h Miscellaneous data containers.
 * @file tkrzw_key_comparators.h Built-in comparators for record keys.
 * @file tkrzw_file_util.h File system utilities.
 * @file tkrzw_file.h File interface.
 * @file tkrzw_file_std.h File implementations by the C++ standard file stream.
 * @file tkrzw_file_mmap.h File implementations by memory mapping.
 * @file tkrzw_file_pos.h File implementations by positional access.
 * @file tkrzw_file_poly.h Polymorphic file adapter.
 * @file tkrzw_message_queue.h Message queue on the file stream.
 * @file tkrzw_dbm.h Database manager interface.
 * @file tkrzw_dbm_ulog.h DBM update logger implementations.
 * @file tkrzw_dbm_common_impl.h Common implementation components for database managers.
 * @file tkrzw_dbm_hash_impl.h Implementation components for the hash database manager.
 * @file tkrzw_dbm_hash.h File database manager implementation based on hash table.
 * @file tkrzw_dbm_tree_impl.h Implementation components for the tree database manager.
 * @file tkrzw_dbm_tree.h File database manager implementation based on B+ tree.
 * @file tkrzw_dbm_skip_impl.h Implementation components for the skip database manager.
 * @file tkrzw_dbm_skip.h File database manager implementation based on skip list.
 * @file tkrzw_dbm_tiny.h On-memory database manager implementations based on hash table.
 * @file tkrzw_dbm_baby.h On-memory database manager implementations based on B+ tree.
 * @file tkrzw_dbm_cache.h On-memory database manager implementations with LRU deletion.
 * @file tkrzw_dbm_std.h On-memory database manager implementations with the C++ standard containers.
 * @file tkrzw_dbm_poly.h Polymorphic database manager adapter.
 * @file tkrzw_dbm_shard.h Sharding database manager adapter.
 * @file tkrzw_dbm_async.h Asynchronous database manager adapter.
 * @file tkrzw_index.h Secondary index implementations.
 * @file tkrzw_cmd_util.h Command-line utilities.
 * @file tkrzw_langc.h C language binding of Tkrzw.
 */

// END OF FILE
