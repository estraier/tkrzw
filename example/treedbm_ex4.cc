/*************************************************************************************************
 * Example for key comparators of the tree database
 *
 * Copyright 2020 Google LLC
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 *************************************************************************************************/

#include "tkrzw_cmd_util.h"
#include "tkrzw_dbm_tree.h"
#include "tkrzw_str_util.h"

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Creates the database manager.
  TreeDBM dbm;

  // Opens a new database with the default key comparator (LexicalKeyComparator).
  Status status = dbm.Open("casket.tkt", true, File::OPEN_TRUNCATE).OrDie();

  // Sets records with the key being a big-endian binary of an integer.
  // e.g: "\x00\x00\x00\x00\x00\x00\x00\x31" -> "hop"
  dbm.Set(IntToStrBigEndian(1), "hop").OrDie();
  dbm.Set(IntToStrBigEndian(256), "step").OrDie();
  dbm.Set(IntToStrBigEndian(32), "jump").OrDie();

  // Gets records with the key being a big-endian binary of an integer.
  std::cout << dbm.GetSimple(IntToStrBigEndian(1)) << std::endl;
  std::cout << dbm.GetSimple(IntToStrBigEndian(256)) << std::endl;
  std::cout << dbm.GetSimple(IntToStrBigEndian(32)) << std::endl;

  // Lists up all records, restoring keys into integers.
  std::unique_ptr<DBM::Iterator> iter = dbm.MakeIterator();
  iter->First();
  std::string key, value;
  while (iter->Get(&key, &value) == Status::SUCCESS) {
    std::cout << StrToIntBigEndian(key) << ":" << value << std::endl;
    iter->Next();
  }

  // Closes the database.
  dbm.Close().OrDie();

  // Opens a new database with the decimal integer comparator.
  TreeDBM::TuningParameters params;
  params.key_comparator = DecimalKeyComparator;
  dbm.OpenAdvanced("casket.tkt", true, File::OPEN_TRUNCATE, params).OrDie();

  // Sets records with the key being a decimal string of an integer.
  // e.g: "1" -> "hop"
  dbm.Set(ToString(1), "hop").OrDie();
  dbm.Set(ToString(256), "step").OrDie();
  dbm.Set(ToString(32), "jump").OrDie();

  // Gets records with the key being a decimal string of an integer.
  std::cout << dbm.GetSimple(ToString(1)) << std::endl;
  std::cout << dbm.GetSimple(ToString(256)) << std::endl;
  std::cout << dbm.GetSimple(ToString(32)) << std::endl;

  // Lists up all records, restoring keys into integers.
  iter = dbm.MakeIterator();
  iter->First();
  while (iter->Get(&key, &value) == Status::SUCCESS) {
    std::cout << StrToInt(key) << ":" << value << std::endl;
    iter->Next();
  }

  // Closes the database.
  dbm.Close().OrDie();

  // Opens a new database with the decimal real number comparator.
  params.key_comparator = RealNumberKeyComparator;
  dbm.OpenAdvanced("casket.tkt", true, File::OPEN_TRUNCATE, params).OrDie();

  // Sets records with the key being a decimal string of a real number.
  // e.g: "1.5" -> "hop"
  dbm.Set(ToString(1.5), "hop").OrDie();
  dbm.Set(ToString(256.5), "step").OrDie();
  dbm.Set(ToString(32.5), "jump").OrDie();

  // Gets records with the key being a decimal string of a real number.
  std::cout << dbm.GetSimple(ToString(1.5)) << std::endl;
  std::cout << dbm.GetSimple(ToString(256.5)) << std::endl;
  std::cout << dbm.GetSimple(ToString(32.5)) << std::endl;

  // Lists up all records, restoring keys into floating-point numbers.
  iter = dbm.MakeIterator();
  iter->First();
  while (iter->Get(&key, &value) == Status::SUCCESS) {
    std::cout << StrToDouble(key) << ":" << value << std::endl;
    iter->Next();
  }

  // Closes the database.
  dbm.Close().OrDie();

  // Opens a new database with the big-endian signed integers comparator.
  params.key_comparator = SignedBigEndianKeyComparator;
  dbm.OpenAdvanced("casket.tkt", true, File::OPEN_TRUNCATE, params).OrDie();

  // Sets records with the key being a big-endian binary of a floating-point number.
  // e.g: "\x00\x00\x00\x00\x00\x00\x00\x31" -> "hop"
  dbm.Set(IntToStrBigEndian(-1), "hop").OrDie();
  dbm.Set(IntToStrBigEndian(-256), "step").OrDie();
  dbm.Set(IntToStrBigEndian(-32), "jump").OrDie();

  // Gets records with the key being a big-endian binary of a floating-point number.
  std::cout << dbm.GetSimple(IntToStrBigEndian(-1)) << std::endl;
  std::cout << dbm.GetSimple(IntToStrBigEndian(-256)) << std::endl;
  std::cout << dbm.GetSimple(IntToStrBigEndian(-32)) << std::endl;

  // Lists up all records, restoring keys into floating-point numbers.
  iter = dbm.MakeIterator();
  iter->First();
  while (iter->Get(&key, &value) == Status::SUCCESS) {
    std::cout << static_cast<int64_t>(StrToIntBigEndian(key)) << ":" << value << std::endl;
    iter->Next();
  }

  // Closes the database.
  dbm.Close().OrDie();

  // Opens a new database with the big-endian floating-point numbers comparator.
  params.key_comparator = FloatBigEndianKeyComparator;
  dbm.OpenAdvanced("casket.tkt", true, File::OPEN_TRUNCATE, params).OrDie();

  // Sets records with the key being a big-endian binary of a floating-point number.
  // e.g: "\x3F\xF8\x00\x00\x00\x00\x00\x00" -> "hop"
  dbm.Set(FloatToStrBigEndian(1.5), "hop").OrDie();
  dbm.Set(FloatToStrBigEndian(256.5), "step").OrDie();
  dbm.Set(FloatToStrBigEndian(32.5), "jump").OrDie();

  // Gets records with the key being a big-endian binary of a floating-point number.
  std::cout << dbm.GetSimple(FloatToStrBigEndian(1.5)) << std::endl;
  std::cout << dbm.GetSimple(FloatToStrBigEndian(256.5)) << std::endl;
  std::cout << dbm.GetSimple(FloatToStrBigEndian(32.5)) << std::endl;

  // Lists up all records, restoring keys into floating-point numbers.
  iter = dbm.MakeIterator();
  iter->First();
  while (iter->Get(&key, &value) == Status::SUCCESS) {
    std::cout << StrToFloatBigEndian(key) << ":" << value << std::endl;
    iter->Next();
  }

  // Closes the database.
  dbm.Close().OrDie();

  return 0;
}

// END OF FILE
