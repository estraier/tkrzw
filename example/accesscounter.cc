/*************************************************************************************************
 * Example of access counter using a lambda processor
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

#include "tkrzw_dbm_hash.h"
#include "tkrzw_str_util.h"

using namespace tkrzw;

int64_t CountUp(DBM* dbm, std::string id) {
  int64_t count = 0;
  std::string new_value;
  dbm->Process(id,
    [&](std::string_view key, std::string_view value) -> std::string_view {
      if (value.data() == DBM::RecordProcessor::NOOP.data()) {
        count = 1;
        return "1";
      }
      count = StrToInt(value) + 1;
      new_value = ToString(count);
      return new_value;
    }, true).OrDie();
  return count;
}

void MultiplyEach(DBM* dbm, double factor) {
  std::string new_value;
  dbm->ProcessEach(
    [&](std::string_view key, std::string_view value) -> std::string_view {
      if (value.data() == DBM::RecordProcessor::NOOP.data()) {
        return DBM::RecordProcessor::NOOP;
      }
      const int64_t count = static_cast<int64_t>(StrToInt(value) * factor);
      if (count < 1) {
        return DBM::RecordProcessor::REMOVE;
      }
      new_value = ToString(count);
      return new_value;
    }, true).OrDie();
}

int main(int argc, char** argv) {
  HashDBM dbm;
  dbm.Open("casket.tkh", true, File::OPEN_TRUNCATE).OrDie();
  std::cout << CountUp(&dbm, "apple") << std::endl;
  std::cout << CountUp(&dbm, "apple") << std::endl;
  std::cout << CountUp(&dbm, "banana") << std::endl;
  std::cout << CountUp(&dbm, "apple") << std::endl;
  std::cout << CountUp(&dbm, "banana") << std::endl;
  MultiplyEach(&dbm, 0.35);
  std::cout << "apple: " << dbm.GetSimple("apple", "*") << std::endl;
  std::cout << "banana: " << dbm.GetSimple("banana", "*") << std::endl;
  dbm.Close().OrDie();
  return 0;
}

// END OF FILE
