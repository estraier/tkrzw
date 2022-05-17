/*************************************************************************************************
 * Example for typical usage of the file secondary index
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
#include "tkrzw_dbm_hash.h"
#include "tkrzw_index.h"
#include "tkrzw_str_util.h"

// All symbols of Tkrzw are under the namespace "tkrzw".
using namespace tkrzw;

// Structure for an employee.
// Actually, you should use a serious implementaton like Protocol Buffers.
struct Employee {
  int64_t employee_id = 0;
  std::string name;
  int64_t division_id = 0;
  std::string Serialize() const {
    return StrCat(employee_id, "\t", name, "\t", division_id);
  }
  bool Deserialize(const std::string_view& serialized) {
    const std::vector<std::string> fields = StrSplit(serialized, "\t");
    if (fields.size() < 3) {
      Die("Deserialize failed");
    }
    employee_id = StrToInt(fields[0]);
    name = fields[1];
    division_id = StrToInt(fields[2]);
    return true;
  }
  void Print() {
    std::cout << "employee_id=" << employee_id << " name=" << name
              << " division_id=" << division_id << std::endl;
  }
};

// Updates information of an employee.
// If the name is empty, the entry is removed.
void UpdateEmployee(const Employee& employee, DBM* dbm, FileIndex* division_index) {
  const std::string& primary_key = ToString(employee.employee_id);
  class Updater : public DBM::RecordProcessor {
   public:
    Updater(const Employee& employee, FileIndex* division_index)
        : employee_(employee), division_index_(division_index) {
    }
    // This is called if there is an existing record of the key.
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      Employee old_record;
      old_record.Deserialize(value);
      // Removes an entry for the existing record.
      if (old_record.division_id > 0) {
        division_index_->Remove(ToString(old_record.division_id), key);
      }
      // If the new name is empty, removes the record.
      if (employee_.name.empty()) {
        return REMOVE;
      }
      // Adds an entry for the new record.
      if (employee_.division_id > 0) {
        division_index_->Add(ToString(employee_.division_id), key);
      }
      // Updates the record.
      new_value_ = employee_.Serialize();
      return new_value_;
    }
    // This is called if there is no existing record of the key.
    std::string_view ProcessEmpty(std::string_view key) override {
      // If the new name is empty, nothing is done.
      if (employee_.name.empty()) {
        return NOOP;
      }
      // Adds an entry for the new record.
      if (employee_.division_id > 0) {
        division_index_->Add(ToString(employee_.division_id), key);
      }
      // Updates the record.
      new_value_ = employee_.Serialize();
      return new_value_;
    }
   private:
    const Employee& employee_;
    FileIndex* division_index_;
    std::string new_value_;
  } updater(employee, division_index);
  dbm->Process(primary_key, &updater, true).OrDie();
}

// Main routine.
int main(int argc, char** argv) {

  // Creates the database manager.
  HashDBM dbm;

  // Prepares an index for divisions and their members.
  FileIndex division_index;

  // Opens a new database,
  dbm.Open("casket.tkh", true, File::OPEN_TRUNCATE).OrDie();

  // Opens a new index.
  // Setting PairDecimalKeyComparator is to sort the division ID in numeric order.
  // Otherwise, "10" would come earlier than "2" in lexical order.
  TreeDBM::TuningParameters division_index_params;
  division_index_params.key_comparator = PairDecimalKeyComparator;
  Status status = division_index.Open(
      "casket-division-index.tkt", true, File::OPEN_TRUNCATE, division_index_params);

  // Registers employee records.
  const std::vector<Employee> employees = {
    {10001, "Anne", 301}, {10002, "Marilla", 301}, {10003, "Matthew", 301},
    {10004, "Diana", 401},
    {10005, "Gilbert", 501},
  };
  for (const Employee& employee : employees) {
    UpdateEmployee(employee, &dbm, &division_index);
  }

  // Closes the index.
  division_index.Close().OrDie();
  
  // Closes the database.
  dbm.Close().OrDie();

  // Opens an existing database,
  dbm.Open("casket.tkh", true).OrDie();

  // Opens an existing index,
  division_index.Open("casket-division-index.tkt", true).OrDie();

  // Updates for employees.
  const std::vector<Employee> updates = {
    {10001, "Anne", 501},  // Anne works with Gilbert.
    {10003, "", 0},        // Matthew left the company.
    {10006, "Minnie May", 401},
    {10007, "Davy", 301}, {10008, "Dora", 301},
  };
  for (const Employee& employee : updates) {
    UpdateEmployee(employee, &dbm, &division_index);
  }

  // Prints members for each division.
  for (const int64_t division_id : {301, 401, 501}) {
    std::cout << "-- Division " << division_id << "  --" << std::endl;
    for (const std::string& employee_id :
             division_index.GetValues(ToString(division_id))) {
      Employee employee;
      employee.Deserialize(dbm.GetSimple(employee_id));
      employee.Print();
    }
  }

  // Closes the index.
  division_index.Close().OrDie();

  // Closes the database.
  dbm.Close().OrDie();

  return 0;
}

// END OF FILE
