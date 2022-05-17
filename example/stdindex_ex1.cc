/*************************************************************************************************
 * Example for typical usage of the on-memory STL secondary index
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

// Defines an index by the division ID to member employee IDs.
typedef StdIndex<int64_t, int64_t> DivisionIndex;

// Structure for an employee.
// Actually, you should use a serious implementaton like Protocol Buffers.
struct Employee {
  int64_t employee_id = 0;
  std::string name;
  int64_t division_id = 0;
  std::string Serialize() const {
    return StrCat(employee_id, "\t", name, "\t", division_id);
  }
  void Deserialize(const std::string_view& serialized) {
    const std::vector<std::string> fields = StrSplit(serialized, "\t");
    if (fields.size() < 3) {
      Die("Deserialize failed");
    }
    employee_id = StrToInt(fields[0]);
    name = fields[1];
    division_id = StrToInt(fields[2]);
  }
  void Print() {
    std::cout << "employee_id=" << employee_id << " name=" << name
              << " division_id=" << division_id << std::endl;
  }
};

// Loads index records from the main database.
void LoadIndexRecords(DBM* dbm, DivisionIndex* division_index) {
  class IndexBuilder : public DBM::RecordProcessor {
   public:
    explicit IndexBuilder(DivisionIndex* division_index)
        : division_index_(division_index) {}
    // This is called for each existing record.
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      const int64_t key_num = StrToInt(key);
      Employee employee;
      employee.Deserialize(value);
      division_index_->Add(employee.division_id, key_num);
      return NOOP;
    }
   private:
    DivisionIndex* division_index_;
  } index_builder(division_index);
  dbm->ProcessEach(&index_builder, false).OrDie();
}

// Updates information of an employee.
// If the name is empty, the entry is removed.
void UpdateEmployee(const Employee& employee, DBM* dbm, DivisionIndex* division_index) {
  const std::string& primary_key = ToString(employee.employee_id);
  class Updater : public DBM::RecordProcessor {
   public:
    Updater(const Employee& employee, DivisionIndex* division_index)
        : employee_(employee), division_index_(division_index) {
    }
    // This is called if there is an existing record of the key.
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      const int64_t key_num = StrToInt(key);
      Employee old_record;
      old_record.Deserialize(value);
      // Removes an entry for the existing record.
      if (old_record.division_id > 0) {
        division_index_->Remove(old_record.division_id, key_num);
      }
      // If the new name is empty, removes the record.
      if (employee_.name.empty()) {
        return REMOVE;
      }
      // Adds an entry for the new record.
      if (employee_.division_id > 0) {
        division_index_->Add(employee_.division_id, key_num);
      }
      // Updates the record.
      new_value_ = employee_.Serialize();
      return new_value_;
    }
    // This is called if there is no existing record of the key.
    std::string_view ProcessEmpty(std::string_view key) override {
      const int64_t key_num = StrToInt(key);
      // If the new name is empty, nothing is done.
      if (employee_.name.empty()) {
        return NOOP;
      }
      // Adds an entry for the new record.
      if (employee_.division_id > 0) {
        division_index_->Add(employee_.division_id, key_num);
      }
      // Updates the record.
      new_value_ = employee_.Serialize();
      return new_value_;
    }
   private:
    const Employee& employee_;
    DivisionIndex* division_index_;
    std::string new_value_;
  } updater(employee, division_index);
  dbm->Process(primary_key, &updater, true).OrDie();
}

// Main routine.
int main(int argc, char** argv) {

  // Creates the database manager.
  HashDBM dbm;

  // Prepares an index for divisions and their members.
  DivisionIndex division_index;

  // Opens a new database,
  dbm.Open("casket.tkh", true, File::OPEN_TRUNCATE).OrDie();

  // After opening the database, indices should be loaded.
  // As the database is empty at this point, there's no effect here.
  LoadIndexRecords(&dbm, &division_index);

  // Registers employee records.
  const std::vector<Employee> employees = {
    {10001, "Anne", 301}, {10002, "Marilla", 301}, {10003, "Matthew", 301},
    {10004, "Diana", 401},
    {10005, "Gilbert", 501},
  };
  for (const Employee& employee : employees) {
    UpdateEmployee(employee, &dbm, &division_index);
  }

  // Closes the database.
  dbm.Close().OrDie();

  // Opens an existing database,
  dbm.Open("casket.tkh", true).OrDie();

  // After opening the database, indices should be loaded.
  // All existing records are reflected on the index.
  LoadIndexRecords(&dbm, &division_index);

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
    for (int64_t employee_id : division_index.GetValues(division_id)) {
      Employee employee;
      employee.Deserialize(dbm.GetSimple(ToString(employee_id)));
      employee.Print();
    }
  }

  // Closes the database.
  dbm.Close().OrDie();

  return 0;
}

// END OF FILE
