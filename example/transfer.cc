/*************************************************************************************************
 * Example of money transfer using lambda processors
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

Status Transfer(DBM* dbm, std::string_view src_key, std::string_view dest_key,
                int64_t amount) {
  Status op_status(Status::SUCCESS);

  // Callback to check the destination account.
  auto check_dest =
      [&](std::string_view key, std::string_view value) -> std::string_view {
        if (value.data() == DBM::RecordProcessor::NOOP.data()) {
          op_status.Set(Status::NOT_FOUND_ERROR, "no such destination account");
        }
        return DBM::RecordProcessor::NOOP;
      };

  // Callback to operate the source account.
  std::string new_src_value;
  auto proc_src =
      [&](std::string_view key, std::string_view value) -> std::string_view {
        if (!op_status.IsOK()) {
          return DBM::RecordProcessor::NOOP;
        }
        if (value.data() == DBM::RecordProcessor::NOOP.data()) {
          op_status.Set(Status::NOT_FOUND_ERROR, "no such source account");
          return DBM::RecordProcessor::NOOP;
        }
        const int64_t current_balance = StrToInt(value);
        if (current_balance < amount) {
          op_status.Set(Status::INFEASIBLE_ERROR, "insufficient balance");
          return DBM::RecordProcessor::NOOP;
        }
        new_src_value = ToString(current_balance - amount);
        return new_src_value;
      };

  // Callback to operate the destination account.
  std::string new_dest_value;
  auto proc_dest =
      [&](std::string_view key, std::string_view value) -> std::string_view {
        if (!op_status.IsOK()) {
          return DBM::RecordProcessor::NOOP;
        }
        if (value.data() == DBM::RecordProcessor::NOOP.data()) {
          op_status.Set(Status::APPLICATION_ERROR, "inconsistent transaction");
          return DBM::RecordProcessor::NOOP;
        }
        const int64_t current_balance = StrToInt(value);
        new_dest_value = ToString(current_balance + amount);
        return new_dest_value;
      };

  // Invokes the three callbacks in sequence atomically.
  const Status status = dbm->ProcessMulti({
      {dest_key, check_dest},
      {src_key, proc_src},
      {dest_key, proc_dest}}, true);
  if (!status.IsOK()) {
    return status;
  }
  return op_status;
}

Status TransferByCompareExchange(
    DBM* dbm, std::string_view src_key, std::string_view dest_key, int64_t amount) {
  // Repeats the transaction until success or failure is determined.
  constexpr int32_t max_tries = 100;
  for (int32_t num_tries = 0; num_tries < max_tries; num_tries++) {

    // Gets the balance of the source account.
    std::string src_value;
    Status status = dbm->Get(src_key, &src_value);
    if (!status.IsOK()) {
      return status;
    }
    const int64_t src_balance = StrToInt(src_value);
    if (src_balance < amount) {
      return Status(Status::INFEASIBLE_ERROR, "insufficient balance");
    }

    // Gets the balance of the destination account.
    std::string dest_value;
    status = dbm->Get(dest_key, &dest_value);
    if (!status.IsOK()) {
      return status;
    }
    const int64_t dest_balance = StrToInt(dest_value);

    // Generate new values for the new balances.
    const std::string src_new_value = ToString(src_balance - amount);
    const std::string dest_new_value = ToString(dest_balance + amount);
    
    // Finish the transaction atomically if the balances are not modified.
    const std::vector<std::pair<std::string_view, std::string_view>> expected =
        {{src_key, src_value}, {dest_key, dest_value}};
    const std::vector<std::pair<std::string_view, std::string_view>> desired =
        {{src_key, src_new_value}, {dest_key, dest_new_value}};
    status = dbm->CompareExchangeMulti(expected, desired);
    if (status.IsOK()) {
      return Status(Status::SUCCESS);
    }
    if (status != Status::INFEASIBLE_ERROR) {
      return status;
    }
  }

  return Status(Status::INFEASIBLE_ERROR, "too busy");
}

int main(int argc, char** argv) {
  HashDBM dbm;
  dbm.Open("casket.tkh", true, File::OPEN_TRUNCATE).OrDie();
  dbm.Set("A", "10000", File::OPEN_TRUNCATE).OrDie();
  dbm.Set("B", "5000", File::OPEN_TRUNCATE).OrDie();
  std::cout << "A > B by 1000 : " << Transfer(&dbm, "A", "B", 1000) << std::endl;
  std::cout << "A > B by 10000 : " << Transfer(&dbm, "A", "B", 10000) << std::endl;
  std::cout << "B > C by 1000 : " << Transfer(&dbm, "A", "C", 1000) << std::endl;
  std::cout << "B > A by 4000 : " << Transfer(&dbm, "B", "A", 4000) << std::endl;
  std::cout << "A:" << dbm.GetSimple("A", "*") << std::endl;
  std::cout << "B:" << dbm.GetSimple("B", "*") << std::endl;
  std::cout << "A > B by 10000 : "
            << TransferByCompareExchange(&dbm, "A", "B", 10000) << std::endl;
  std::cout << "A > B by 5000 : "
            << TransferByCompareExchange(&dbm, "A", "B", 5000) << std::endl;
  std::cout << "A > C by 1000 : "
            << TransferByCompareExchange(&dbm, "A", "C", 1000) << std::endl;
  std::cout << "B > A by 2000 : "
            << TransferByCompareExchange(&dbm, "B", "A", 2000) << std::endl;
  std::cout << "A:" << dbm.GetSimple("A", "*") << std::endl;
  std::cout << "B:" << dbm.GetSimple("B", "*") << std::endl;
  dbm.Close().OrDie();
  return 0;
}

// END OF FILE
