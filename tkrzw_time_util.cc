/*************************************************************************************************
 * Time utilities
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

#include "tkrzw_sys_config.h"

#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_time_util.h"

namespace tkrzw {

double GetWallTime() {
  const auto epoch = std::chrono::time_point<std::chrono::system_clock>();
  const auto current = std::chrono::system_clock::now();
  const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(current - epoch);
  return elapsed.count() / 1000000.0;
}

void GetLocalCalendar(int64_t wtime, struct std::tm* cal) {
#if defined(_SYS_WINDOWS_)
  assert(cal != nullptr);
  time_t twtime = wtime;
  if (localtime_s(cal, &twtime) != 0) {
    throw std::runtime_error("localtime_s failed");
  }
#else
  assert(cal != nullptr);
  time_t twtime = wtime;
  if (localtime_r(&twtime, cal) == nullptr) {
    throw std::runtime_error("localtime_r failed");
  }
#endif
}

void GetUniversalCalendar(int64_t wtime, struct std::tm* cal) {
#if defined(_SYS_WINDOWS_)
  assert(cal != nullptr);
  time_t twtime = wtime;
  if (gmtime_s(cal, &twtime) != 0) {
    throw std::runtime_error("gmtime_s failed");
  }
#else
  assert(cal != nullptr);
  time_t twtime = wtime;
  if (gmtime_r(&twtime, cal) == nullptr) {
    throw std::runtime_error("gmtime_r failed");
  }
#endif
}

int64_t MakeUniversalTime(struct std::tm& cal) {
#if defined(_SYS_LINUX_)
  return timegm(&cal);
#else
  return std::mktime(&cal) + GetLocalTimeDifference(true);
#endif
}

int32_t GetLocalTimeDifference(bool use_cache) {
#if defined(_SYS_LINUX_) || defined(_SYS_MACOSX_)
  static std::atomic_int32_t tz_cache(INT32MIN);
  int32_t tz_value = use_cache ? tz_cache.load() : INT32MIN;
  if (tz_value == INT32MIN) {
    tzset();
    tz_cache.store(-timezone);
    tz_value = tz_cache.load();
  }
  return tz_value;
#else
  static std::atomic_int32_t tz_cache(INT32MIN);
  int32_t tz_value = use_cache ? tz_cache.load() : INT32MIN;
  if (tz_value == INT32MIN) {
    const time_t t = 86400;
    struct std::tm uts;
    GetUniversalCalendar(t, &uts);
    struct std::tm lts;
    GetLocalCalendar(t, &lts);
    tz_cache.store(std::mktime(&lts) - std::mktime(&uts));
    tz_value = tz_cache.load();
  }
  return tz_value;
#endif
}

int32_t GetDayOfWeek(int32_t year, int32_t mon, int32_t day) {
  if (mon < 3) {
    year--;
    mon += 12;
  }
  return (day + ((8 + (13 * mon)) / 5) + (year + (year / 4) - (year / 100) + (year / 400))) % 7;
}

size_t FormatDateSimple(char* result, int64_t wtime, int32_t td) {
  assert(result != nullptr);
  if (wtime == INT64MIN) {
    wtime = GetWallTime();
  }
  if (td == INT32MIN) {
    td = GetLocalTimeDifference(true);
  }
  struct std::tm uts;
  GetUniversalCalendar(static_cast<time_t>(wtime + td), &uts);
  uts.tm_year += 1900;
  uts.tm_mon += 1;
  return std::sprintf(result, "%04d/%02d/%02d %02d:%02d:%02d",
                      uts.tm_year, uts.tm_mon, uts.tm_mday,
                      uts.tm_hour, uts.tm_min, uts.tm_sec);
}

size_t FormatDateSimpleWithFrac(char* result, double wtime, int32_t td, int32_t frac_cols) {
  assert(result != nullptr);
  if (wtime < 0) {
    wtime = GetWallTime();
  }
  if (td == INT32MIN) {
    td = GetLocalTimeDifference(true);
  }
  double integ, frac;
  frac = std::modf(wtime, &integ);
  frac_cols = std::min(frac_cols, 12);
  struct std::tm uts;
  GetUniversalCalendar(static_cast<time_t>(integ + td), &uts);
  uts.tm_year += 1900;
  uts.tm_mon += 1;
  if (frac_cols < 1) {
    return std::sprintf(result, "%04d/%02d/%02d %02d:%02d:%02d",
                        uts.tm_year, uts.tm_mon, uts.tm_mday,
                        uts.tm_hour, uts.tm_min, uts.tm_sec);
  }
  char dec[16];
  std::sprintf(dec, "%.12f", frac);
  char* wp = dec;
  if (*wp == '0') {
    wp++;
  }
  wp[frac_cols + 1] = '\0';
  return std::sprintf(result, "%04d/%02d/%02d %02d:%02d:%02d%s",
                      uts.tm_year, uts.tm_mon, uts.tm_mday, uts.tm_hour,
                      uts.tm_min, uts.tm_sec, wp);
}

size_t FormatDateW3CDTF(char* result, int64_t wtime, int32_t td) {
  assert(result != nullptr);
  if (wtime == INT64MIN) {
    wtime = GetWallTime();
  }
  if (td == INT32MIN) {
    td = GetLocalTimeDifference(true);
  }
  struct std::tm uts;
  GetUniversalCalendar(static_cast<time_t>(wtime + td), &uts);
  uts.tm_year += 1900;
  uts.tm_mon += 1;
  td /= 60;
  char tzone[16];
  if (td == 0) {
    std::sprintf(tzone, "Z");
  } else {
    char* wp = tzone;
    if (td < 0) {
      td *= -1;
      *wp++ = '-';
    } else {
      *wp++ = '+';
    }
    std::sprintf(wp, "%02d:%02d", td / 60, td % 60);
  }
  return std::sprintf(result, "%04d-%02d-%02dT%02d:%02d:%02d%s",
                      uts.tm_year, uts.tm_mon, uts.tm_mday,
                      uts.tm_hour, uts.tm_min, uts.tm_sec, tzone);
}

size_t FormatDateW3CDTFWithFrac(char* result, double wtime, int32_t td, int32_t frac_cols) {
  assert(result != nullptr);
  if (wtime < 0) {
    wtime = GetWallTime();
  }
  if (td == INT32MIN) {
    td = GetLocalTimeDifference(true);
  }
  double integ, frac;
  frac = std::modf(wtime, &integ);
  frac_cols = std::min(frac_cols, 12);
  struct std::tm uts;
  GetUniversalCalendar(static_cast<time_t>(integ + td), &uts);
  uts.tm_year += 1900;
  uts.tm_mon += 1;
  td /= 60;
  char tzone[16];
  if (td == 0) {
    std::sprintf(tzone, "Z");
  } else {
    char* wp = tzone;
    if (td < 0) {
      td *= -1;
      *wp++ = '-';
    } else {
      *wp++ = '+';
    }
    std::sprintf(wp, "%02d:%02d", td / 60, td % 60);
  }
  if (frac_cols < 1) {
    return std::sprintf(result, "%04d-%02d-%02dT%02d:%02d:%02d%s",
                        uts.tm_year, uts.tm_mon, uts.tm_mday,
                        uts.tm_hour, uts.tm_min, uts.tm_sec, tzone);
  }
  char dec[16];
  std::sprintf(dec, "%.12f", frac);
  char* wp = dec;
  if (*wp == '0') {
    wp++;
  }
  wp[frac_cols + 1] = '\0';
  return std::sprintf(result, "%04d-%02d-%02dT%02d:%02d:%02d%s%s",
                      uts.tm_year, uts.tm_mon, uts.tm_mday, uts.tm_hour,
                      uts.tm_min, uts.tm_sec, wp, tzone);
}

size_t FormatDateRFC1123(char* result, int64_t wtime, int32_t td) {
  assert(result != nullptr);
  if (wtime < 0) {
    wtime = GetWallTime();
  }
  if (td == INT32MIN) {
    td = GetLocalTimeDifference(true);
  }
  struct std::tm uts;
  GetUniversalCalendar(static_cast<time_t>(wtime + td), &uts);
  uts.tm_year += 1900;
  td /= 60;
  char* wp = result;
  switch (uts.tm_wday) {
    case 0: wp += std::sprintf(wp, "Sun, "); break;
    case 1: wp += std::sprintf(wp, "Mon, "); break;
    case 2: wp += std::sprintf(wp, "Tue, "); break;
    case 3: wp += std::sprintf(wp, "Wed, "); break;
    case 4: wp += std::sprintf(wp, "Thu, "); break;
    case 5: wp += std::sprintf(wp, "Fri, "); break;
    case 6: wp += std::sprintf(wp, "Sat, "); break;
  }
  wp += std::sprintf(wp, "%02d ", uts.tm_mday);
  switch (uts.tm_mon) {
    case 0: wp += std::sprintf(wp, "Jan "); break;
    case 1: wp += std::sprintf(wp, "Feb "); break;
    case 2: wp += std::sprintf(wp, "Mar "); break;
    case 3: wp += std::sprintf(wp, "Apr "); break;
    case 4: wp += std::sprintf(wp, "May "); break;
    case 5: wp += std::sprintf(wp, "Jun "); break;
    case 6: wp += std::sprintf(wp, "Jul "); break;
    case 7: wp += std::sprintf(wp, "Aug "); break;
    case 8: wp += std::sprintf(wp, "Sep "); break;
    case 9: wp += std::sprintf(wp, "Oct "); break;
    case 10: wp += std::sprintf(wp, "Nov "); break;
    case 11: wp += std::sprintf(wp, "Dec "); break;
  }
  wp += std::sprintf(wp, "%04d %02d:%02d:%02d ",
                     uts.tm_year, uts.tm_hour, uts.tm_min, uts.tm_sec);
  if (td == 0) {
    wp += std::sprintf(wp, "GMT");
  } else {
    if (td < 0) {
      td *= -1;
      *wp++ += '-';
    } else {
      *wp++ += '+';
    }
    wp += std::sprintf(wp, "%02d%02d", td / 60, td % 60);
  }
  return wp - result;
}

double ParseDateStr(std::string_view str) {
  const char* rp = str.data();
  size_t len = str.size();
  while (len > 0 && *rp <= ' ') {
    rp++;
    len--;
  }
  if (len == 0) {
    return DOUBLENAN;
  }
  if (len > 1 && rp[0] == '0' && (rp[1] == 'x' || rp[1] == 'X')) {
    return StrToIntHex(std::string_view(reinterpret_cast<const char*>(rp + 2), len - 2));
  }
  const double wtime = StrToDouble(std::string_view(reinterpret_cast<const char*>(rp), len));
  const char* pv = rp;
  size_t plen = len;
  if (plen > 0 && (*pv == '+' || *pv == '-')) {
    pv++;
    plen--;
  }
  bool has_num = false;
  while (plen > 0 && ((*pv >= '0' && *pv <= '9') || *pv == '.')) {
    has_num = true;
    pv++;
    plen--;
  }
  while (plen > 0 && *pv <= ' ') {
    pv++;
    plen--;
  }
  if (has_num && plen == 0) {
    return wtime;
  }
  if (has_num && plen > 0 && pv > rp && (plen > 1 || pv[1] <= ' ')) {
    if (pv[0] == 's' || pv[0] == 'S') {
      return wtime;
    }
    if (pv[0] == 'm' || pv[0] == 'M') {
      return wtime * 60;
    }
    if (pv[0] == 'h' || pv[0] == 'H') {
      return wtime * 60 * 60;
    }
    if (pv[0] == 'd' || pv[0] == 'D') {
      return wtime * 60 * 60 * 24;
    }
  }
  struct std::tm uts;
  std::memset(&uts, 0, sizeof(uts));
  uts.tm_year = 70;
  uts.tm_mon = 0;
  uts.tm_mday = 1;
  uts.tm_hour = 0;
  uts.tm_min = 0;
  uts.tm_sec = 0;
  uts.tm_isdst = 0;
  if (len >= 10 && (rp[4] == '-' || rp[4] == '/' || rp[4] == ':') &&
      (rp[7] == '-' || rp[7] == '/' || rp[7] == ':')) {
    double frac = 0;
    int32_t td = 0;
    uts.tm_year = StrToInt(std::string_view(rp + 0, 4)) - 1900;
    uts.tm_mon = StrToInt(std::string_view(rp + 5, 2)) - 1;
    uts.tm_mday = StrToInt(std::string_view(rp + 8, 2));
    if (len >= 19 && (rp[10] == 'T' || rp[10] == ' ') &&
        (rp[13] == '-' || rp[13] == '/' || rp[13] == ':') &&
        (rp[16] == '-' || rp[16] == '/' || rp[16] == ':')) {
      uts.tm_hour = StrToInt(std::string_view(rp + 11, 2));
      uts.tm_min = StrToInt(std::string_view(rp + 14, 2));
      uts.tm_sec = StrToInt(std::string_view(rp + 17, 2));
      rp += 19;
      len -= 19;
      if (len > 0 && *rp == '.') {
        frac = StrToDouble(rp, len);
        rp++;
        len--;
        while (len > 0 && (*rp >= '0' && *rp <= '9')) {
          rp++;
          len--;
        }
      }
      while (len > 0 && (*rp >= '\0' && *rp <= ' ')) {
        rp++;
        len--;
      }
      if (len >= 3 && (rp[0] == '+' || rp[0] == '-')) {
        td = StrToInt(std::string_view(rp + 1, 2)) * 3600;
        if (len >= 6 && rp[3] == ':') {
          td += StrToInt(std::string_view(rp + 4, 2)) * 60;
        }
        if (rp[0] == '-') {
          td *= -1;
        }
      }
    }
    return MakeUniversalTime(uts) + frac - td;
  }
  if (len >= 4 && rp[3] == ',') {
    rp += 4;
    len -= 4;
    while (len > 0 && *rp == ' ') {
      rp++;
      len--;
    }
    uts.tm_mday = StrToInt(std::string_view(rp, len));
    if (uts.tm_mday < 1) {
      return DOUBLENAN;
    }
    while (len > 0 && ((*rp >= '0' && *rp <= '9') || *rp == ' ')) {
      rp++;
      len--;
    }
    if (len < 3) {
      return DOUBLENAN;
    }
    std::string_view month(rp, 3);
    if (StrCaseCompare(month, "jan") == 0) {
      uts.tm_mon = 0;
    } else if (StrCaseCompare(month, "feb") == 0) {
      uts.tm_mon = 1;
    } else if (StrCaseCompare(month, "mar") == 0) {
      uts.tm_mon = 2;
    } else if (StrCaseCompare(month, "apr") == 0) {
      uts.tm_mon = 3;
    } else if (StrCaseCompare(month, "may") == 0) {
      uts.tm_mon = 4;
    } else if (StrCaseCompare(month, "jun") == 0) {
      uts.tm_mon = 5;
    } else if (StrCaseCompare(month, "jul") == 0) {
      uts.tm_mon = 6;
    } else if (StrCaseCompare(month, "aug") == 0) {
      uts.tm_mon = 7;
    } else if (StrCaseCompare(month, "sep") == 0) {
      uts.tm_mon = 8;
    } else if (StrCaseCompare(month, "oct") == 0) {
      uts.tm_mon = 9;
    } else if (StrCaseCompare(month, "nov") == 0) {
      uts.tm_mon = 10;
    } else if (StrCaseCompare(month, "dec") == 0) {
      uts.tm_mon = 11;
    } else {
      return DOUBLENAN;
    }
    rp += 3;
    len -= 3;
    while (len > 0 && *rp == ' ') {
      rp++;
      len--;
    }
    uts.tm_year = StrToInt(std::string_view(rp, len));
    if (uts.tm_year >= 1969) {
      uts.tm_year -= 1900;
    }
    while (len > 0 && *rp >= '0' && *rp <= '9') {
      rp++;
      len--;
    }
    while (len > 0 && *rp == ' ') {
      rp++;
      len--;
    }
    int32_t td = 0;
    if (len >= 8 && rp[2] == ':' && rp[5] == ':') {
      uts.tm_hour = StrToInt(std::string_view(rp + 0, 2));
      uts.tm_min = StrToInt(std::string_view(rp + 3, 2));
      uts.tm_sec = StrToInt(std::string_view(rp + 6, 2));
      rp += 8;
      len -= 8;
      while (len > 0 && (*rp >= '\0' && *rp <= ' ')) {
        rp++;
        len--;
      }
      if (len >= 3 && (rp[0] == '+' || rp[0] == '-')) {
        td = StrToInt(std::string_view(rp + 1, 2)) * 3600;
        if (len >= 5 && (rp[4] >= '0' && rp[4] <= '9')) {
          td += StrToInt(std::string_view(rp + 3, 2)) * 60;
        }
        if (rp[0] == '-') {
          td *= -1;
        }
      } else if (len >= 3) {
        std::string_view tzone(rp, 3);
        if (StrCaseCompare(tzone, "jst") == 0) {
          td = 9 * 3600;
        } else if (StrCaseCompare(tzone, "cct") == 0) {
          td = 8 * 3600;
        } else if (StrCaseCompare(tzone, "kst") == 0) {
          td = 9 * 3600;
        } else if (StrCaseCompare(tzone, "edt") == 0) {
          td = -4 * 3600;
        } else if (StrCaseCompare(tzone, "est") == 0) {
          td = -5 * 3600;
        } else if (StrCaseCompare(tzone, "cdt") == 0) {
          td = -5 * 3600;
        } else if (StrCaseCompare(tzone, "cst") == 0) {
          td = -6 * 3600;
        } else if (StrCaseCompare(tzone, "mdt") == 0) {
          td = -6 * 3600;
        } else if (StrCaseCompare(tzone, "mst") == 0) {
          td = -7 * 3600;
        } else if (StrCaseCompare(tzone, "pdt") == 0) {
          td = -7 * 3600;
        } else if (StrCaseCompare(tzone, "pst") == 0) {
          td = -8 * 3600;
        } else if (StrCaseCompare(tzone, "hdt") == 0) {
          td = -9 * 3600;
        } else if (StrCaseCompare(tzone, "hst") == 0) {
          td = -10 * 3600;
        }
      }
    }
    return MakeUniversalTime(uts) - td;
  }
  return DOUBLENAN;
}

double ParseDateStrYYYYMMDD(std::string_view str, int32_t td) {
  if (td == INT32MIN) {
    td = GetLocalTimeDifference(true);
  }
  char buf[16];
  char* wp = buf;
  for (int32_t c : str) {
    if (static_cast<size_t>(wp - buf) >= sizeof(buf)) {
      break;
    }
    if (c >= '0' && c <= '9') {
      *wp++ = c;
    }
  }
  const size_t len = wp - buf;
  if (len < 4) {
    return DOUBLENAN;
  }
  struct std::tm uts;
  std::memset(&uts, 0, sizeof(uts));
  uts.tm_year = StrToInt(std::string_view(buf, 4)) - 1900;
  uts.tm_mon = 0;
  uts.tm_mday = 1;
  uts.tm_hour = 0;
  uts.tm_min = 0;
  uts.tm_sec = 0;
  uts.tm_isdst = 0;
  if (len >= 6) {
    uts.tm_mon = StrToInt(std::string_view(buf + 4, 2)) - 1;
  }
  if (len >= 8) {
    uts.tm_mday = StrToInt(std::string_view(buf + 6, 2));
  }
  if (len >= 10) {
    uts.tm_hour = StrToInt(std::string_view(buf + 8, 2));
  }
  if (len >= 12) {
    uts.tm_min = StrToInt(std::string_view(buf + 10, 2));
  }
  if (len >= 14) {
    uts.tm_sec = StrToInt(std::string_view(buf + 12, 2));
  }
  return MakeUniversalTime(uts) - td;
}

std::string MakeRelativeTimeExpr(double diff) {
  const double abs_diff = std::fabs(diff);
  if (abs_diff >= 86400) {
    return SPrintF("%0.1f days", diff / 86400);
  } else if (abs_diff >= 3600) {
    return SPrintF("%0.1f hours", diff / 3600);
  } else if (abs_diff >= 60) {
    return SPrintF("%0.1f minutes", diff / 60);
  }
  return SPrintF("%0.1f seconds", diff);
}

}  // namespace tkrzw

// END OF FILE
