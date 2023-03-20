#pragma once

#include "util/str.h"

#define PACKED __attribute__((packed))
#define ALWAYS_INLINE __attribute__((always_inline))

static inline std::string RandomStr(fast_random &r, uint len) {
  // this is a property of the oltpbench implementation...
  if (!len) return "";

  uint i = 0;
  std::string buf(len - 1, 0);
  while (i < (len - 1)) {
    const char c = r.next_char();
    // XXX(stephentu): oltpbench uses java's Character.isLetter(), which
    // is a less restrictive filter than isalnum()
    if (!isalnum(c)) continue;
    buf[i++] = c;
  }
  return buf;
}

static inline std::string RandomNStr(fast_random &r, uint len) {
  const char base = '0';
  std::string buf(len, 0);
  for (uint i = 0; i < len; i++) buf[i] = (char)(base + (r.next() % 10));
  return buf;
}

static inline ALWAYS_INLINE int RandomNumber(fast_random &r, int min, int max) {
  int res = (int)(r.next_uniform() * (max - min + 1) + min);
  assert(res >= min && res <= max);
  return res;
}

static inline ALWAYS_INLINE int NonUniformRandom(fast_random &r, int A, int C,
                                                 int min, int max) {
  return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) %
          (max - min + 1)) +
         min;
}

std::string NameTokens[] = {
    std::string("BAR"),  std::string("OUGHT"), std::string("ABLE"),
    std::string("PRI"),  std::string("PRES"),  std::string("ESE"),
    std::string("ANTI"), std::string("CALLY"), std::string("ATION"),
    std::string("EING"),
};

// all tokens are at most 5 chars long
const size_t CustomerLastNameMaxSize = 5 * 3;

static inline size_t GetCustomerLastName(uint8_t *buf, fast_random &r,
                                         int num) {
  const std::string &s0 = NameTokens[num / 100];
  const std::string &s1 = NameTokens[(num / 10) % 10];
  const std::string &s2 = NameTokens[num % 10];
  uint8_t *const begin = buf;
  const size_t s0_sz = s0.size();
  const size_t s1_sz = s1.size();
  const size_t s2_sz = s2.size();
  memcpy(buf, s0.data(), s0_sz);
  buf += s0_sz;
  memcpy(buf, s1.data(), s1_sz);
  buf += s1_sz;
  memcpy(buf, s2.data(), s2_sz);
  buf += s2_sz;
  return buf - begin;
}

static inline std::string GetCustomerLastName(fast_random &r, int num) {
  std::string ret;
  ret.resize(CustomerLastNameMaxSize);
  ret.resize(GetCustomerLastName((uint8_t *)&ret[0], r, num));
  return ret;
}

static inline uint64_t GetCustomerNameIndex(uint32_t d, const std::string& last) {
  uint64_t key = 0;
	char offset = 'A';
  ASSERT(last.size() <= 15) << last.size();
	for (uint32_t i = 0; i < last.size(); i++) {
		key = (key << 4) + (last[i] - offset);
  }
	key += ((uint64_t)d) << 60;
  return key;
}

static inline ALWAYS_INLINE std::string GetNonUniformCustomerLastName(
    fast_random &r, int c_last) {
  return GetCustomerLastName(r, NonUniformRandom(r, 255, c_last, 0, 999));
}

static inline uint32_t GetCurrentTimeMillis() {
  static __thread uint32_t tl_hack = 3001;
  return tl_hack++;
}

struct alignas(8) W_val {
  W_val() { memset(this, 0, sizeof(W_val)); }
  float w_ytd = 0.0;
  float w_tax = 0.0;
  varibale_str<10> w_name;
  varibale_str<20> w_street_1;
  varibale_str<20> w_street_2;
  varibale_str<20> w_city;
  varibale_str<2> w_state;
  varibale_str<9> w_zip;
  friend bool operator<(const W_val &l, const W_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(W_val)) < 0;
  }
  friend bool operator>(const W_val &l, const W_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(W_val)) > 0;
  }
  friend bool operator==(const W_val &l, const W_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(W_val)) == 0;
  }
  friend bool operator!=(const W_val &l, const W_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(W_val)) != 0;
  }
} PACKED;

struct alignas(8) D_val {
  D_val() { memset(this, 0, sizeof(D_val)); }
  float d_ytd;
  float d_tax;
  uint32_t d_next_o_id;
  varibale_str<10> d_name;
  varibale_str<20> d_street1;
  varibale_str<20> d_street2;
  varibale_str<20> d_city;
  varibale_str<2> d_state;
  varibale_str<9> d_zip;
  friend bool operator<(const D_val &l, const D_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(D_val)) < 0;
  }
  friend bool operator>(const D_val &l, const D_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(D_val)) > 0;
  }
  friend bool operator==(const D_val &l, const D_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(D_val)) == 0;
  }
  friend bool operator!=(const D_val &l, const D_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(D_val)) != 0;
  }
} PACKED;

struct alignas(8) C_val {
  C_val() { memset(this, 0, sizeof(C_val)); }
  float c_discount;
  float c_credit_lim;
  float c_balance;
  float c_ytd_payment;
  uint32_t c_payment_cnt;
  uint32_t c_delivery_cnt;
  uint32_t c_since;
  varibale_str<2> c_credit;
  varibale_str<16> c_last;
  varibale_str<16> c_first;
  varibale_str<20> c_street_1;
  varibale_str<20> c_street_2;
  varibale_str<20> c_city;
  varibale_str<2> c_state;
  varibale_str<9> c_zip;
  varibale_str<16> c_phone;
  varibale_str<2> c_middle;
  varibale_str<500> c_data;
  friend bool operator<(const C_val &l, const C_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(C_val)) < 0;
  }
  friend bool operator>(const C_val &l, const C_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(C_val)) > 0;
  }
  friend bool operator==(const C_val &l, const C_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(C_val)) == 0;
  }
  friend bool operator!=(const C_val &l, const C_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(C_val)) != 0;
  }
} PACKED;

struct alignas(8) O_val {
  O_val() { memset(this, 0, sizeof(O_val)); }
  uint32_t o_c_id;
  uint32_t o_carrier_id;
  uint8_t o_ol_cnt;
  bool o_all_local;
  uint32_t o_entry_d;
  friend bool operator<(const O_val &l, const O_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(O_val)) < 0;
  }
  friend bool operator>(const O_val &l, const O_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(O_val)) > 0;
  }
  friend bool operator==(const O_val &l, const O_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(O_val)) == 0;
  }
  friend bool operator!=(const O_val &l, const O_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(O_val)) != 0;
  }
} PACKED;

struct alignas(8) NO_val {
  NO_val() { memset(this, 0, sizeof(NO_val)); }
  varibale_str<2> no_dummy;
  bool no_exist;
  friend bool operator<(const NO_val &l, const NO_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(NO_val)) < 0;
  }
  friend bool operator>(const NO_val &l, const NO_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(NO_val)) > 0;
  }
  friend bool operator==(const NO_val &l, const NO_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(NO_val)) == 0;
  }
  friend bool operator!=(const NO_val &l, const NO_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(NO_val)) != 0;
  }
} PACKED;

struct alignas(8) I_val {
  I_val() { memset(this, 0, sizeof(I_val)); }
  float i_price;
  varibale_str<24> i_name;
  varibale_str<50> i_data;
  uint32_t i_im_id;
  friend bool operator<(const I_val &l, const I_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(I_val)) < 0;
  }
  friend bool operator>(const I_val &l, const I_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(I_val)) > 0;
  }
  friend bool operator==(const I_val &l, const I_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(I_val)) == 0;
  }
  friend bool operator!=(const I_val &l, const I_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(I_val)) != 0;
  }
} PACKED;

struct alignas(8) S_val {
  S_val() { memset(this, 0, sizeof(S_val)); }
  uint32_t s_quantity;
  float s_ytd;
  uint32_t s_order_cnt;
  uint32_t s_remote_cnt;
  friend bool operator<(const S_val &l, const S_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(S_val)) < 0;
  }
  friend bool operator>(const S_val &l, const S_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(S_val)) > 0;
  }
  friend bool operator==(const S_val &l, const S_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(S_val)) == 0;
  }
  friend bool operator!=(const S_val &l, const S_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(S_val)) != 0;
  }
} PACKED;

// S_Data_val
struct SD_val {
  varibale_str<50> s_data;
  varibale_str<24> s_dist_01;
  varibale_str<24> s_dist_02;
  varibale_str<24> s_dist_03;
  varibale_str<24> s_dist_04;
  varibale_str<24> s_dist_05;
  varibale_str<24> s_dist_06;
  varibale_str<24> s_dist_07;
  varibale_str<24> s_dist_08;
  varibale_str<24> s_dist_09;
  varibale_str<24> s_dist_10;
  friend bool operator<(const SD_val &l, const SD_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(SD_val)) < 0;
  }
  friend bool operator>(const SD_val &l, const SD_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(SD_val)) > 0;
  }
  friend bool operator==(const SD_val &l, const SD_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(SD_val)) == 0;
  }
  friend bool operator!=(const SD_val &l, const SD_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(SD_val)) != 0;
  }
} PACKED;

struct alignas(8) OL_val {
  OL_val() { memset(this, 0, sizeof(OL_val)); }
  uint32_t ol_i_id;
  uint32_t ol_delivery_d;
  float ol_amount;
  uint32_t ol_supply_w_id;
  uint8_t ol_quantity;
  varibale_str<24> ol_dist;
  friend bool operator<(const OL_val &l, const OL_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(OL_val)) < 0;
  }
  friend bool operator>(const OL_val &l, const OL_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(OL_val)) > 0;
  }
  friend bool operator==(const OL_val &l, const OL_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(OL_val)) == 0;
  }
  friend bool operator!=(const OL_val &l, const OL_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(OL_val)) != 0;
  }
} PACKED;

struct alignas(8) H_val {
  H_val() { memset(this, 0, sizeof(H_val)); }
  float h_amount;
  varibale_str<24> h_data;
  friend bool operator<(const H_val &l, const H_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(H_val)) < 0;
  }
  friend bool operator>(const H_val &l, const H_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(H_val)) > 0;
  }
  friend bool operator==(const H_val &l, const H_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(H_val)) == 0;
  }
  friend bool operator!=(const H_val &l, const H_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(H_val)) != 0;
  }
} PACKED;