#include <stdint.h>
#include <cstring>

template <uint64_t N>
class Str {
   public:
    Str(){};
    Str(const char* src) {
        memcpy(data, src, N);
    }
    Str(const uint8_t* src) {
        memcpy(data, src, N);
    }

    Str& operator=(const char* src) {
        memcpy(data, src, N);
        return *this;
    }

    Str& operator=(const uint8_t* src) {
        memcpy(data, src, N);
        return *this;
    }

    bool operator<(const Str& other) const {
        return memcmp(data, other.data, N) < 0;
    }

    bool operator==(const Str& other) const {
        return memcmp(data, other.data, N) == 0;
    }

   private:
    uint8_t data[N];
};

template <size_t N, char FILL_CHAR = '\0'>
class varibale_str {
 public:
  uint8_t buf[N];

  inline void assign(const char *s) {
    size_t n = strlen(s);
    memcpy(buf, s, n < N ? n : N);
    if (N > n) {
      memset(&buf[n], FILL_CHAR, N - n);
    }
  }

  inline void assign(const char *s, size_t n) {
    memcpy(buf, s, n < N ? n : N);
    if (N > n) {
      memset(&buf[n], FILL_CHAR, N - n);
    }
  }

  inline void assign(const std::string &s) { assign(s.data()); }

  inline bool operator==(const varibale_str &other) const {
    return memcmp(buf, other.buf, N) == 0;
  }

  inline bool operator!=(const varibale_str &other) const {
    return !operator==(other);
  }

  std::string to_string() { return std::string((char*)buf); }
};