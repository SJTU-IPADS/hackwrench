#include <vector>

#include "util/types.h"

class PageSnapshot {
   public:
    bool is_read = false;
    bool is_write = false;
};

struct BatchSnapshot {
    bool is_read = false;
    bool is_write = false;
};