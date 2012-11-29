/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id: key_value_pack_t.hpp 392 2012-04-01 02:02:41Z ganyu.hfl@taobao.com $
 *
 * Authors:
 *   ganyu.hfl <ganyu.hfl@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_key_value_pack_t_HPP
#define TAIR_key_value_pack_t_HPP
#include "tbnet.h"

namespace tair {
  namespace common {
    struct key_value_pack_t {
      key_value_pack_t() {
        memset(this, 0, sizeof(*this));
      }

      key_value_pack_t(const key_value_pack_t &rhs)
        : key(new data_entry(*rhs.key)),
          value(new data_entry(*rhs.value)){
        version = rhs.version;
        expire = rhs.expire;
      }

      key_value_pack_t& operator=(const key_value_pack_t &rhs) {
        if (this != &rhs) {
          *key = *rhs.key;
          *value = *rhs.value;
          version = rhs.version;
          expire = rhs.expire;
        }
        return *this;
      }

      data_entry    *key;
      data_entry    *value;
      int           version;
      int           expire;
    };
  }
}
#endif
