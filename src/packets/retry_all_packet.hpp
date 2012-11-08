#ifndef RETRY_ALL_PACKET_H
#define RETRY_ALL_PACKET_H
#include "base_packet.hpp"
#include "invalid_packet.hpp"
namespace tair {
  class request_retry_all : public request_invalid {
  public:
    request_retry_all()
    {
      setPCode(TAIR_REQ_RETRY_ALL_PACKET);
      is_sync = ASYNC_INVALID;
    }

    request_retry_all(request_retry_all& retry_all) : request_invalid(retry_all)
    {
      setPCode(TAIR_REQ_RETRY_ALL_PACKET);
      is_sync = ASYNC_INVALID;
    }

    ~request_retry_all()
    {
    }
  };
}
#endif
