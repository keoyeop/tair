#ifndef INVAL_HEARTBEAT_PACKET_H
#define INVAL_HEARTBEAT_PACKET_H
#include "base_packet.hpp"
namespace tair {
  class request_inval_heartbeat : public base_packet
  {
    public:
      request_inval_heartbeat()
      {
        setPCode(TAIR_REQ_INVAL_HEARTBEAT_PACKET);
        version = 0U;
        server_id = 0U;
      }

      request_inval_heartbeat(request_inval_heartbeat &packet) : base_packet(packet)
      {
        setPCode(TAIR_REQ_INVAL_HEARTBEAT_PACKET);
        version = packet.version;
        server_id = packet.server_id;
      }

      ~request_inval_heartbeat()
      {
      }

      bool encode(tbnet::DataBuffer *output)
      {
        output->writeInt32(version);
        output->writeInt64(server_id);
        return true;
      }

      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
        if (header->_dataLen < 12) {
          log_warn("buffer data too few");
          return false;
        }
        version = input->readInt32();
        server_id = input->readInt64();
        return true;
      }
    public:
      uint32_t version;
      uint64_t server_id;
    };

    class response_inval_heartbeat : public base_packet
    {
    public:
      response_inval_heartbeat() : base_packet()
      {
        setPCode(TAIR_RESP_INVAL_HEARTBEAT_PACKET);
        version = 0;
        config_server_version = 0;
      }
      response_inval_heartbeat(request_inval_heartbeat &packet) : base_packet(packet)
      {
        setPCode(TAIR_RESP_INVAL_HEARTBEAT_PACKET);
        version = 0;
        config_server_version = 0;
      }
      ~response_inval_heartbeat()
      {
      }
      void set_config_server_version(int version)
      {
        config_server_version = version;
      }
      int get_config_server_version()
      {
        return config_server_version;
      }

      bool encode(tbnet::DataBuffer *output)
      {
        output->writeInt32(version);
        output->writeInt32(config_server_version);
        return true;
      }

      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
        if (header->_dataLen < 8)
        {
          log_warn("buffer data too few.");
          return false;
        }
        version = input->readInt32();
        config_server_version = input->readInt32();
        return true;
      }
    protected:
      uint32_t version;
      uint32_t config_server_version;
    };
  }
#endif
