/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * put packet
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_PUT_PACKET_H
#define TAIR_PACKET_PUT_PACKET_H
#include "base_packet.hpp"
namespace tair {
   class request_put : public base_packet {
   public:
      request_put()
      {
         setPCode(TAIR_REQ_PUT_PACKET);
         server_flag = 0;
         area = 0;
         version = 0;
         expired = 0;
      }


      request_put(request_put &packet)
      {
         setPCode(TAIR_REQ_PUT_PACKET);
         server_flag = packet.server_flag;
         area = packet.area;
         version = packet.version;
         expired = packet.expired;
         key.clone(packet.key);
         data.clone(packet.data);
      }

      ~request_put()
      {
      }

      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt8(server_flag);
         output->writeInt16(area);
         output->writeInt16(version);
         output->writeInt32(expired);
         key.encode(output);
         data.encode(output);


         return true;
      }

      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         if (header->_dataLen < 15) {
            log_warn( "buffer data too few.");
            return false;
         }
         server_flag = input->readInt8();
         area = input->readInt16();
         version = input->readInt16();
         expired = input->readInt32();

         key.decode(input);
         data.decode(input);
         key.data_meta.version = version;

         return true;
      }

   public:
      uint16_t        area;
      uint16_t        version;
      int32_t         expired;
      data_entry    key;
      data_entry    data;
   };

   class mput_record {
    public:
     mput_record()
     {
       key = NULL;
       value = NULL;
     }

     mput_record(mput_record &rec)
     {
       key = new data_entry(*(rec.key));
       value = new value_entry(*(rec.value));
     }

     ~mput_record()
     {
       if (key != NULL ) {
         delete key;
         key = NULL;
       }
       if (value != NULL) {
         delete value;
         value = NULL;
       }
     }

     public:
      data_entry* key;
      value_entry* value;
   };

   typedef vector<mput_record* > mput_record_vec;

   class request_mput : public base_packet {
     public:
       request_mput()
       {
         setPCode(TAIR_REQ_MPUT_PACKET);
         server_flag = 0;
         area = 0;
         count = 0;
         len = 8;
         record_vec = NULL;
       }

       request_mput(request_mput &packet)
       {
         setPCode(TAIR_REQ_MPUT_PACKET);
         server_flag = packet.server_flag;
         area = packet.area;
         count = packet.count;
         len = packet.len;
         record_vec = NULL;
         if (packet.record_vec != NULL) {
           record_vec = new mput_record_vec();
           mput_record_vec::iterator it;
           for (it = packet.record_vec->begin(); it != packet.record_vec->end(); ++it) {
             mput_record* rec = new mput_record(**it);
             record_vec->push_back(rec);
           }
         }
       }

       ~request_mput()
       {
         if (record_vec != NULL) {
            mput_record_vec::iterator it;
            for (it = record_vec->begin(); it != record_vec->end(); ++it) {
               delete (*it);
            }

            delete record_vec;
            record_vec = NULL;
         }
       }

       bool encode(tbnet::DataBuffer *output)
       {
         output->writeInt8(server_flag);
         output->writeInt16(area);
         output->writeInt32(count);
         if (record_vec != NULL) {
            mput_record_vec::iterator it;
            for (it = record_vec->begin(); it != record_vec->end(); ++it) {
               mput_record* rec = (*it);
               rec->key->encode(output);
               rec->value->encode(output);
            }
         }
         return true;
       }

       bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
       {
         if (header->_dataLen < 8) {
            log_warn( "buffer data too few.");
            return false;
         }
         server_flag = input->readInt8();
         area = input->readInt16();
         count = input->readInt32();

         if (count > 0) {
            record_vec = new mput_record_vec();
            for (uint32_t i = 0; i < count; i++) {
               mput_record *rec = new mput_record();
               data_entry* key = new data_entry();
               key->decode(input);
               rec->key = key;
               value_entry* value = new value_entry();
               value->decode(input);
               rec->value = value;
               record_vec->push_back(rec);
            }
         }
         return true;
       }

       bool add_put_key_data(const data_entry &key, const value_entry &data)
       {
         uint32_t temp = len + key.get_size() + 1 + data.get_size();
         if (temp > MAX_MPUT_PACKET_SIZE && count > 0) {
           return false;
         }
         if (record_vec == NULL) {
           record_vec = new mput_record_vec();
         }

         mput_record* rec = new mput_record();
         rec->key = new data_entry();
         rec->key->clone(key);
         rec->value = new value_entry();
         rec->value->clone(data);
         record_vec->push_back(rec);
         len += key.get_size() + 1;
         len += data.get_size();
         count++;
         return true;
       }

     public:
       uint16_t area;
       uint32_t count;
       uint32_t len;
       mput_record_vec* record_vec;
   };
}
#endif
