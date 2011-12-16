/*
 * (C) 2011-2012 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * support range query
 *
 * Version: $Id$
 *
 * Authors:
 *   balanth <balanth.quanxf@alipay.com>
 *
 */

#ifndef TAIR_PACKET_GET_RANGE_PACKET_H
#define TAIR_PACKET_GET_RANGE_PACKET_H
#include "base_packet.hpp"
namespace tair {
		class request_get_range : public base_packet {
				public:
						request_get_range()
						{
								setPCode(TAIR_REQ_GET_RANGE_PACKET);
								server_flag = 0;
								area = 0;
								key_count = 0;
								limit = 0;
						}

						request_get_range(request_get_range &packet)
						{
								setPCode(TAIR_REQ_GET_RANGE_PACKET);
								server_flag = packet.server_flag;
								area = packet.area;
								key_count = packet.key_count;
								limit = packet.limit;
								key_start = packet.key_start;
								key_end = packet.key_end;
						}

						~request_get_range()
						{
					    }

						bool encode(tbnet::DataBuffer *output)
						{
								output->writeInt8(server_flag);
								output->writeInt16(area);
								output->writeInt32(key_count);
								output->writeInt32(limit);

								key_start.encode(output);
								key_end.encode(output);
								return true;
						}

						bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
						{
								if (header->_dataLen < 7) {
										log_warn( "buffer data too few.");
										return false;
								}
								server_flag = input->readInt8();
								area = input->readInt16();
								//read limit
								limit = input->readInt32();		 
								key_count = input->readInt32();

								if(key_count == 2){
										key_start.decode(input);
										key_end.decode(input);
								}else{
										log_error("range get: range failure.the number of begin or end key not match.");
								}

								return true;
						}

				public:
						uint16_t           area;
						uint32_t           key_count;
						uint32_t           limit;
						data_entry       key_start;
						data_entry       key_end;
		};

}
#endif //TAIR_PACKET_GET_RANGE_PACKET_H

