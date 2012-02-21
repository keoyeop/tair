/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: duplicate_manager.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#ifndef DUP_SYNC_MANAGER_H
#define DUP_SYNC_MANAGER_H


#include <tbsys.h>
#include <tbnet.h>
#include "table_manager.hpp"
//#include "boost/shared_ptr.hpp"
#include "duplicate_base.hpp"
#include "duplicate_packet.hpp"

#include "BlockQueueEx.hpp"
#include "put_packet.hpp"
#include "scoped_wrlock.hpp"

#include <queue>
#include <map>
#include <ext/hash_map>

namespace tair {
     

	 struct CPacket_wait_Nodes
     {
#define MAX_DUP_COUNT 2 
       public:
				 tbnet::Connection *conn;
				 uint32_t chid;
				 int pcode;
         int conf_version;
         int bucket_number;
         int inc_value_result; //for TAIR_REQ_INCDEC_PACKET 
       private:
         uint64_t des_server_ids[MAX_DUP_COUNT ]; //3 copy is enough.
       public:
         CPacket_wait_Nodes(int _bucket_number,base_packet* _request,const vector<uint64_t>&  _des_server_ids,int _conf_version,const data_entry* value)
		 {
		   bucket_number=_bucket_number;
		   conn=_request->get_connection();
		   chid=_request->getChannelId();
		   pcode=_request->getPCode();
		   conf_version=_conf_version;

		   int _des_size=_des_server_ids.size();
		   int i=0;
		   for(;i<_des_size&& i<MAX_DUP_COUNT;i++)
		   {
			 des_server_ids[i]= _des_server_ids[i];
		   }
		   for(;i<MAX_DUP_COUNT;i++)
		   {
			 des_server_ids[i]=0;
		   }

		   //now we have a ugly code. the TAIR_REQ_INCDEC_PACKET should have result value
		   const int ITEM_HEAD_LENGTH = 2;
		   if(TAIR_REQ_INCDEC_PACKET==_request->getPCode())
		   {
			 inc_value_result= *((int32_t *)(value->get_data() + ITEM_HEAD_LENGTH));
		   }
		   else
		   {
			 inc_value_result=0;
		   }
		 }

         ~CPacket_wait_Nodes()
         {
         }

        int do_response(int,uint64_t des_srvid)
				{
					bool bfound=false;
					int _acked=0;
					for(int i=0;i<MAX_DUP_COUNT;i++)
					{
						if(des_srvid==des_server_ids[i]) 
						{
							des_server_ids[i]=0;
							bfound=true;
						}
						if(0==des_server_ids[i]) _acked++;
					}

					if(!bfound) 
					{
						return TAIR_RETURN_DUPLICATE_REACK;
					}

					if(_acked==MAX_DUP_COUNT)
					{
						return 0;
					}
					else
					{
						return TAIR_RETURN_DUPLICATE_ACK_WAIT;
					}
				}

     };

     typedef __gnu_cxx::hash_map<uint32_t,struct CPacket_wait_Nodes*> CDuplicatPkgMap;
     typedef __gnu_cxx::hash_map<uint32_t,struct CPacket_wait_Nodes*>::iterator CDuplicatPkgMapIter;

#ifndef __PACKET_TIME_OUT_HINT
#define __PACKET_TIME_OUT_HINT
	 class CPacket_Timeout_hint
	 {
	   public:
		 uint32_t packet_id; 
		 time_t expired;
		CPacket_Timeout_hint(uint32_t _packet_id)
		{
		  packet_id=_packet_id;
		  expired=time(NULL)+TAIR_SERVER_OP_TIME;
		}
		~CPacket_Timeout_hint()
		{
		}
	 };
	 typedef BlockQueueEx<CPacket_Timeout_hint * > CWaitPacketQueue;
#endif

	 class CPacket_wait_manager
   {
#define MAP_BUCKET_SLOT  17
#define MAP_BUCKET_SIZE  200000
#define TAIR_MAX_BUCKET_NUMBER  1024
     public:
       CPacket_wait_manager();
       ~CPacket_wait_manager();
     public:
       int addWaitNode(int area, const data_entry* , const data_entry* ,int bucket_number, const vector<uint64_t>& des_server_ids,base_packet *request, uint32_t max_packet_id,int &version);
			 int doResponse(int bucket_number, uint64_t des_server_id,uint32_t max_packet_id,struct CPacket_wait_Nodes **pNode);
			 int doTimeout( uint32_t max_packet_id, time_t __post_time);
			 int clear_waitnode( uint32_t max_packet_id);
       bool isBucketFree(int bucket_number)  ;
       int  changeBucketCount(int bucket_number,int number);
     public:
       bool put_timeout_hint(int index,CPacket_Timeout_hint *hint);
       CPacket_Timeout_hint *get_timeout_hint(int index,int msec);
     private:
       int get_map_index(uint32_t max_packet_id);
     private:
       //pthread_rwlock_t    m_mutex;
       CSlotLocks *m_slots_locks;
       CDuplicatPkgMap m_PkgWaitMap[MAP_BUCKET_SLOT];
       int         m_bucket_count [TAIR_MAX_BUCKET_NUMBER]; //indictor for bucket's node count,not older than 1024.
  	   CWaitPacketQueue dup_wait_queue[MAX_DUP_COUNT];
   };


	 //monitor bucket_number until packet timeout

   class dup_sync_sender_manager : public base_duplicator, public tbsys::CDefaultRunnable, public tbnet::IPacketHandler {
   public:

      dup_sync_sender_manager( tbnet::Transport *transport,
                                tbnet::DefaultPacketStreamer *streamer, table_manager* table_mgr);
      ~dup_sync_sender_manager();
      void do_hash_table_changed();
      void set_max_queue_size(uint32_t max_queue_size) {
         this->max_queue_size = max_queue_size;
      }

      bool is_bucket_available(uint32_t bucket_id);


      int duplicate_data(int area, const data_entry* key, const data_entry* value,int expire_time,
                          int bucket_number, const vector<uint64_t>& des_server_ids,base_packet *request,int version);
      int direct_send(int area, const data_entry* key, const data_entry* value,int  expire_time,
            int bucket_number, const vector<uint64_t>& des_server_ids,uint32_t max_packet_id);


      bool has_bucket_duplicate_done(int bucket_number);

      int do_duplicate_response(uint32_t bucket_id, uint64_t d_server_id, uint32_t packet_id);

      void run(tbsys::CThread *thread, void *arg);


	  void handleTimeOutPacket(CPacket_Timeout_hint  * _pkg);
      tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet *packet, void *args);
			int rspPacket(const CPacket_wait_Nodes * pNode);
   private:
	  table_manager* table_mgr;
	  tbnet::ConnectionManager* conn_mgr;

	  CPacket_wait_manager packets_mgr;

	  volatile int have_data_to_send;
	  uint32_t max_queue_size;

	  atomic_t packet_id_creater;
   };
}
#endif
