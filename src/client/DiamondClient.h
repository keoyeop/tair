/*
 * $Id: DiamondClient.h 1444 2012-04-18 02:24:56Z shijia.wxr $
 */
#ifndef _LIBDIACLI_DIAMOND_CLIENT_H__
#define _LIBDIACLI_DIAMOND_CLIENT_H__
#include <string>
#include <map>

namespace DIAMOND
{
	class SubscriberListener
	{
	public:
		SubscriberListener();
		virtual ~SubscriberListener();
		virtual void configOnChanged(const char* dataId, const char* groupId, const char* newContent) = 0;
	};

	typedef std::map<std::string, std::string> DiamondConfig;

	class DiamondClientImpl;

	class DiamondClient
	{
		DiamondClientImpl* m_Impl;

	public:
		DiamondClient(const DiamondConfig* config = NULL);
		virtual ~DiamondClient();

		/**
		 * listener对象的创建和销毁都由外部调用者完成
		 */
		bool registerListener(const char* dataId, const char* groupId, SubscriberListener* listener, const std::string& md5);
      void removeListener(const char* dataId, const char* groupId);
      void shutdownListen();

		/**
		 * 根据dataId、groupId从Diamond服务器获取配置
		 */
		bool getConfig(const char* dataId, const char* groupId, std::string& content);
		bool getConfig(const char* dataId, const char* groupId, std::string& content, std::string& error);
      bool getConfig(const char* dataId, const char* groupId, std::string& md5, std::string& content, std::string& error);

		/**
		 * 根据dataId、groupId设置Diamond服务器已有配置，如果没有，则创建
		 */
		bool setConfig(const char* dataId, const char* groupId, const char* content);
		bool setConfig(const char* dataId, const char* groupId, const char* content, std::string& error);
	};
}
#endif // end of _LIBDIACLI_DIAMOND_CLIENT_H__
