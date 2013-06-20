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
		 * listener����Ĵ��������ٶ����ⲿ���������
		 */
		bool registerListener(const char* dataId, const char* groupId, SubscriberListener* listener, const std::string& md5);
      void removeListener(const char* dataId, const char* groupId);
      void shutdownListen();

		/**
		 * ����dataId��groupId��Diamond��������ȡ����
		 */
		bool getConfig(const char* dataId, const char* groupId, std::string& content);
		bool getConfig(const char* dataId, const char* groupId, std::string& content, std::string& error);
      bool getConfig(const char* dataId, const char* groupId, std::string& md5, std::string& content, std::string& error);

		/**
		 * ����dataId��groupId����Diamond�������������ã����û�У��򴴽�
		 */
		bool setConfig(const char* dataId, const char* groupId, const char* content);
		bool setConfig(const char* dataId, const char* groupId, const char* content, std::string& error);
	};
}
#endif // end of _LIBDIACLI_DIAMOND_CLIENT_H__
