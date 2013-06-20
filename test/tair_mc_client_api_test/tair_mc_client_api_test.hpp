/*
 * (C) 2007-2013 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_api.cpp 1245 2013-06-13 10:24:38Z huan.wanghuan@alibaba-inc.com $
 *
 * Authors:
 *   yunhen <huan.wanghuan@alibaba-inc.com>
 *
 */

const char* GROUP_right_choose_CM3=
   "10.235.177.=yunhen-test-CM3\r\n"
   "10.232.4.=yunhen-test-CM3\r\n" // my ip match
   "10.207.=yunhen-test-CM3\r\n"
   "10.238.=yunhen-test-CM4\r\n";

const char* GROUP_wrong=
   "10.235.177.=yunhen-test-CM3\r\n"
   "10.232.4.=-test-CM4\r\n" // my ip match
   "10.207.=yunhen-test-CM3\r\n"
   "10.238.=yunhen-test-CM4\r\n";


const char* GROUP_right_netdevice_test=
   "10.235.177.=yunhen-test-CM3\r\n"
   "10.232.4.=yunhen-test-CM3\r\n" // my ip match eth0
   "10.207.=yunhen-test-CM3\r\n"
   "10.238.=yunhen-test-CM4\r\n"
   "10.7.=yunhen-test-CM4\n\n";  // eth0:0

const char* GROUP_wrong_choose_CM3_nofatal=
   "10.23.1\r\n"
   "= yunehntste\r\n"
   "10.25=\r\n"
   "10.232.4. yunhen-test-CM3 \r\n"
   "10.235.177.=yunhen-test-CM3\r\n"
   "10.232.4.=yunhen-test-CM3\r\n" // my ip match
   "10.207.=yunhen-test-CM3\r\n"
   "10.238.=yunhen-test-CM4\r\n";

const char* GROUP_wrong_choose_CM3_fatal=
   "10.23.1\r\n"
   "= yunehntste\r\n"
   "10.25=\r\n"
   "10.232.4. yunhen-test-CM3 \r\n"
   "10.235.177.=yunhen-test-CM3\r\n"
   "10.232.4.=\r\n" // my ip match
   "10.207.=yunhen-test-CM3\r\n"
   "10.238.=yunhen-test-CM4\r\n";

const char* GROUP_right_choose_CM3_another=
   "10.235.177.=yunhen-test-CM4\r\n"
   "10.232.4.=yunhen-test-CM3\r\n" // my ip match
   "10.238.=yunhen-test-CM4\r\n";

const char* GROUP_right_choose_CM4=
   "10.235.177.=yunhen-test-CM3\r\n"
   "10.232.4.=yunhen-test-CM4\r\n" // my ip match
   "10.207.=yunhen-test-CM3\r\n"
   "10.238.=yunhen-test-CM4\r\n";

const char* GROUP_right_choose_CM4_another=
//   " 10.232.4.177 \r\n"
   "  10.235.177.=yunhen-test-CM3 \r\n"
   "  10.232.4. = yunhen-test-CM4 \r\n" // my ip match
   "10.207.=yunhen-test-CM3\r\n"
   "  10.238.=yunhen-test-CM4\r\n";

const char* CM3_right=
   "{\r\n\"clusters\":[\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.14:15000\",\r\n\"slave\":  \"10.232.4.14:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"1\",\r\n\"writeWeight\":\"1\"\r\n},\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.16:15000\",\r\n\"slave\":  \"10.232.4.16:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"1\",\r\n\"writeWeight\":\"2\"\r\n}\r\n],\r\n\"localcache\":[\r\n{\"ns\":\"205\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"206\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"207\", \"cap\":\"30\", \"ttl\":\"300\"},\r\n{\"ns\":\"208\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"1\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"3\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"6\", \"cap\":\"5000\", \"ttl\":\"300\"}\r\n]\r\n}\r\n";

const char* CM3_right_another=
   "{\r\n\"clusters\":[\r\n{\r\n\"address\"  :\r\n{\r\n\"master\": \"10.232.4.14:15000\",\r\n\"slave\":  \"10.232.4.14:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"1\",\r\n\"writeWeight\":\"1\"\r\n},\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.16:15000\",\r\n\"slave\":  \"10.232.4.16:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"1\",\r\n\"writeWeight\":\"2\"\r\n}\r\n],\r\n\"localcache\":[\r\n{\"ns\":\"205\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"206\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"207\", \"cap\":\"30\", \"ttl\":\"300\"},\r\n{\"ns\":\"208\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"1\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"3\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"6\", \"cap\":\"5000\", \"ttl\":\"300\"}\r\n]\r\n}\r\n";

const char* CM3_wrong_json_format=
   "{\r\n\"clusters\":[\r\n{\r\n\"address:\r\n{\r\n\"master\": \"10.232.4.14:15000\",\r\n\"slave\":  \"10.232.4.14:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"1\",\r\n\"writeWeight\":\"1\"\r\n},\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.16:15000\",\r\n\"slave\":  \"10.232.4.16:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"1\",\r\n\"writeWeight\":\"2\"\r\n}\r\n],\r\n\"localcache\":[\r\n{\"ns\":\"205\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"206\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"207\", \"cap\":\"30\", \"ttl\":\"300\"},\r\n{\"ns\":\"208\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"1\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"3\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"6\", \"cap\":\"5000\", \"ttl\":\"300\"}\r\n]\r\n}\r\n";

const char* CM3_wrong_spell=
   "{\r\n\"clusters\":[\r\n{\r\n\"addrass\":\r\n{\r\n\"master\": \"10.232.4.14:15000\",\r\n\"slave\":  \"10.232.4.14:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"1\",\r\n\"writeWeight\":\"1\"\r\n},\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.16:15000\",\r\n\"slave\":  \"10.232.4.16:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"1\",\r\n\"writeWeight\":\"2\"\r\n}\r\n],\r\n\"localcache\":[\r\n{\"ns\":\"205\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"206\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"207\", \"cap\":\"30\", \"ttl\":\"300\"},\r\n{\"ns\":\"208\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"1\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"3\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"6\", \"cap\":\"5000\", \"ttl\":\"300\"}\r\n]\r\n}\r\n";


const char* CM3_wrong_weight1=
   "{\r\n\"clusters\":[\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.14:15000\",\r\n\"slave\":  \"10.232.4.14:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"-1\",\r\n\"writeWeight\":\"1\"\r\n},\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.16:15000\",\r\n\"slave\":  \"10.232.4.16:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"1\",\r\n\"writeWeight\":\"2\"\r\n}\r\n],\r\n\"localcache\":[\r\n{\"ns\":\"205\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"206\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"207\", \"cap\":\"30\", \"ttl\":\"300\"},\r\n{\"ns\":\"208\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"1\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"3\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"6\", \"cap\":\"5000\", \"ttl\":\"300\"}\r\n]\r\n}\r\n";

const char* CM3_wrong_weight2=
   "{\r\n\"clusters\":[\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.14:15000\",\r\n\"slave\":  \"10.232.4.14:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"1\"\r\n},\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.16:15000\",\r\n\"slave\":  \"10.232.4.16:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"1\",\r\n\"writeWeight\":\"2\"\r\n}\r\n],\r\n\"localcache\":[\r\n{\"ns\":\"205\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"206\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"207\", \"cap\":\"30\", \"ttl\":\"300\"},\r\n{\"ns\":\"208\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"1\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"3\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"6\", \"cap\":\"5000\", \"ttl\":\"300\"}\r\n]\r\n}\r\n";


const char* CM4_right=
   "{\r\n\"clusters\":[\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.17:15000\",\r\n\"slave\":  \"10.232.4.17:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"3\",\r\n\"writeWeight\":\"1\"\r\n},\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.18:15000\",\r\n\"slave\":  \"10.232.4.18:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"1\",\r\n\"writeWeight\":\"2\"\r\n},\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.20:15000\",\r\n\"slave\":  \"10.232.4.20:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"2\",\r\n\"writeWeight\":\"5\"\r\n}\r\n\r\n],\r\n\"localcache\":[\r\n{\"ns\":\"205\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"206\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"207\", \"cap\":\"30\", \"ttl\":\"300\"},\r\n{\"ns\":\"208\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"1\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"3\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"6\", \"cap\":\"5000\", \"ttl\":\"300\"}\r\n]\r\n}\r\n";

const char* CM4_right_another=
   "{\r\n\"clusters\":[\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.17:15000\",\r\n\"slave\":  \"10.232.4.17:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\"  :\"3\",\r\n\"writeWeight\":\"1\"\r\n},\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.18:15000\",\r\n\"slave\":  \"10.232.4.18:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"1\",\r\n\"writeWeight\":\"2\"\r\n},\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.20:15000\",\r\n\"slave\":  \"10.232.4.20:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"2\",\r\n\"writeWeight\":\"5\"\r\n}\r\n\r\n],\r\n\"localcache\":[\r\n{\"ns\":\"205\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"206\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"207\", \"cap\":\"30\", \"ttl\":\"300\"},\r\n{\"ns\":\"208\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"1\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"3\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"6\", \"cap\":\"5000\", \"ttl\":\"300\"}\r\n]\r\n}\r\n";

const char* CM4_wrong=
   "{\r\n\"clusters\":[\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.17:15000\",\r\n\"slave\":  \"10.232.4.17:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"3\",\r\n\"witeWeight\":\"1\"\r\n},\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.18:15000\",\r\n\"slave\":  \"10.232.4.18:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"1\",\r\n\"writeWeight\":\"2\"\r\n},\r\n{\r\n\"address\":\r\n{\r\n\"master\": \"10.232.4.20:15000\",\r\n\"slave\":  \"10.232.4.20:15000\",\r\n\"group\": \"group_1\"\r\n},\r\n\"readWeight\":\"2\",\r\n\"writeWeight\":\"5\"\r\n}\r\n\r\n],\r\n\"localcache\":[\r\n{\"ns\":\"205\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"206\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"207\", \"cap\":\"30\", \"ttl\":\"300\"},\r\n{\"ns\":\"208\", \"cap\":\"100\", \"ttl\":\"300\"},\r\n{\"ns\":\"1\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"3\", \"cap\":\"300\", \"ttl\":\"300\"},\r\n{\"ns\":\"6\", \"cap\":\"5000\", \"ttl\":\"300\"}\r\n]\r\n}\r\n";
