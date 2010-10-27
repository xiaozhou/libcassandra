/*
 * LibCassandra
 * Copyright (C) 2010 Padraig O'Sullivan
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#include <string>
#include <set>
#include <sstream>

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include <libgenthrift/Cassandra.h>

#include "cassandra.h"
#include "cassandra_factory.h"

using namespace libcassandra;
using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace org::apache::cassandra;
using namespace boost;


CassandraFactory::CassandraFactory(const string &server_list)
  :
    host(),
    port(0),
    conn_timeout(-1),
    recv_timeout(-1),
    send_timeout(-1)
{
  /* get the host name from the server list string */
  string::size_type pos= server_list.find_first_of(':');
  host= server_list.substr(0, pos);
  /* get the port from the server list string */
  string tmp_port= server_list.substr(pos + 1);
  /* convert to integer */
  istringstream int_stream(tmp_port);
  int_stream >> port;
}

CassandraFactory::CassandraFactory(const string &in_host, int in_port)
  :
    host(in_host),
    port(in_port),
    conn_timeout(-1),
    recv_timeout(-1),
    send_timeout(-1)
{}

CassandraFactory::CassandraFactory(const string &in_host, int in_port, int in_timeout)
  :
    host(in_host),
    port(in_port),
    conn_timeout(in_timeout),
    recv_timeout(in_timeout),
    send_timeout(in_timeout)
{}

CassandraFactory::~CassandraFactory() {}


tr1::shared_ptr<Cassandra> CassandraFactory::create()
{
  /* Default is Buffered Transport */
  return create(0);
}

tr1::shared_ptr<Cassandra> CassandraFactory::create(int framed_transport)
{
  CassandraClient *thrift_client= createThriftClient(host, port, conn_timeout, recv_timeout, send_timeout, framed_transport);
  tr1::shared_ptr<Cassandra> ret(new Cassandra(thrift_client, host, port));
  return ret;
}

CassandraClient *CassandraFactory::createThriftClient(const string &in_host,
                                                      int in_port, 
                                                      int in_conn_timeout,
                                                      int in_recv_timeout,
                                                      int in_send_timeout,
                                                      int framed_transport)
{
  boost::shared_ptr<TSocket> socket(new TSocket(in_host, in_port));
  boost::shared_ptr<TTransport> transport;

  if (in_conn_timeout >= 0) {
    socket->setConnTimeout(in_conn_timeout);
  }
  if (in_recv_timeout >= 0) {
    socket->setRecvTimeout(in_recv_timeout);
  }
  if (in_send_timeout >= 0) {
    socket->setSendTimeout(in_send_timeout);
  }

  if (framed_transport) 
  {
    transport= boost::shared_ptr<TTransport> (new TFramedTransport(socket));
  }
  else
  {
    transport= boost::shared_ptr<TTransport> (new TBufferedTransport(socket));
  }
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  CassandraClient *client= new(std::nothrow) CassandraClient(protocol);

  transport->open(); /* throws an exception */

  return client;
}

