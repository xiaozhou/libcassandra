#include <string.h>
#include <sstream>
#include <iostream>
#include <stdlib.h>
#include <set>
#include <string>
#include <stdio.h>

#include <libcassandra/cassandra_factory.h>
#include <libcassandra/cassandra.h>
#include <libcassandra/column_family_definition.h>
#include <libcassandra/keyspace.h>
#include <libcassandra/keyspace_definition.h>

using namespace std;
using namespace libcassandra;

static string host("localhost");
static int port= 9160;

#define KS_NAME     "sirikata"
#define CF_NAME_PER "persistence"
#define CF_NAME_OBJ "objects"

int main()
{
  CassandraFactory factory(host, port);
  boost::shared_ptr<Cassandra> client(factory.create());

  std::string objectHost = "oh_test";

  try {
    client->setKeyspace("sirikata");
    client->remove(objectHost, CF_NAME_OBJ, "", "");
  }
  catch (org::apache::cassandra::NotFoundException &ire) {
    cout <<"NotFoundException Caught" << endl;
  }
  catch (org::apache::cassandra::InvalidRequestException &ire) {
    cout << ire.why << endl;
  }
	
  return 0;
}
