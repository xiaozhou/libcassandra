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

  string clus_name= client->getClusterName();
  cout << "cluster name: " << clus_name << endl;

  vector<KeyspaceDefinition> key_out= client->getKeyspaces();
  for (vector<KeyspaceDefinition>::iterator it = key_out.begin(); it != key_out.end(); ++it) {
    cout << "keyspace: " << (*it).getName() << endl;
  }

  try {
    // create keyspace
    KeyspaceDefinition ks_def:
    ks_def.setName("sirikata");
    client->createKeyspace(ks_def);
    client->setKeyspace("sirikata");

    ColumnFamilyDefinition cf_def_1;
    cf_def_1.setName("persistence");
    cf_def_1.setColumnType("Super");
    cf_def_1.setKeyspaceName("sirikata");
    client->createColumnFamily(cf_def_1);

    ColumnFamilyDefinition cf_def_2;
    cf_def_2.setName("objects");
    cf_def_2.setColumnType("Super");
    cf_def_2.setKeyspaceName("sirikata");
    client->createColumnFamily(cf_def_2);

    // insert data
    client->insertColumn("persistence_test", CF_NAME_PER, "sup", "a", "abcde");
    // retrieve that data
    string res= client->getColumnValue("persistence_test", CF_NAME_PER, "sup", "a");
    cout << "persistence -- read after insert: " <<res<<endl;
    // remove that data
    client->remove("persistence_test", CF_NAME_PER, "sup", "a");
    // retrieve that data again
    res= client->getColumnValue("persistence_test", CF_NAME_PER, "sup", "a");
    cout << "persistence -- read after erase: " <<res<<endl;

  }
  catch (org::apache::cassandra::NotFoundException &ire) {
    cout <<"NotFoundException Caught" << endl;
  }
  catch (org::apache::cassandra::InvalidRequestException &ire) {
    cout << ire.why << endl;
  }
  catch (...) {
    cout << "Other Exception Caught" << endl;
  }

  try {
    // insert data
    client->insertColumn("objects_test", CF_NAME_OBJ, "sup", "f", "fhijk");
    // retrieve that data
    string res= client->getColumnValue("objects_test", CF_NAME_OBJ, "sup", "f");
    cout << "objects -- read after insert: " <<res<<endl;
    // remove that data
    client->remove("objects_test", CF_NAME_OBJ, "sup", "f");
    // retrieve that data again
    res= client->getColumnValue("persistence_test", CF_NAME_OBJ, "sup", "f");
    cout << "objects -- read after erase: " <<res<<endl;
  }
  catch (org::apache::cassandra::NotFoundException &ire) {
    cout <<"NotFoundException Caught" << endl;
  }
  catch (org::apache::cassandra::InvalidRequestException &ire) {
    cout << ire.why << endl;
  }
  catch (...) {
    cout << "Other Exception Caught" << endl;
  }

  return 0:
}
