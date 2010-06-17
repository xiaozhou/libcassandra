#include <string.h>
#include <sstream>
#include <iostream>
#include <stdlib.h>
#include <set>
#include <string>
#include <stdio.h>

#include <libcassandra/cassandra_factory.h>
#include <libcassandra/cassandra.h>
#include <libcassandra/keyspace.h>

using namespace std;
using namespace libcassandra;

static string host("127.0.0.1");
static int port= 9160;

void print_cluster_info(tr1::shared_ptr<Cassandra> client) {

  string clus_name= client->getClusterName();
  cout << "\tCluster name: " << clus_name << endl;

  set<string> key_out= client->getKeyspaces();
  for (set<string>::iterator it = key_out.begin(); it != key_out.end(); ++it)
  {
    cout << "\tKeyspace: " << *it << endl;
  }

  map<string, string> tokens= client->getTokenMap(false);
  for (map<string, string>::iterator it= tokens.begin();
      it != tokens.end();
      ++it)
  {
    cout << "\t" << it->first << " : " << it->second << endl;
  }

}

void crud_simple(tr1::shared_ptr<Cassandra> client) {

  Keyspace *key_space= client->getKeyspace("Keyspace1");
  string res;

  key_space->insertColumn("bob", "Standard1", "age", "20");
  res = key_space->getColumnValue("bob", "Standard1", "age");
  cout << "\tValue in column retrieved is: " << res << endl;

  key_space->insertColumn("bob", "Standard1", "age", "35");
  res = key_space->getColumnValue("bob", "Standard1", "age");
  cout << "\tValue in column retrieved is: " << res << endl;

  key_space->removeColumn("bob", "Standard1", "age");

}

void crud_super(tr1::shared_ptr<Cassandra> client) {

  Keyspace *key_space= client->getKeyspace("Keyspace1");
  string res;

  key_space->insertColumn("bob", "Super1", "attrs", "age", "20");
  res = key_space->getColumnValue("bob", "Super1", "attrs", "age");
  cout << "\tValue in column retrieved is: " << res << endl;

  key_space->insertColumn("bob", "Super1", "attrs", "age", "35");
  res = key_space->getColumnValue("bob", "Super1", "attrs", "age");
  cout << "\tValue in column retrieved is: " << res << endl;

  key_space->removeSuperColumn("bob", "Super1", "attrs");

}


void getcolumns_bykeys(tr1::shared_ptr<Cassandra> client) {

  Keyspace *key_space= client->getKeyspace("Keyspace1");

  key_space->insertColumn("bob", "Standard1", "age", "20");
  key_space->insertColumn("bob", "Standard1", "eyes", "blue");

  vector<string> keys;
  keys.push_back("age");
  keys.push_back("eyes");
  vector<org::apache::cassandra::Column> cols= key_space->getColumns("bob", "Standard1", keys);

  for (vector<org::apache::cassandra::Column>::iterator it = cols.begin(); it != cols.end(); ++it) {
	  cout << "\tGot column " << it->name << " = " << it->value << endl;
  }

}

void getsupercolumns_bykeys(tr1::shared_ptr<Cassandra> client) {

  Keyspace *key_space= client->getKeyspace("Keyspace1");

  key_space->insertColumn("bob", "Super1", "friend1", "name", "tim");
  key_space->insertColumn("bob", "Super1", "friend1", "age", "20");

  key_space->insertColumn("bob", "Super1", "friend2", "name", "mac");
  key_space->insertColumn("bob", "Super1", "friend2", "age", "30");

  key_space->insertColumn("bob", "Super1", "friend3", "name", "bud");
  key_space->insertColumn("bob", "Super1", "friend3", "age", "40");

  vector<string> keys;
  keys.push_back("friend1");
  keys.push_back("friend2");
  vector<org::apache::cassandra::SuperColumn> scols= key_space->getSuperColumns("bob", "Super1", keys);

  for (vector<org::apache::cassandra::SuperColumn>::iterator it = scols.begin(); it != scols.end(); ++it) {
	  cout << "\tGot super column " << it->name << endl;
	  for (vector<org::apache::cassandra::Column>::iterator it2 = it->columns.begin(); it2 != it->columns.end(); ++it2) {
		  cout << "\t\tChild column " << it2->name << " = " << it2->value << endl;
	  }
  }

}

void getsupercolumns_byrange(tr1::shared_ptr<Cassandra> client) {

  Keyspace *key_space= client->getKeyspace("Keyspace1");

  key_space->insertColumn("bob", "Super1", "friend1", "name", "tim");
  key_space->insertColumn("bob", "Super1", "friend1", "age", "20");

  key_space->insertColumn("bob", "Super1", "friend2", "name", "mac");
  key_space->insertColumn("bob", "Super1", "friend2", "age", "30");

  key_space->insertColumn("bob", "Super1", "friend3", "name", "bud");
  key_space->insertColumn("bob", "Super1", "friend3", "age", "40");

  org::apache::cassandra::SliceRange range;
  range.start.assign("friend2");
  range.finish.assign("friend3");
  vector<org::apache::cassandra::SuperColumn> scols= key_space->getSuperColumns("bob", "Super1", range);

  for (vector<org::apache::cassandra::SuperColumn>::iterator it = scols.begin(); it != scols.end(); ++it) {
	  cout << "\tGot super column " << it->name << endl;
	  for (vector<org::apache::cassandra::Column>::iterator it2 = it->columns.begin(); it2 != it->columns.end(); ++it2) {
		  cout << "\t\tChild column " << it2->name << " = " << it2->value << endl;
	  }
  }

}

void getcolumns_byrange(tr1::shared_ptr<Cassandra> client) {

  Keyspace *key_space= client->getKeyspace("Keyspace1");

  key_space->insertColumn("bob", "Standard1", "aaa1", "vaaa1");
  key_space->insertColumn("bob", "Standard1", "bbb1", "vbbb1");
  key_space->insertColumn("bob", "Standard1", "bbb2", "vbbb2");
  key_space->insertColumn("bob", "Standard1", "bbb3", "vbbb3");
  key_space->insertColumn("bob", "Standard1", "ccc1", "vccc1");

  org::apache::cassandra::SliceRange range;
  range.start.assign("bbb1");
  range.finish.assign("bbb3");
  vector<org::apache::cassandra::Column> cols= key_space->getColumns("bob", "Standard1", range);

  for (vector<org::apache::cassandra::Column>::iterator it = cols.begin(); it != cols.end(); ++it) {
	  cout << "\tGot column " << it->name << " = " << it->value << endl;
  }

}

int main() {

  CassandraFactory factory(host, port);
  tr1::shared_ptr<Cassandra> client(factory.create());

  cout << "Cluster Info:" << endl;
  print_cluster_info(client);
  cout << endl;

  cout << "Single CRUD:" << endl;
  crud_simple(client);
  cout << endl;

  cout << "SuperColumn CRUD:" << endl;
  crud_super(client);
  cout << endl;

  cout << "GetColumns by keys:" << endl;
  getcolumns_bykeys(client);
  cout << endl;

  cout << "GetColumns by range:" << endl;
  getcolumns_byrange(client);
  cout << endl;

  cout << "GetSuperColumns by keys:" << endl;
  getsupercolumns_bykeys(client);
  cout << endl;

  cout << "GetSuperColumns by range:" << endl;
  getsupercolumns_byrange(client);
  cout << endl;

  return 0;
}
