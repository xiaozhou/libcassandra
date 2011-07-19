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

static string host("127.0.0.1");
static int port= 9160;

int main()
{
	CassandraFactory factory(host, port);
	tr1::shared_ptr<Cassandra> client(factory.create());

	string clus_name= client->getClusterName();
	cout << "cluster name: " << clus_name << endl;

	vector<KeyspaceDefinition> key_out= client->getKeyspaces();
	for (vector<KeyspaceDefinition>::iterator it = key_out.begin(); it != key_out.end(); ++it)
	{
	  cout << "keyspace: " << (*it).getName() << endl;
	}

	try
	{
		const string mock_data("this is mock data being inserted...");

	  client->setKeyspace("SNS");
	  ColumnFamilyDefinition cf_def;
	  cf_def.setName("Super1");
	  cf_def.setColumnType("Super");
	  cf_def.setKeyspaceName("SNS");
	  client->createColumnFamily(cf_def);

	  client->insertColumn("teeny", "Super1", "padraig", "third", mock_data);
	  string res= client->getColumnValue("teeny", "Super1", "padraig", "third");
	  cout<<res<<endl;

	}
	catch (org::apache::cassandra::InvalidRequestException &ire)
	{
	  cout << ire.why << endl;
	  return 1;
	}

	return 0;
}
