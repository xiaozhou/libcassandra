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
	  /* create keyspace */
	  KeyspaceDefinition ks_def;
	  ks_def.setName("SNS");
	  client->createKeyspace(ks_def);
	  client->setKeyspace(ks_def.getName());

	  /* create standard column family */
	  ColumnFamilyDefinition cf_def;
	  cf_def.setName("People");
	  cf_def.setKeyspaceName(ks_def.getName());
	  client->createColumnFamily(cf_def);

	  /* insert data */
	  client->insertColumn("xl", "People", "First", "Xiaozhou");
	  client->insertColumn("xl", "People", "Last", "Li");
	  client->insertColumn("xl", "People", "Email", "xl@princeton.edu");
	  client->insertColumn("xl", "People", "Status", "2nd year PhD");
	  /* retrieve that data */
	  string res_first= client->getColumnValue("xl", "People", "First");
	  string res_last= client->getColumnValue("xl", "People", "Last");
	  cout << "Name of xl is: " << res_first<<' '<<res_last<<endl;
	}
	catch (org::apache::cassandra::InvalidRequestException &ire)
	{
	  cout << ire.why << endl;
	  return 1;
	}

	return 0;
}
