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

int main()
{
	CassandraFactory factory(host, port);
	tr1::shared_ptr<Cassandra> client(factory.create());
       
       	try
       	{
	  client->setKeyspace("system");
	  /*	  	  	  	  	  
	  ColumnFamilyDefinition cf_def;
          cf_def.setName("PersistenceStorage");
          cf_def.setKeyspaceName("system");
          client->createColumnFamily(cf_def);
	  */
          client->insertColumn("72a537a6c18f48fea97d90b40727062e", "PersistenceStorage", "a", "abcde");
 
	  /* retrieve that data */
	  string res= client->getColumnValue("72a537a6c18f48fea97d90b40727062e", "PersistenceStorage", "a");
	  cout << "value: " <<res<<endl;
      	  client->remove("72a537a6c18f48fea97d90b40727062e", "PersistenceStorage", "", "a");
	  res= client->getColumnValue("72a537a6c18f48fea97d90b40727062e", "PersistenceStorage", "a");
 	  cout << "value: " <<res<<endl;
       	}
	catch (org::apache::cassandra::NotFoundException &ire)
	{
	  cout <<"Catch Exception: org::apache::cassandra::NotFoundException" << endl;
	  return 1;
	}

	return 0;
}
