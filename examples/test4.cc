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
#include <libcassandra/indexed_slices_query.h>
#include <libgenthrift/Cassandra.h>
#include "libcassandra/util_functions.h"

using namespace std;
using namespace libcassandra;
using namespace org::apache::cassandra;

static string host("127.0.0.1");
static int port= 9160;

int main()
{
	CassandraFactory factory(host, port);
	tr1::shared_ptr<Cassandra> c(factory.create());

	string clus_name= c->getClusterName();
	cout << "cluster name: " << clus_name << endl;

	vector<KeyspaceDefinition> key_out= c->getKeyspaces();
	for (vector<KeyspaceDefinition>::iterator it = key_out.begin(); it != key_out.end(); ++it)
	{
	  cout << "keyspace: " << (*it).getName() << endl;
	}


	try
	{
		  KeyspaceDefinition ks_def;
		  ks_def.setName("unittest");
		  c->createKeyspace(ks_def);
		  ColumnFamilyDefinition cf_def;
		  cf_def.setName("users");
		  cf_def.setKeyspaceName(ks_def.getName());
		  ColumnDefinition name_col;
		  name_col.setName("full_name");
		  name_col.setValidationClass("UTF8Type");
		  ColumnDefinition sec_col;
		  sec_col.setName("birth_date");
		  sec_col.setValidationClass("LongType");
		  sec_col.setIndexType(IndexType::KEYS);
		  ColumnDefinition third_col;
		  third_col.setName("state");
		  third_col.setValidationClass("UTF8Type");
		  third_col.setIndexType(IndexType::KEYS);
		  cf_def.addColumnMetadata(name_col);
		  cf_def.addColumnMetadata(sec_col);
		  cf_def.addColumnMetadata(third_col);
		  c->setKeyspace(ks_def.getName());
		  c->createColumnFamily(cf_def);
		  c->insertColumn("bsanderson", cf_def.getName(), "full_name", "Brandon Sanderson");
		  c->insertColumn("bsanderson", cf_def.getName(), "birth_date", 1975);
		  c->insertColumn("bsanderson", cf_def.getName(), "state", "UT");
		  c->insertColumn("prothfuss", cf_def.getName(), "full_name", "Patrick Rothfuss");
		  c->insertColumn("prothfuss", cf_def.getName(), "birth_date", 1973);
		  c->insertColumn("prothfuss", cf_def.getName(), "state", "WI");
		  c->insertColumn("htayler", cf_def.getName(), "full_name", "Howard Tayler");
		  c->insertColumn("htayler", cf_def.getName(), "birth_date", 1968);
		  c->insertColumn("htayler", cf_def.getName(), "state", "UT");
		  IndexedSlicesQuery query;
		  vector<string> column_names;
		  column_names.push_back("full_name");
		  column_names.push_back("birth_date");
		  column_names.push_back("state");
		  query.setColumns(column_names);
		  query.addLtExpression("birth_date", 1970);
		  query.addEqualsExpression("state", "UT");
		  query.setColumnFamily("users");
		  map<string, map<string, string> > res= c->getIndexedSlices(query);

		  map<string, map<string, string> >::iterator pos;
		  for (pos = res.begin(); pos!=res.end();++pos){
			  cout<< pos->first<<endl;
			  map<string, string> res2=pos->second;
			  for (map<string, string>::iterator pos2 = res2.begin(); pos2!=res2.end();++pos2){
				  cout<< pos2->first<<" ";
				  if(pos2->first=="birth_date")
					  cout<< deserializeLong(pos2->second)<<endl;
				  else
					  cout<< pos2->second<<endl;
			  }

		  }
		  c->dropKeyspace("unittest");
	}
	catch (org::apache::cassandra::InvalidRequestException &ire)
	{
	  cout << ire.why << endl;
	  return 1;
	}

	return 0;
}
