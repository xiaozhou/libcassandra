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
	tr1::shared_ptr<Cassandra> client(factory.create());

/*	string clus_name= client->getClusterName();
	cout << "cluster name: " << clus_name << endl;

	vector<KeyspaceDefinition> key_out= client->getKeyspaces();
	for (vector<KeyspaceDefinition>::iterator it = key_out.begin(); it != key_out.end(); ++it)
	{
	  cout << "keyspace: " << (*it).getName() << endl;
	}
*/
	try
	{
//	  KeyspaceDefinition ks_def;
//	  ks_def.setName("TEST");
//	  client->createKeyspace(ks_def);

/*	  ColumnFamilyDefinition cf_def;
	  cf_def.setName("Users");
	  cf_def.setKeyspaceName("TEST");

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
*/
	  client->setKeyspace("TEST");
//	  client->createColumnFamily(cf_def);
	  client->insertColumn("bsanderson", "Users", "full_name", "Brandon Sanderson");
	  client->insertColumn("bsanderson", "Users", "birth_date", 1975);
	  client->insertColumn("bsanderson", "Users", "state", "UT");
	  client->insertColumn("prothfuss", "Users", "full_name", "Patrick Rothfuss");
	  client->insertColumn("prothfuss", "Users", "birth_date", 1973);
	  client->insertColumn("prothfuss", "Users", "state", "WI");
	  client->insertColumn("htayler", "Users", "full_name", "Howard Tayler");
	  client->insertColumn("htayler", "Users", "birth_date", 1968);
	  client->insertColumn("htayler", "Users", "state", "UT");
	  client->insertColumn("htayler", "Users", "etest", "e");
	  client->insertColumn("htayler", "Users", "xtest", "x");
	  client->insertColumn("htayler", "Users", "ztest", "z");
	  IndexedSlicesQuery query;
	  vector<string> column_names;
	  column_names.push_back("full_name");
	  column_names.push_back("birth_date");
	  column_names.push_back("state");
	  query.setColumns(column_names);
	  query.addGtExpression("birth_date", 1970);
	  query.addEqualsExpression("state", "UT");
	  query.setColumnFamily("Users");
	  map<string, map<string, string> > res= client->getIndexedSlices(query);

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

	  ColumnParent col_parent;
	  col_parent.column_family="Users";
	  SlicePredicate pred;
	  SliceRange srange;
	  srange.start="etest";
	  srange.finish="ztest";
	  srange.reversed=false;
	  srange.count=100;
	  pred.slice_range=srange;
	  vector<Column> slice=client->getSliceRange("htayler",col_parent,pred);
	  for (vector<Column>::iterator pos=slice.begin();pos!=slice.end();++pos){
		  cout<<pos->name<<": "<<pos->value<<endl;
	  }

//	  client->dropKeyspace("TEST");
	}
	catch (org::apache::cassandra::InvalidRequestException &ire)
	{
	  cout << ire.why << endl;
	  return 1;
	}

	return 0;
}
