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
#include <libgenthrift/Cassandra.h>

using namespace std;
using namespace libcassandra;
using namespace org::apache::cassandra;

static string host("localhost");
static int port= 10051;

int main()
{
	CassandraFactory factory(host, port);
	tr1::shared_ptr<Cassandra> client(factory.create());
	typedef std::tr1::unordered_map<string, string> ReadSet;

	try
	{

	  client->setKeyspace("sirikata");

	  /* create standard column family */
/*	  ColumnFamilyDefinition cf_def;
	  cf_def.setName("Object_test");
	  cf_def.setColumnType("Super");
	  cf_def.setKeyspaceName("sirikata");
	  client->createColumnFamily(cf_def);
*/
	  std::vector<SuperColumnTuple> SupColTuple;

	  SuperColumnTuple t1("Object_test","host_1","object_1","script_type","type_data_1",false);
	  SuperColumnTuple t2("Object_test","host_1","object_1","script_args","args_data_1",false);
	  SuperColumnTuple t3("Object_test","host_1","object_1","script_contents","contents_data_1",false);
	  SuperColumnTuple t4("Object_test","host_1","object_2","script_type","type_data_2",false);
	  SuperColumnTuple t5("Object_test","host_1","object_2","script_args","args_data_2",false);
	  SuperColumnTuple t6("Object_test","host_1","object_2","script_contents","contents_data_2",false);
	  SuperColumnTuple t7("Object_test","host_1","object_3","script_type","type_data_3",false);
	  SuperColumnTuple t8("Object_test","host_1","object_3","script_args","args_data_3",false);
	  SuperColumnTuple t9("Object_test","host_1","object_3","script_contents","contents_data_3",false);

	  SupColTuple.push_back(t1);
	  SupColTuple.push_back(t2);
	  SupColTuple.push_back(t3);
	  SupColTuple.push_back(t4);
	  SupColTuple.push_back(t5);
	  SupColTuple.push_back(t6);
	  SupColTuple.push_back(t7);
	  SupColTuple.push_back(t8);
	  SupColTuple.push_back(t9);

	  client->batchMutate(SupColTuple);

	  std::vector<string> ColumnNames;
	  ColumnNames.push_back("script_type");
	  ColumnNames.push_back("script_args");
	  ColumnNames.push_back("script_contents");
/*	  ReadSet rs = client->getColumnsValues("host_1", "Object_test", "object_1", ColumnNames);
	  cout<<"script_type: "<<rs["script_type"]<<endl;
      cout<<"script_args: "<<rs["script_args"]<<endl;
	  cout<<"script_contents: "<<rs["script_contents"]<<endl;
*/
	  SliceRange range;
	  range.count=1000000;
	  vector<SuperColumn> supCols = client->getSuperColumns("host_1", "Object_test", range);
	  for (vector<SuperColumn>::iterator it= supCols.begin(); it != supCols.end(); ++it)
	  {
		  cout<<"\nObject ID: "<<(*it).name<<endl;
/*		  vector<Column> columns = (*it).columns;
		  for (vector<Column>::iterator pos= columns.begin(); pos != columns.end(); ++pos)
		  {
			  cout<<(*pos).name<<": "<<(*pos).value<<endl;
		  }
*/		  cout<<(*it).columns[0].name<<":- "<<(*it).columns[0].value<<endl;
		  cout<<(*it).columns[1].name<<":- "<<(*it).columns[1].value<<endl;
		  cout<<(*it).columns[2].name<<":- "<<(*it).columns[2].value<<endl;
//		  string res((*it).columns[2].value);
//		  cout<<res<<endl;
	  }


	  SupColTuple.clear();

	  SuperColumnTuple t10("Object_test","host_1","object_1","","",true);
	  SupColTuple.push_back(t10);

	  client->batchMutate(SupColTuple);

	  ReadSet rs = client->getColumnsValues("host_1", "Object_test", "object_1", ColumnNames);
	  cout<<"script_type: "<<rs["script_type"]<<endl;
	  cout<<"script_args: "<<rs["script_args"]<<endl;
	  cout<<"script_contents: "<<rs["script_contents"]<<endl;
	}
    catch (org::apache::cassandra::NotFoundException &ire)
	  {
	    cout <<"NotFoundException Caught" << endl;
	    return 1;
	  }
	catch (org::apache::cassandra::InvalidRequestException &ire)
	  {
	    cout << "InvalidRequestException" << endl;
	    return 1;
	  }

	return 0;
}
