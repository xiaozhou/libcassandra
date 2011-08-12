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
static int port= 10051;

int main()
{
	CassandraFactory factory(host, port);
	boost::shared_ptr<Cassandra> client(factory.create());

	try
	{

	  client->setKeyspace("sirikata");

	  std::vector<ColumnTuple> CLT;

	  ColumnTuple t2("persistence","key_1","a","abcde",false);
	  ColumnTuple t3("persistence","key_1","k","klmno",false);

	  CLT.push_back(t2);
	  CLT.push_back(t3);

	  client->batchMutate(CLT);
	  string res= client->getColumnValue("key_1", "Persistence", "a");
	  cout << "value: " <<res<<endl;
	  res= client->getColumnValue("key_1", "Persistence", "k");
	  cout << "value: " <<res<<endl;


	  ColumnTuple t4("persistence","key_1","a","xxxx",false);
	  ColumnTuple t6("persistence","key_1","k","yyyy",false);

	  CLT.clear();
	  CLT.push_back(t4);
	  CLT.push_back(t6);

	  client->batchMutate(CLT);
	  res= client->getColumnValue("key_1", "persistence", "a");
	  cout << "value: " <<res<<endl;
	  res= client->getColumnValue("key_1", "persistence", "k");
	  cout << "value: " <<res<<endl;
	}
    catch (org::apache::cassandra::NotFoundException &ire)
	  {
	    cout <<"NotFoundException Caught" << endl;
	    return 1;
	  }
	catch (org::apache::cassandra::InvalidRequestException &ire)
	  {
	    cout << ire.why << endl;
	    return 1;
	  }W


	return 0;
}
