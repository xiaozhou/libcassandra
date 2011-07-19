#include <string.h>
#include <sstream>
#include <iostream>
#include <stdlib.h>
#include <set>
#include <string>
#include <stdio.h>
#include <sys/time.h>

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
	int data_size = 10;
	string DATA_="@";
	for (int i=0; i<0; i++)
	{
	  DATA_=DATA_+"@";
	}
	string DATA=DATA_;
	for (int i=0; i<data_size-1; i++)
	{
	  DATA=DATA+DATA_;
	}


	CassandraFactory factory(host, port);
	tr1::shared_ptr<Cassandra> client(factory.create());

	string clus_name= client->getClusterName();

	try
	{
	  int RW_Num = 10000;
	  cout<<"data size: "<<data_size<<endl;
	  cout<<"rw number: "<<RW_Num<<endl;

	  client->setKeyspace("SNS");
	  client->dropColumnFamily("People");
	  ColumnFamilyDefinition cf_def;
	  cf_def.setName("People");
	  cf_def.setKeyspaceName("SNS");
	  client->createColumnFamily(cf_def);

	  timeval ts;
	  gettimeofday(&ts,NULL);
	  long int time1_s = ts.tv_sec;
	  int time1_us=ts.tv_usec;

	  for (int i=0; i<=RW_Num; i++){
	    /* insert data */
		ostringstream col;
		col<<i;
	    client->insertColumn(col.str(), "People", col.str(), DATA);
	  }

	  gettimeofday(&ts,NULL);
	  long int time2_s = ts.tv_sec;
	  int time2_us=ts.tv_usec;
	  long int diff_s=time2_s - time1_s;
	  int diff_us=time2_us - time1_us;
	  double diff_t=diff_s+diff_us/(double)1000000;
	  cout<<"write time: "<<diff_t<<endl;

	  string res;
	  for (int i=0; i<=RW_Num; i++){
	    /* retrieve that data */
		ostringstream col;
		col<<i;
	    res= client->getColumnValue(col.str(), "People", col.str());
	  }
	  cout<<res<<endl;

	  gettimeofday(&ts,NULL);
	  long int time3_s = ts.tv_sec;
	  int time3_us=ts.tv_usec;
	  diff_s=time3_s - time2_s;
	  diff_us=time3_us - time2_us;
	  diff_t=diff_s+diff_us/(double)1000000;
	  cout<<"read time: "<<diff_t<<endl;
	  client->dropColumnFamily("People");
	}
	catch (org::apache::cassandra::InvalidRequestException &ire)
	{
	  cout << ire.why << endl;
	  return 1;
	}

	return 0;
}
