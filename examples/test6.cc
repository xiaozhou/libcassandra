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

	int data_size = 10000;
	string DATA_="@";
	for (int i=0; i<99; i++)
	{
	  DATA_=DATA_+"@";
	}
	string DATA=DATA_;
	for (int i=0; i<data_size/100-1; i++)
	{
	  DATA=DATA+DATA_;
	}

	CassandraFactory factory(host, port);
	tr1::shared_ptr<Cassandra> client(factory.create());

	try
	{
	  int RW_Num = 10000;
	  cout<<"data size: "<<data_size<<endl;
	  cout<<"rw number: "<<RW_Num<<endl;


	  client->setKeyspace("TEST");
	  client->dropColumnFamily("T6");
	  /* create standard column family */
	  ColumnFamilyDefinition cf_def;
	  cf_def.setName("T6");
	  cf_def.setKeyspaceName("TEST");

	  ColumnDefinition rank_col;
	  rank_col.setName("rank");
	  rank_col.setValidationClass("LongType");
	  rank_col.setIndexType(IndexType::KEYS);
	  ColumnDefinition status_col;
	  status_col.setName("status");
	  status_col.setValidationClass("UTF8Type");
	  status_col.setIndexType(IndexType::KEYS);
	  cf_def.addColumnMetadata(rank_col);
	  cf_def.addColumnMetadata(status_col);

	  client->createColumnFamily(cf_def);
/*
	  client->insertColumn("key_1", "T6", "rank", 1);
	  client->insertColumn("key_2", "T6", "rank", 3);
	  client->insertColumn("key_3", "T6", "rank", 5);
	  client->insertColumn("key_1", "T6", "status", "ON");
	  client->insertColumn("key_2", "T6", "status", "OFF");
	  client->insertColumn("key_3", "T6", "status", "ON");
*/
	  timeval ts;
	  gettimeofday(&ts,NULL);
	  long int time1_s = ts.tv_sec;
	  int time1_us=ts.tv_usec;

	  for (int i=0; i<=RW_Num; i++){
	    /* insert data */
		ostringstream col;
		col<<i;
		client->insertColumn(col.str(), "T6", "rank", i);
		client->insertColumn(col.str(), "T6", "status", "ON");
//		client->insertColumn(col.str(), "T6", "data", DATA);
	    client->insertColumn(col.str(), "T6", col.str(), DATA);
	  }

	  gettimeofday(&ts,NULL);
	  long int time2_s = ts.tv_sec;
	  int time2_us=ts.tv_usec;
	  long int diff_s=time2_s - time1_s;
	  int diff_us=time2_us - time1_us;
	  double diff_t=diff_s+diff_us/(double)1000000;
	  cout<<"write time: "<<diff_t<<endl;

	  IndexedSlicesQuery query;
	  query.setRange("0","z",false,100000);
	  query.addGtExpression("rank", 0);
	  query.addEqualsExpression("status", "ON");
	  query.setColumnFamily("T6");
	  map<string, map<string, string> > res= client->getIndexedSlices(query);
/*    map<string, map<string, string> >::iterator pos;
  	  for (pos = res.begin(); pos!=res.end();++pos){
	  	  cout<<"\nkey: "<<pos->first<<endl;
  		  map<string, string> res2=pos->second;
  		  for (map<string, string>::iterator pos2 = res2.begin(); pos2!=res2.end();++pos2){
  			if(pos2->first.length()>0)
  			{
  				cout<< pos2->first<<": ";
  				if(pos2->first=="rank")
  					cout<< deserializeLong(pos2->second)<<endl;
  				else
  					cout<< pos2->second<<endl;
  			}
  		  }
  	  }
*/
	  gettimeofday(&ts,NULL);
	  long int time3_s = ts.tv_sec;
	  int time3_us=ts.tv_usec;
	  diff_s=time3_s - time2_s;
	  diff_us=time3_us - time2_us;
	  diff_t=diff_s+diff_us/(double)1000000;
	  cout<<"read time: "<<diff_t<<endl;

/*
	  ColumnParent col_parent;
	  col_parent.column_family="T6";
	  SlicePredicate pred;
	  SliceRange srange;
	  srange.start="1_col";
	  srange.finish="2_col";
	  srange.reversed=false;
	  srange.count=100;
	  pred.slice_range=srange;
	  vector<Column> slice=client->getSliceRange("1_key",col_parent,pred);
	  for (vector<Column>::iterator pos=slice.begin();pos!=slice.end();++pos){
		  cout<<pos->name<<": "<<pos->value<<endl;
	  }
*/
	}
	catch (org::apache::cassandra::InvalidRequestException &ire)
	{
	  cout << ire.why << endl;
	  return 1;
	}

	return 0;
}



