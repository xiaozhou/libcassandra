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
#include "libcassandra/util_functions.h"

using namespace std;
using namespace libcassandra;
using namespace org::apache::cassandra;

static string host("localhost");
static int port= 9160;

int main()
{
  CassandraFactory factory(host, port);
  boost::shared_ptr<Cassandra> client(factory.create());
    
  typedef std::tr1::unordered_map<string, string> ReadSet;
    
  string key = "key_1";
    
  try {
    client->setKeyspace("sirikata");
        
    vector<Column> Columns;
    vector<string> Names;

    Column C1;
    C1.name  = "map:name:a";
    C1.value = "abcd";
    C1.timestamp = createTimestamp();
        
    Column C2;
    C2.name  = "map:name:e";
    C2.value = "efgh";
    C2.timestamp = createTimestamp();
        
    Column C3;
    C3.name  = "map:name:i";
    C3.value = "ijkl";
    C3.timestamp = createTimestamp();
 
    Columns.push_back(C1);
    Columns.push_back(C2);
    Columns.push_back(C3);
        
    batchSuperColumnTuple tuple = batchSuperColumnTuple("persistence", key, "@", Columns, Names);       
    client->batchMutate(tuple);       
        
    Names.push_back("map:name:a");
    Names.push_back("map:name:e");
    ReadSet rs1 = client->getColumnsValues(key, "persistence", "@", Names);
    cout<<"Read-------------"<<endl;
    cout<<"a: "<<rs1["map:name:a"]<<endl;
    cout<<"e: "<<rs1["map:name:e"]<<endl;

    vector<Column> cols;
    SliceRange range;
    range.start="map:name";
    range.finish="map:name@";
    range.count=100;

    ReadSet rs2 = client->getColumnsValues(key, "persistence", "@", range);

    for (ReadSet::iterator it=rs2.begin(); it!=rs2.end(); it++) {
    	string key = it->first; string value = it->second;
    	cout<<key<<" == "<<value<<endl;
    }
    /*
    cols = client->getColumns(key, "persistence", "@", range);
    cout<<"Read ALL---------"<<endl;
    for (vector<Column>::iterator it = cols.begin(); it!=cols.end();it++) {
      cout<<(*it).name<<" -- "<<(*it).value<<endl;
    }*/

    ColumnParent col_parent;
    col_parent.__isset.super_column=true;
    col_parent.column_family="persistence";
    col_parent.super_column="@";

    SlicePredicate predicate;
    predicate.__isset.slice_range=true;
    predicate.slice_range=range;

    cout<<"Count-------------"<<endl;
    int32_t count = client->getCount(key, col_parent, predicate);
    cout<<"count: "<<count<<endl;
        
  }
  catch (org::apache::cassandra::NotFoundException &ire) {
    cout <<"NotFoundException Caught" << endl;
    return 1;
  }
  catch (org::apache::cassandra::InvalidRequestException &ire) {
    cout << ire.why << endl;
    return 1;
  }
    
  return 0;
}
