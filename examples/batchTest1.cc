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
  boost::shared_ptr<Cassandra> client(factory.create());
  
  typedef std::tr1::unordered_map<string, string> ReadSet;
  bool write = false;
  bool erase = true;

  try {
    client->setKeyspace("sirikata");

    vector<SuperColumnTuple> tuples;
    vector<string> names;

    SuperColumnTuple t1("persistence","key_1","@","a","abcde",write);
    SuperColumnTuple t2("persistence","key_1","@","f","fghij",write);
    SuperColumnTuple t3("persistence","key_1","@","k","klmno",write);

    tuples.push_back(t1);
    tuples.push_back(t2);
    tuples.push_back(t3);   
    client->batchMutate(tuples);
    tuples.clear();
    
    names.push_back("a");
    names.push_back("f");
    names.push_back("k");

    cout<<"-------------"<<endl;   
    ReadSet rs = client->getColumnsValues("key_1", "persistence","@",names);    
    cout<<"a: "<<rs["a"]<<endl;
    cout<<"f: "<<rs["f"]<<endl;
    cout<<"k: "<<rs["k"]<<endl;

    t1 = SuperColumnTuple("persistence","key_1","@","a","",erase);
    t2 = SuperColumnTuple("persistence","key_1","@","f","",erase);
    t3 = SuperColumnTuple("persistence","key_1","@","k","kkkkk",write);
    
    tuples.push_back(t1);
    tuples.push_back(t2);
    tuples.push_back(t3);   
    client->batchMutate(tuples);
    tuples.clear();
    
    cout<<"-------------"<<endl;   
    string ret=client->getColumnValue("key_1", "persistence","@","k");
    cout<<"k: "<<ret<<endl;
    
    cout<<"-------------"<<endl;   
    rs = client->getColumnsValues("key_1", "persistence","@",names);
    
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
