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
static int port= 10051;

int main()
{
	CassandraFactory factory(host, port);
	boost::shared_ptr<Cassandra> client(factory.create());

        typedef std::tr1::unordered_map<string, string> ReadSet;

        bool write = false;
        bool erase = true;

	try
	{

	  client->setKeyspace("sirikata");

	  std::vector<ColumnTuple> ColumnTuples;
          std::vector<string> ColumnNames;

          ColumnTuple t1("persistence","key_1","a","abcde",write);
	  ColumnTuple t2("persistence","key_1","f","fghij",write);
	  ColumnTuple t3("persistence","key_1","k","klmno",write);
	  ColumnTuple t4("persistence","key_1","p","pqrst",write);
	  ColumnTuple t5("persistence","key_1","u","uvwxy",write);

	  ColumnTuples.push_back(t1);
	  ColumnTuples.push_back(t2);
	  ColumnTuples.push_back(t3);
	  ColumnTuples.push_back(t4);
	  ColumnTuples.push_back(t5);

	  ColumnNames.push_back("a");
	  ColumnNames.push_back("f");
	  ColumnNames.push_back("k");
	  ColumnNames.push_back("p");
	  ColumnNames.push_back("u");

	  client->batchMutate(ColumnTuples);
	 
          //string res= client->getColumnValue("key_1", "persistence", "a");
	  //cout << "value: " <<res<<endl;
	  //res= client->getColumnValue("key_1", "persistence", "k");
	  //cout << "value: " <<res<<endl;
	  ColumnTuples.clear();
          
          //ColumnNames.push_back("k");     
          ReadSet rs = client->getColumnsValues("key_1", "persistence", ColumnNames);
          cout<<"a: "<<rs["a"]<<endl;
          cout<<"f: "<<rs["f"]<<endl;
          cout<<"k: "<<rs["k"]<<endl;
          cout<<"p: "<<rs["p"]<<endl;
          cout<<"u: "<<rs["u"]<<endl;

          t1 = ColumnTuple("persistence","key_1","a","",erase);
          t2 = ColumnTuple("persistence","key_1","f","",erase);
          t3 = ColumnTuple("persistence","key_1","k","",erase);
          //t4 = ColumnTuple("persistence","key_1","p","",erase);
          //t5 = ColumnTuple("persistence","key_1","u","",erase);

	  ColumnTuples.push_back(t1);
	  ColumnTuples.push_back(t2);
	  ColumnTuples.push_back(t3);
	  //ColumnTuples.push_back(t4);
	  //ColumnTuples.push_back(t5);

          client->batchMutate(ColumnTuples);

          rs = client->getColumnsValues("key_1", "persistence", ColumnNames);
          cout<<"a: "<<rs["a"]<<endl;
          cout<<"f: "<<rs["f"]<<endl;
          cout<<"k: "<<rs["k"]<<endl;
          cout<<"p: "<<rs["p"]<<endl;
          cout<<"u: "<<rs["u"]<<endl;
          cout<<"z: "<<rs["z"]<<endl;
         
	}
        catch (...)
          {
            cout<<"exception found"<<endl;
          }
        /*        catch (org::apache::cassandra::NotFoundException &ire)
	  {
	    cout <<"NotFoundException Caught" << endl;
	    return 1;
	  }
	catch (org::apache::cassandra::InvalidRequestException &ire)
	  {
	    cout << ire.why << endl;
	    return 1;
	  }
        */
        return 0;


	return 0;
}
