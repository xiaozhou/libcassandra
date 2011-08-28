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
static int port= 9160;

int main()
{
  CassandraFactory factory(host, port);
  boost::shared_ptr<Cassandra> client(factory.create());

  string script_type="js";
  string script_args="";
  string script_contents="system.require('std/shim/restore/simpleStorage.em');";

  string script_value = "#type#"+script_type+"#args#"+script_args+"#contents#"+script_contents;
  try {
    client->setKeyspace("sirikata");
    client->insertColumn("testoh_1", "objects", "@", "a", script_value);

    std::vector<Column> Columns;
    SliceRange range;
    range.count=1000000; // set large enough to get all the objects of the object host
    Columns = client->getColumns("testoh_1", "objects", "@", range);

    for (std::vector<Column>::iterator it= Columns.begin(); it != Columns.end(); ++it) {
      string object_str((*it).name);
      string script_value((*it).value);
      string _args=script_value.substr(6,script_value.find("#args#")-6);
      string _type=script_value.substr(script_value.find("#args#")+6,script_value.find("#contents#")-script_value.find("#args#")-6);
      string _contents=script_value.substr(script_value.find("#contents#")+10);

      cout<<"object_name:\n"<<object_str<<endl;
      cout<<"script_args:\n"<<_args<<endl;
      cout<<"script_type:\n"<<_type<<endl;
      cout<<"script_contents:\n"<<_contents<<endl;
    }

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
