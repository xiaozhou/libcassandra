/*
 * LibCassandra
 * Copyright (C) 2010 Padraig O'Sullivan
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#include <time.h>
#include <netinet/in.h>

#include <string>
#include <set>
#include <sstream>
#include <iostream>

#include "libgenthrift/Cassandra.h"
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>

#include "libcassandra/cassandra.h"
#include "libcassandra/exception.h"
#include "libcassandra/indexed_slices_query.h"
#include "libcassandra/keyspace.h"
#include "libcassandra/keyspace_definition.h"
#include "libcassandra/util_functions.h"

using namespace std;
using namespace org::apache::cassandra;
using namespace libcassandra;


Cassandra::Cassandra()
  :
    thrift_client(NULL),
	host(),
	port(0),
	cluster_name(),
	server_version(),
	current_keyspace(),
	key_spaces(),
	token_map()
{
}


Cassandra::Cassandra(CassandraClient *in_thrift_client,
                     const string &in_host,
                     int in_port)
  :
    thrift_client(in_thrift_client),
    host(in_host),
    port(in_port),
    cluster_name(),
    server_version(),
    current_keyspace(),
    key_spaces(),
    token_map()
{}


Cassandra::Cassandra(CassandraClient *in_thrift_client,
                     const string &in_host,
                     int in_port,
                     const string& keyspace)
  :
    thrift_client(in_thrift_client),
    host(in_host),
    port(in_port),
    cluster_name(),
    server_version(),
    current_keyspace(keyspace),
    key_spaces(),
    token_map()
{}


Cassandra::~Cassandra()
{
  delete thrift_client;
}

void Cassandra::setRecvTimeout(int recv_timeout) {

  if (recv_timeout > 0) {
    boost::shared_ptr<apache::thrift::transport::TTransport>            t1 = thrift_client->getInputProtocol()->getTransport();
    boost::shared_ptr<apache::thrift::transport::TFramedTransport>      t2 = boost::dynamic_pointer_cast<apache::thrift::transport::TFramedTransport>(t1);
    boost::shared_ptr<apache::thrift::transport::TTransport>            t3 = t2->getUnderlyingTransport();
    boost::shared_ptr<apache::thrift::transport::TSocket>                s = boost::dynamic_pointer_cast<apache::thrift::transport::TSocket>(t3);
    s->setRecvTimeout(recv_timeout);
  }

}

void Cassandra::setSendTimeout(int send_timeout) {

  if (send_timeout > 0) {
    boost::shared_ptr<apache::thrift::transport::TTransport>            t1 = thrift_client->getOutputProtocol()->getTransport();
    boost::shared_ptr<apache::thrift::transport::TFramedTransport>      t2 = boost::dynamic_pointer_cast<apache::thrift::transport::TFramedTransport>(t1);
    boost::shared_ptr<apache::thrift::transport::TTransport>            t3 = t2->getUnderlyingTransport();
    boost::shared_ptr<apache::thrift::transport::TSocket>                s = boost::dynamic_pointer_cast<apache::thrift::transport::TSocket>(t3);
    s->setSendTimeout(send_timeout);
  }

}

CassandraClient *Cassandra::getCassandra()
{
  return thrift_client;
}


void Cassandra::login(const string& user, const string& password)
{
  AuthenticationRequest req;
  req.credentials["username"]= user;
  req.credentials["password"]= password;
  thrift_client->login(req);
}


void Cassandra::setKeyspace(const string& ks_name)
{
  current_keyspace.assign(ks_name);
  thrift_client->set_keyspace(ks_name);
}


string Cassandra::getCurrentKeyspace() const
{
  return current_keyspace;
}


void Cassandra::insertColumn(const string& key,
                             const string& column_family,
                             const string& super_column_name,
                             const string& column_name,
                             const string& value,
                             ConsistencyLevel::type level,
                             int32_t ttl= 0)
{
  ColumnParent col_parent;
  col_parent.column_family.assign(column_family);
  if (! super_column_name.empty()) 
  {
    col_parent.super_column.assign(super_column_name);
    col_parent.__isset.super_column= true;
  }
  Column col;
  col.name.assign(column_name);
  col.value.assign(value);
  col.timestamp= createTimestamp();
  if (ttl) 
  {
    col.ttl=ttl;
    col.__isset.ttl=true;
  }
  /* 
   * actually perform the insert 
   * TODO - validate the ColumnParent before the insert
   */
  thrift_client->insert(key, col_parent, col, level);
}


void Cassandra::insertColumn(const string& key,
                             const string& column_family,
                             const string& super_column_name,
                             const string& column_name,
                             const string& value)
{
  insertColumn(key, column_family, super_column_name, column_name, value, ConsistencyLevel::QUORUM);
}


void Cassandra::insertColumn(const string& key,
                             const string& column_family,
                             const string& column_name,
                             const string& value)
{
  insertColumn(key, column_family, "", column_name, value, ConsistencyLevel::QUORUM);
}


void Cassandra::insertColumn(const string& key,
                             const string& column_family,
                             const string& column_name,
                             const int64_t value)
{
  //int64_t store_value= htonll(value);
  insertColumn(key, column_family, "", column_name, serializeLong(value), ConsistencyLevel::QUORUM);
}


void Cassandra::remove(const string &key,
                      const ColumnPath &col_path,
                      ConsistencyLevel::type level)
{
  thrift_client->remove(key, col_path, createTimestamp(), level);
}


void Cassandra::remove(const string &key,
                      const ColumnPath &col_path)
{
  thrift_client->remove(key, col_path, createTimestamp(), ConsistencyLevel::QUORUM);
}


void Cassandra::remove(const string& key,
                       const string& column_family,
                       const string& super_column_name,
                       const string& column_name)
{
  ColumnPath col_path;
  col_path.column_family.assign(column_family);
  if (! super_column_name.empty()) 
  {
    col_path.super_column.assign(super_column_name);
    col_path.__isset.super_column= true;
  }
  if (! column_name.empty()) 
  {
    col_path.column.assign(column_name);
    col_path.__isset.column= true;
  }
  remove(key, col_path);
}


void Cassandra::removeColumn(const string& key,
                             const string& column_family,
                             const string& super_column_name,
                             const string& column_name)
{
  remove(key, column_family, super_column_name, column_name);
}


void Cassandra::removeSuperColumn(const string& key,
                                  const string& column_family,
                                  const string& super_column_name)
{
  remove(key, column_family, super_column_name, "");
}


Column Cassandra::getColumn(const string& key,
                            const string& column_family,
                            const string& super_column_name,
                            const string& column_name,
                            ConsistencyLevel::type level)
{
  ColumnPath col_path;
  col_path.column_family.assign(column_family);
  if (! super_column_name.empty()) 
  {
    col_path.super_column.assign(super_column_name);
    col_path.__isset.super_column= true;
  }
  col_path.column.assign(column_name);
  col_path.__isset.column= true;
  ColumnOrSuperColumn cosc;
  /* TODO - validate column path */
  thrift_client->get(cosc, key, col_path, level);
  if (cosc.column.name.empty())
  {
    /* throw an exception */
    throw(InvalidRequestException());
  }
  return cosc.column;
}


Column Cassandra::getColumn(const string& key,
                            const string& column_family,
                            const string& super_column_name,
                            const string& column_name)
{
  return getColumn(key, column_family, super_column_name, column_name, ConsistencyLevel::QUORUM);
}

Column Cassandra::getColumn(const string& key,
                            const string& column_family,
                            const string& column_name)
{
  return getColumn(key, column_family, "", column_name, ConsistencyLevel::QUORUM);
}


string Cassandra::getColumnValue(const string& key,
                                 const string& column_family,
                                 const string& super_column_name,
                                 const string& column_name)
{
  return getColumn(key, column_family, super_column_name, column_name).value;
}


string Cassandra::getColumnValue(const string& key,
                                 const string& column_family,
                                 const string& column_name)
{
	return getColumn(key, column_family, column_name).value;
}


int64_t Cassandra::getIntegerColumnValue(const string& key,
                                         const string& column_family,
                                         const string& column_name)
{
	string ret= getColumn(key, column_family, column_name).value;
  return deserializeLong(ret);
}


SuperColumn Cassandra::getSuperColumn(const string& key,
                                      const string& column_family,
                                      const string& super_column_name,
                                      ConsistencyLevel::type level)
{
  ColumnPath col_path;
  col_path.column_family.assign(column_family);
  col_path.super_column.assign(super_column_name);
  /* this is ugly but thanks to thrift is needed */
  col_path.__isset.super_column= true;
  ColumnOrSuperColumn cosc;
  /* TODO - validate super column path */
  thrift_client->get(cosc, key, col_path, level);
  if (cosc.super_column.name.empty())
  {
    /* throw an exception */
    throw(InvalidRequestException());
  }
  return cosc.super_column;
}


SuperColumn Cassandra::getSuperColumn(const string& key,
                                      const string& column_family,
                                      const string& super_column_name)
{
  return getSuperColumn(key, column_family, super_column_name, ConsistencyLevel::QUORUM);
}

vector<Column> Cassandra::getColumns(
    const string &key,
    const string &column_family,
    const string &super_column_name,
    const vector<string> column_names,
    org::apache::cassandra::ConsistencyLevel::type level
    )
{

  ColumnParent col_parent;
  SlicePredicate pred;
  vector<ColumnOrSuperColumn> ret_cosc;
  vector<Column> result;

  col_parent.column_family.assign(column_family);
  if (! super_column_name.empty()) 
  {
    col_parent.super_column.assign(super_column_name);
    col_parent.__isset.super_column= true;
  }

  pred.column_names = column_names;
  pred.__isset.column_names= true;

  thrift_client->get_slice(ret_cosc, key, col_parent, pred, level);
  for (vector<ColumnOrSuperColumn>::iterator it= ret_cosc.begin();
      it != ret_cosc.end();
      ++it)
  {
    if (! (*it).column.name.empty())
    {
      result.push_back((*it).column);
    }
  }
  return result;
}

vector<Column> Cassandra::getColumns(
    const string &key,
    const string &column_family,
    const string &super_column_name,
    const vector<string> column_names
    )
{
	return getColumns(key, column_family, super_column_name, column_names, ConsistencyLevel::QUORUM);
}

vector<Column> Cassandra::getColumns(
    const string &key,
    const string &column_family,
    const vector<string> column_names,
    org::apache::cassandra::ConsistencyLevel::type level
    )
{
  return getColumns(key, column_family, "", column_names, level);
}

vector<Column> Cassandra::getColumns(
    const string &key,
    const string &column_family,
    const vector<string> column_names
    )
{
  return getColumns(key, column_family, "", column_names, ConsistencyLevel::QUORUM);
}

tr1::unordered_map<string, string> Cassandra::getColumnsValues(
    const string &key,
    const string &column_family,
    const string &super_column_name,
    const vector<string> column_names
    )
{
	tr1::unordered_map<string, string> result;
	vector<Column> columns = getColumns(key, column_family, super_column_name, column_names, ConsistencyLevel::QUORUM);
        //	uint32_t count = 0;
	for (vector<Column>::iterator it= columns.begin(); it != columns.end(); ++it)
	{
		result[(*it).name]=(*it).value;
		//count++;
	}
	if(columns.size()<column_names.size())
	{
	    /* throw an exception */
	    throw(NotFoundException());
	}
	return result;
}

tr1::unordered_map<string, string> Cassandra::getColumnsValues(
    const string &key,
    const string &column_family,
    const vector<string> column_names
    )
{
	return getColumnsValues(key, column_family, "", column_names);
}

vector<Column> Cassandra::getColumns(const string &key,
                           const std::string &column_family,
                           const std::string &super_column_name,
                           const org::apache::cassandra::SliceRange &range,
			   org::apache::cassandra::ConsistencyLevel::type level
			   )
{

  ColumnParent col_parent;
  SlicePredicate pred;
  vector<ColumnOrSuperColumn> ret_cosc;
  vector<Column> result;

  col_parent.column_family.assign(column_family);
  if (! super_column_name.empty()) 
  {
    col_parent.super_column.assign(super_column_name);
    col_parent.__isset.super_column= true;
  }

  pred.slice_range = range;
  pred.__isset.slice_range= true;

  thrift_client->get_slice(ret_cosc, key, col_parent, pred, level);
  for (vector<ColumnOrSuperColumn>::iterator it= ret_cosc.begin();
      it != ret_cosc.end();
      ++it)
  {
    if (! (*it).column.name.empty())
    {
      result.push_back((*it).column);
    }
  }
  return result;
}

vector<Column> Cassandra::getColumns(const string &key,
                           const std::string &column_family,
                           const std::string &super_column_name,
                           const org::apache::cassandra::SliceRange &range
			   )
{
	return getColumns(key, column_family, super_column_name, range, ConsistencyLevel::QUORUM);
}

vector<Column> Cassandra::getColumns(const string &key,
                           const std::string &column_family,
                           const org::apache::cassandra::SliceRange &range,
			   org::apache::cassandra::ConsistencyLevel::type level
			   )
{
  return getColumns(key, column_family, "", range, level);
}

vector<Column> Cassandra::getColumns(const string &key,
                           const std::string &column_family,
                           const org::apache::cassandra::SliceRange &range
			   )
{
  return getColumns(key, column_family, "", range, ConsistencyLevel::QUORUM);
}

vector<SuperColumn> Cassandra::getSuperColumns(
  const string& key,
  const string &column_family,
  const vector<string> super_column_names,
  ConsistencyLevel::type level)
{
  ColumnParent col_parent;
  SlicePredicate pred;
  vector<ColumnOrSuperColumn> ret_cosc;
  vector<SuperColumn> result;

  col_parent.column_family.assign(column_family);

  pred.column_names = super_column_names;
  pred.__isset.column_names= true;

  thrift_client->get_slice(ret_cosc, key, col_parent, pred, level);
  for (vector<ColumnOrSuperColumn>::iterator it= ret_cosc.begin();
       it != ret_cosc.end();
       ++it)
  {
    if (! (*it).super_column.name.empty())
    {
      result.push_back((*it).super_column);
    }
  }
  return result;
}

vector<SuperColumn> Cassandra::getSuperColumns(
  const string& key,
  const string &column_family,
  const vector<string> super_column_names)
{
  return getSuperColumns(key, column_family, super_column_names, ConsistencyLevel::QUORUM);
}

vector<SuperColumn> Cassandra::getSuperColumns(
    const string& key,
    const string& column_family,
    const org::apache::cassandra::SliceRange &range,
    ConsistencyLevel::type level)
{
  ColumnParent col_parent;
  SlicePredicate pred;
  vector<ColumnOrSuperColumn> ret_cosc;
  vector<SuperColumn> result;

  col_parent.column_family.assign(column_family);

  pred.slice_range = range;
  pred.__isset.slice_range= true;

  thrift_client->get_slice(ret_cosc, key, col_parent, pred, level);
  for (vector<ColumnOrSuperColumn>::iterator it= ret_cosc.begin();
       it != ret_cosc.end();
       ++it)
  {
    if (! (*it).super_column.name.empty())
    {
      result.push_back((*it).super_column);
    }
  }
  return result;
}


vector<SuperColumn> Cassandra::getSuperColumns(
    const string& key,
    const string& column_family,
    const org::apache::cassandra::SliceRange &range)
{
  return getSuperColumns(key, column_family, range, ConsistencyLevel::QUORUM);
}


map<string, vector<Column> > Cassandra::getRangeSlice(const ColumnParent& col_parent,
                                                      const SlicePredicate& pred,
                                                      const string& start,
                                                      const string& finish,
                                                      const int32_t row_count,
                                                      ConsistencyLevel::type level)
{
  map<string, vector<Column> > ret;
  vector<KeySlice> key_slices;
  KeyRange key_range;
  key_range.start_key.assign(start);
  key_range.end_key.assign(finish);
  key_range.count= row_count;
  key_range.__isset.start_key= true;
  key_range.__isset.end_key= true;
  thrift_client->get_range_slices(key_slices,
                                  col_parent,
                                  pred,
                                  key_range,
                                  level);
  if (! key_slices.empty())
  {
    for (vector<KeySlice>::iterator it= key_slices.begin();
         it != key_slices.end();
         ++it)
    {
      ret.insert(make_pair((*it).key, getColumnList((*it).columns)));
    }
  }
  return ret;
}


map<string, vector<Column> > Cassandra::getRangeSlice(const ColumnParent& col_parent,
                                                      const SlicePredicate& pred,
                                                      const string& start,
                                                      const string& finish,
                                                      const int32_t row_count)
{
  return getRangeSlice(col_parent, pred, start, finish, row_count, ConsistencyLevel::QUORUM);
}


map<string, vector<SuperColumn> > Cassandra::getSuperRangeSlice(const ColumnParent& col_parent,
                                                                const SlicePredicate& pred,
                                                                const string& start,
                                                                const string& finish,
                                                                const int32_t row_count,
                                                                ConsistencyLevel::type level)
{
  map<string, vector<SuperColumn> > ret;
  vector<KeySlice> key_slices;
  KeyRange key_range;
  key_range.start_key.assign(start);
  key_range.end_key.assign(finish);
  key_range.count= row_count;
  key_range.__isset.start_key= true;
  key_range.__isset.end_key= true;
  thrift_client->get_range_slices(key_slices,
                                  col_parent,
                                  pred,
                                  key_range,
                                  level);
  if (! key_slices.empty())
  {
    for (vector<KeySlice>::iterator it= key_slices.begin();
         it != key_slices.end();
         ++it)
    {
      ret.insert(make_pair((*it).key, getSuperColumnList((*it).columns)));
    }
  }
  return ret;
}



map<string, vector<SuperColumn> > Cassandra::getSuperRangeSlice(const ColumnParent& col_parent,
                                                                const SlicePredicate& pred,
                                                                const string& start,
                                                                const string& finish,
                                                                const int32_t row_count)
{
  return getSuperRangeSlice(col_parent, pred, start, finish, row_count, ConsistencyLevel::QUORUM);
}


map<string, map<string, string> >
Cassandra::getIndexedSlices(const IndexedSlicesQuery& query)
{
  map<string, map<string, string> > ret_map;
  vector<KeySlice> ret;
  SlicePredicate thrift_slice_pred= createSlicePredicateObject(query);
  ColumnParent thrift_col_parent;
  thrift_col_parent.column_family.assign(query.getColumnFamily());
  thrift_client->get_indexed_slices(ret, 
                                    thrift_col_parent, 
                                    query.getIndexClause(),
                                    thrift_slice_pred,
                                    query.getConsistencyLevel());
  
  for(vector<KeySlice>::iterator it= ret.begin();
      it != ret.end();
      ++it)
  {
    vector<Column> thrift_cols= getColumnList((*it).columns);
    map<string, string> rows;
    for (vector<Column>::iterator inner_it= thrift_cols.begin();
         inner_it != thrift_cols.end();
         ++inner_it)
    {
      rows.insert(make_pair((*inner_it).name, (*inner_it).value));
    }
    ret_map.insert(make_pair((*it).key, rows));
  }

  return ret_map;
}


int32_t Cassandra::getCount(const string& key, 
                            const ColumnParent& col_parent,
                            const SlicePredicate& pred,
                            ConsistencyLevel::type level)
{
  return (thrift_client->get_count(key, col_parent, pred, level));
}


int32_t Cassandra::getCount(const string& key, 
                            const ColumnParent& col_parent,
                            const SlicePredicate& pred)
{
  return (getCount(key, col_parent, pred, ConsistencyLevel::QUORUM));
}


vector<KeyspaceDefinition> Cassandra::getKeyspaces()
{
  if (key_spaces.empty())
  {
    vector<KsDef> thrift_ks_defs;
    thrift_client->describe_keyspaces(thrift_ks_defs);
    for (vector<KsDef>::iterator it= thrift_ks_defs.begin();
         it != thrift_ks_defs.end();
         ++it)
    {
      KsDef thrift_entry= *it;
      KeyspaceDefinition entry(thrift_entry.name,
                               thrift_entry.strategy_class,
                               thrift_entry.strategy_options,
                               thrift_entry.replication_factor,
                               thrift_entry.cf_defs);
      key_spaces.push_back(entry);
    }
  }
  return key_spaces;
}


string Cassandra::createColumnFamily(const ColumnFamilyDefinition& cf_def)
{
  string schema_id;
  CfDef thrift_cf_def= createCfDefObject(cf_def);
  thrift_client->system_add_column_family(schema_id, thrift_cf_def);
  return schema_id;
}


string Cassandra::dropColumnFamily(const string& cf_name)
{
  string schema_id;
  thrift_client->system_drop_column_family(schema_id, cf_name);
  return schema_id;
}


string Cassandra::createKeyspace(const KeyspaceDefinition& ks_def)
{
  string ret;
  KsDef thrift_ks_def= createKsDefObject(ks_def);
  thrift_client->system_add_keyspace(ret, thrift_ks_def);
  return ret;
}


string Cassandra::dropKeyspace(const string& ks_name)
{
  string ret;
  thrift_client->system_drop_keyspace(ret, ks_name);
  return ret;
}


string Cassandra::getClusterName()
{
  if (cluster_name.empty())
  {
    thrift_client->describe_cluster_name(cluster_name);
  }
  return cluster_name;
}


string Cassandra::getServerVersion()
{
  if (server_version.empty())
  {
    thrift_client->describe_version(server_version);
  }
  return server_version;
}


string Cassandra::getHost()
{
  return host;
}


int Cassandra::getPort() const
{
  return port;
}


bool Cassandra::findKeyspace(const string& name)
{
  for (vector<KeyspaceDefinition>::iterator it= key_spaces.begin();
       it != key_spaces.end();
       ++it)
  {
    if (name == it->getName())
    {
      return true;
    }
  }
  return false;
}

void Cassandra::batchMutate(const vector<ColumnTuple> &tuples, const ConsistencyLevel::type level){
	MutationsMap mutations;

    for (std::vector<ColumnTuple>::const_iterator tuple = tuples.begin();
    		tuple != tuples.end(); tuple++) {
    	addToMap(*tuple, mutations);
    }

    thrift_client->batch_mutate(mutations, level);
}

void Cassandra::batchMutate(const vector<ColumnTuple> &tuples){
	batchMutate(tuples, ConsistencyLevel::QUORUM);
}

void Cassandra::batchMutate(const vector<SuperColumnTuple> &tuples, const ConsistencyLevel::type level){
	MutationsMap mutations;

    for (vector<SuperColumnTuple>::const_iterator tuple = tuples.begin();
    		tuple != tuples.end(); tuple++) {
    	addToMap(*tuple, mutations);
    }

    thrift_client->batch_mutate(mutations, level);
}

void Cassandra::batchMutate(const vector<SuperColumnTuple> &tuples){
    batchMutate(tuples, ConsistencyLevel::QUORUM);
}

void Cassandra::batchMutate(const vector<insertSuperColumnTuple> &ituples, const vector<removeSuperColumnTuple> &rtuples,
		  ConsistencyLevel::type level) {
    MutationsMap mutations;

    for (vector<insertSuperColumnTuple>::const_iterator tuple = ituples.begin();
    		tuple != ituples.end(); tuple++) {
    	addToMap(*tuple, mutations);
    }

    for (vector<removeSuperColumnTuple>::const_iterator tuple = rtuples.begin();
    		tuple != rtuples.end(); tuple++) {
    	addToMap(*tuple, mutations);
    }

    thrift_client->batch_mutate(mutations, level);
}

void Cassandra::batchMutate(const vector<insertSuperColumnTuple> &ituples, const vector<removeSuperColumnTuple> &rtuples) {
    batchMutate(ituples, rtuples, ConsistencyLevel::QUORUM);
}

void Cassandra::batchMutate(const vector<batchSuperColumnTuple> &tuples, const org::apache::cassandra::ConsistencyLevel::type level) {
    MutationsMap mutations;
    for (vector<batchSuperColumnTuple>::const_iterator tuple = tuples.begin();
    		tuple != tuples.end(); tuple++) {
    	addToMap(*tuple, mutations);
    }    
    
    thrift_client->batch_mutate(mutations, level);  
}

void Cassandra::batchMutate(const vector<batchSuperColumnTuple> &tuples) {
    batchMutate(tuples, ConsistencyLevel::QUORUM); 
}

void Cassandra::addToMap(const ColumnTuple &tuple, MutationsMap &mutations)
{
  string column_family = tr1::get<0>(tuple);
  string key           = tr1::get<1>(tuple);
  string name          = tr1::get<2>(tuple);
  string value         = tr1::get<3>(tuple);
  bool is_delete       = tr1::get<4>(tuple);

  Mutation mutation;

  if(!is_delete){
      mutation.__isset.column_or_supercolumn          = true;
      mutation.column_or_supercolumn.__isset.column   = true;
      mutation.column_or_supercolumn.column.name      = name;
      mutation.column_or_supercolumn.column.value     = value;
      mutation.column_or_supercolumn.column.timestamp = createTimestamp();
  }

  if(is_delete){
      mutation.__isset.deletion                       = true;
      mutation.deletion.timestamp                     = createTimestamp();

      mutation.deletion.__isset.predicate             = true;
      mutation.deletion.predicate.__isset.column_names= true;
      mutation.deletion.predicate.column_names.push_back(name);
  }

  if (mutations.find(key) == mutations.end()) {
    mutations[key] = map<string,
                        vector<Mutation> >();
  }

  map<string,
      vector<Mutation>
     > &mutations_per_cf = mutations[key];

  if (mutations_per_cf.find(column_family) == mutations_per_cf.end()) {
    mutations_per_cf[column_family] = std::vector<Mutation>();
  }

  vector<Mutation> &mutation_list = mutations_per_cf[column_family];

  mutation_list.push_back(mutation);

}

void Cassandra::addToMap(const SuperColumnTuple &tuple, MutationsMap &mutations)
{
  string column_family = tr1::get<0>(tuple);
  string key           = tr1::get<1>(tuple);
  string super_name    = tr1::get<2>(tuple);
  string name          = tr1::get<3>(tuple);
  string value         = tr1::get<4>(tuple);
  bool is_delete       = tr1::get<5>(tuple);

  Mutation mutation;

  if(!is_delete){
      mutation.__isset.column_or_supercolumn              = true;
      mutation.column_or_supercolumn.__isset.super_column = true;
      mutation.column_or_supercolumn.super_column.name    = super_name;

      Column column;
      column.name = name;
      column.value = value;
      column.timestamp = createTimestamp();
      mutation.column_or_supercolumn.super_column.columns.push_back(column);
  }

  if(is_delete){
      mutation.__isset.deletion                       = true;
      mutation.deletion.timestamp                     = createTimestamp();

      mutation.deletion.__isset.super_column          = true;
      mutation.deletion.super_column                  = super_name;

      mutation.deletion.__isset.predicate             = true;
      mutation.deletion.predicate.__isset.column_names= true;
      mutation.deletion.predicate.column_names.push_back(name);
  }

  if (mutations.find(key) == mutations.end()) {
	  mutations[key] = map<string,
                           vector<Mutation> >();
  }

  map<string,
      vector<Mutation>
     > &mutations_per_cf = mutations[key];

  if (mutations_per_cf.find(column_family) == mutations_per_cf.end()) {
	  mutations_per_cf[column_family] = vector<Mutation>();
  }

  vector<Mutation> &mutation_list = mutations_per_cf[column_family];

  mutation_list.push_back(mutation);

}

void Cassandra::addToMap(const insertSuperColumnTuple &tuple, MutationsMap &mutations)
{
  string column_family   = tr1::get<0>(tuple);
  string key             = tr1::get<1>(tuple);
  string super_name      = tr1::get<2>(tuple);
  vector<Column> columns = tr1::get<3>(tuple);

  Mutation mutation;

  mutation.__isset.column_or_supercolumn              = true;
  mutation.column_or_supercolumn.__isset.super_column = true;
  mutation.column_or_supercolumn.super_column.name    = super_name;
  mutation.column_or_supercolumn.super_column.columns = columns;

  map<string,
      vector<Mutation>
     > &mutations_per_cf = mutations[key];

  if (mutations_per_cf.find(column_family) == mutations_per_cf.end()) {
    mutations_per_cf[column_family] = std::vector<Mutation>();
  }

  vector<Mutation> &mutation_list = mutations_per_cf[column_family];

  mutation_list.push_back(mutation);
}

void Cassandra::addToMap(const removeSuperColumnTuple &tuple, MutationsMap &mutations)
{
  string column_family = tr1::get<0>(tuple);
  string key           = tr1::get<1>(tuple);
  string super_name    = tr1::get<2>(tuple);
  vector<string> names = tr1::get<3>(tuple);

  Mutation mutation;

  mutation.__isset.deletion                        = true;
  mutation.deletion.__isset.super_column           = true;
  mutation.deletion.super_column                   = super_name;
  mutation.deletion.__isset.predicate              = true;
  mutation.deletion.predicate.__isset.column_names = true;
  mutation.deletion.predicate.column_names         = names;

  map<string,
      vector<Mutation>
     > &mutations_per_cf = mutations[key];

  if (mutations_per_cf.find(column_family) == mutations_per_cf.end()) {
    mutations_per_cf[column_family] = std::vector<Mutation>();
  }

  vector<Mutation> &mutation_list = mutations_per_cf[column_family];

  mutation_list.push_back(mutation);
}

void Cassandra::addToMap(const batchSuperColumnTuple &tuple, MutationsMap &mutations)
{
  string column_family   = tr1::get<0>(tuple);
  string key             = tr1::get<1>(tuple);
  string super_name      = tr1::get<2>(tuple);
  vector<Column> columns = tr1::get<3>(tuple);
  vector<string> names   = tr1::get<4>(tuple);
  
  map<string,
      vector<Mutation>
     > &mutations_per_cf = mutations[key];

  if (mutations_per_cf.find(column_family) == mutations_per_cf.end()) {
    mutations_per_cf[column_family] = std::vector<Mutation>();
  }

  vector<Mutation> &mutation_list = mutations_per_cf[column_family];  
  
  if(columns.size()>0){
    Mutation mutation_i;

    mutation_i.__isset.column_or_supercolumn              = true;
    mutation_i.column_or_supercolumn.__isset.super_column = true;
    mutation_i.column_or_supercolumn.super_column.name    = super_name;
    mutation_i.column_or_supercolumn.super_column.columns = columns;
     
    mutation_list.push_back(mutation_i);
  }
     
  if(names.size()>0){

    Mutation mutation_d;

    mutation_d.__isset.deletion                        = true;
    mutation_d.deletion.timestamp                      = createTimestamp();
    mutation_d.deletion.__isset.super_column           = true;
    mutation_d.deletion.super_column                   = super_name;
    mutation_d.deletion.__isset.predicate              = true;
    mutation_d.deletion.predicate.__isset.column_names = true;
    mutation_d.deletion.predicate.column_names         = names;

    mutation_list.push_back(mutation_d);  
  }
}