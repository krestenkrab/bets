/*
%%
%% This file is part of EBDB - Erlang Berkeley DB API
%%
%% Copyright (c) 2011 by Trifork
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
*/

#include <db.h> 
#include <errno.h> 
#include <string.h> 
#include "erl_nif.h"
#include "erl_driver.h"

#define BUFFER_SIZE (128*1024)
#define MAX_KEY_SIZE (4*1024)

ERL_NIF_TERM ebdb_nifs_db_create (ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);


static ErlNifResourceType* ebdb_db_RESOURCE;
static ErlNifResourceType* ebdb_env_RESOURCE;
static ErlNifResourceType* ebdb_txn_RESOURCE;
static ErlNifResourceType* ebdb_cursor_RESOURCE;

static ErlNifTSDKey ebdb_buffer_TSD;

// Atoms (initialized in on_load)
static ERL_NIF_TERM ATOM_NEXT;
static ERL_NIF_TERM ATOM_NEXT_DUP;
static ERL_NIF_TERM ATOM_SET;
static ERL_NIF_TERM ATOM_SET_RANGE;

static ERL_NIF_TERM ATOM_INIT_CDB;
static ERL_NIF_TERM ATOM_INIT_LOCK;
static ERL_NIF_TERM ATOM_INIT_LOG;
static ERL_NIF_TERM ATOM_INIT_MPOOL;
static ERL_NIF_TERM ATOM_INIT_REP;
static ERL_NIF_TERM ATOM_INIT_TXN;
static ERL_NIF_TERM ATOM_RECOVER;
static ERL_NIF_TERM ATOM_RECOVER_FATAL;
static ERL_NIF_TERM ATOM_USE_ENVIRON;
static ERL_NIF_TERM ATOM_USE_ENVIRON_ROOT;
static ERL_NIF_TERM ATOM_CREATE;
static ERL_NIF_TERM ATOM_LOCKDOWN;
static ERL_NIF_TERM ATOM_PRIVATE;
static ERL_NIF_TERM ATOM_REGISTER;
static ERL_NIF_TERM ATOM_SYSTEM_MEM;
static ERL_NIF_TERM ATOM_THREAD;
static ERL_NIF_TERM ATOM_AUTO_COMMIT;
static ERL_NIF_TERM ATOM_EXCL;
static ERL_NIF_TERM ATOM_MULTIVERSION;
static ERL_NIF_TERM ATOM_NOMMAP;
static ERL_NIF_TERM ATOM_RDONLY;
static ERL_NIF_TERM ATOM_READ_UNCOMMITTED;
static ERL_NIF_TERM ATOM_TRUNCATE;
static ERL_NIF_TERM ATOM_NOSYNC;

static ERL_NIF_TERM ATOM_CONSUME;
static ERL_NIF_TERM ATOM_CONSUME_WAIT;
static ERL_NIF_TERM ATOM_RMW;

static ERL_NIF_TERM ATOM_APPEND;
static ERL_NIF_TERM ATOM_NODUPDATA;
static ERL_NIF_TERM ATOM_NOOVERWRITE;
static ERL_NIF_TERM ATOM_OVERWRITE_DUP;

static ERL_NIF_TERM ATOM_READ_COMMITTED;
static ERL_NIF_TERM ATOM_TXN_BULK;
static ERL_NIF_TERM ATOM_TXN_SYNC;
static ERL_NIF_TERM ATOM_TXN_NOSYNC;
static ERL_NIF_TERM ATOM_TXN_WRITE_NOSYNC;
static ERL_NIF_TERM ATOM_TXN_WAIT;
static ERL_NIF_TERM ATOM_TXN_NOWAIT;
static ERL_NIF_TERM ATOM_TXN_SNAPSHOT;

static ERL_NIF_TERM ATOM_CURSOR_BULK;
static ERL_NIF_TERM ATOM_WRITECURSOR;

static ERL_NIF_TERM ATOM_NOTFOUND;
static ERL_NIF_TERM ATOM_KEYEMPTY;

static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_FALSE;
static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_TRUE;
static ERL_NIF_TERM ATOM_UNDEFINED;

static ERL_NIF_TERM ATOM_BTREE;
static ERL_NIF_TERM ATOM_HASH;
static ERL_NIF_TERM ATOM_QUEUE;
static ERL_NIF_TERM ATOM_RECNO;
static ERL_NIF_TERM ATOM_UNKNOWN;

/*   */

typedef struct {
  DB_ENV *envp;
} ebdb_env_handle;

typedef struct {
  DB_TXN *tid;
} ebdb_txn_handle;

typedef struct {
  DBC *cursor;
} ebdb_cursor_handle;

typedef struct {
  DB *dbp;
  ebdb_env_handle* env_handle;
} ebdb_db_handle;


int decode_flags(ErlNifEnv* env, ERL_NIF_TERM list, u_int32_t *flags) 
{
  u_int32_t f = 0;
  ERL_NIF_TERM head;
  while (enif_get_list_cell(env, list, &head, &list)) {
    
    if (enif_is_identical(head, ATOM_NEXT)) {
      f |= DB_NEXT;
    } else if (enif_is_identical(head, ATOM_SET)) {
      f |= DB_SET;
    } else if (enif_is_identical(head, ATOM_SET_RANGE)) {
      f |= DB_SET_RANGE;
    } else if (enif_is_identical(head, ATOM_NEXT_DUP)) {
      f |= DB_NEXT;
    } else if (enif_is_identical(head, ATOM_READ_UNCOMMITTED)) {
      f |= DB_READ_UNCOMMITTED;
    } else if (enif_is_identical(head, ATOM_READ_COMMITTED)) {
      f |= DB_READ_COMMITTED;
    } else if (enif_is_identical(head, ATOM_TXN_BULK)) {
      f |= DB_TXN_BULK;
    } else if (enif_is_identical(head, ATOM_CURSOR_BULK)) {
      f |= DB_CURSOR_BULK;
    } else if (enif_is_identical(head, ATOM_WRITECURSOR)) {
      f |= DB_WRITECURSOR;
    } else if (enif_is_identical(head, ATOM_TXN_SYNC)) {
      f |= DB_TXN_SYNC;
    } else if (enif_is_identical(head, ATOM_TXN_NOSYNC)) {
      f |= DB_TXN_NOSYNC;
    } else if (enif_is_identical(head, ATOM_TXN_WRITE_NOSYNC)) {
      f |= DB_TXN_WRITE_NOSYNC;
    } else if (enif_is_identical(head, ATOM_TXN_WAIT)) {
      f |= DB_TXN_WAIT;
    } else if (enif_is_identical(head, ATOM_TXN_NOWAIT)) {
      f |= DB_TXN_NOWAIT;
    } else if (enif_is_identical(head, ATOM_TXN_SNAPSHOT)) {
      f |= DB_TXN_SNAPSHOT;
    } else if (enif_is_identical(head, ATOM_TRUNCATE)) {
      f |= DB_TRUNCATE;
    } else if (enif_is_identical(head, ATOM_NOSYNC)) {
      f |= DB_NOSYNC;
    } else if (enif_is_identical(head, ATOM_CONSUME)) {
      f |= DB_CONSUME;
    } else if (enif_is_identical(head, ATOM_CONSUME_WAIT)) {
      f |= DB_CONSUME_WAIT;
    } else if (enif_is_identical(head, ATOM_RMW)) {
      f |= DB_RMW;
    } else if (enif_is_identical(head, ATOM_APPEND)) {
      f |= DB_APPEND;
    } else if (enif_is_identical(head, ATOM_NODUPDATA)) {
      f |= DB_NODUPDATA;
    } else if (enif_is_identical(head, ATOM_NOOVERWRITE)) {
      f |= DB_NOOVERWRITE;
    } else if (enif_is_identical(head, ATOM_OVERWRITE_DUP)) {
      f |= DB_OVERWRITE_DUP;

    } else if (enif_is_identical(head, ATOM_INIT_CDB)) {
      f |= DB_INIT_CDB;
    } else if (enif_is_identical(head, ATOM_INIT_LOCK)) {
      f |= DB_INIT_LOCK;
    } else if (enif_is_identical(head, ATOM_INIT_MPOOL)) {
      f |= DB_INIT_MPOOL;
    } else if (enif_is_identical(head, ATOM_INIT_REP)) {
      f |= DB_INIT_REP;
    } else if (enif_is_identical(head, ATOM_INIT_TXN)) {
      f |= DB_INIT_TXN;
    } else if (enif_is_identical(head, ATOM_RECOVER)) {
      f |= DB_RECOVER;
    } else if (enif_is_identical(head, ATOM_RECOVER_FATAL)) {
      f |= DB_RECOVER_FATAL;
    } else if (enif_is_identical(head, ATOM_USE_ENVIRON)) {
      f |= DB_USE_ENVIRON;
    } else if (enif_is_identical(head, ATOM_USE_ENVIRON_ROOT)) {
      f |= DB_USE_ENVIRON_ROOT;
    } else if (enif_is_identical(head, ATOM_LOCKDOWN)) {
      f |= DB_LOCKDOWN;
    } else if (enif_is_identical(head, ATOM_CREATE)) {
      f |= DB_CREATE;
    } else if (enif_is_identical(head, ATOM_PRIVATE)) {
      f |= DB_PRIVATE;
    } else if (enif_is_identical(head, ATOM_REGISTER)) {
      f |= DB_REGISTER;
    } else if (enif_is_identical(head, ATOM_SYSTEM_MEM)) {
      f |= DB_SYSTEM_MEM;
    } else if (enif_is_identical(head, ATOM_THREAD)) {
      f |= DB_THREAD;
    } else if (enif_is_identical(head, ATOM_AUTO_COMMIT)) {
      f |= DB_AUTO_COMMIT;
    } else if (enif_is_identical(head, ATOM_EXCL)) {
      f |= DB_EXCL;
    } else if (enif_is_identical(head, ATOM_MULTIVERSION)) {
      f |= DB_MULTIVERSION;
    } else if (enif_is_identical(head, ATOM_NOMMAP)) {
      f |= DB_NOMMAP;
    } else if (enif_is_identical(head, ATOM_RDONLY)) {
      f |= DB_RDONLY;

    } else {
      return 0;
    }
  } 


  *flags = f;
  return 1;
}



ERL_NIF_TERM describe_error(ErlNifEnv* env, int err) {
  switch (err) {
  case DB_RUNRECOVERY: 
    return enif_make_atom(env, "runrecovery");
  case DB_VERSION_MISMATCH: 
    return enif_make_atom(env, "version_mismatch");
  case DB_LOCK_DEADLOCK:
    return enif_make_atom(env, "lock_deadlock");
  case DB_LOCK_NOTGRANTED:
    return enif_make_atom(env, "lock_notgranted");
  case DB_BUFFER_SMALL:
    return enif_make_atom(env, "buffer_small");
  case DB_SECONDARY_BAD:
    return enif_make_atom(env, "secondary_bad");
  case DB_FOREIGN_CONFLICT:
    return enif_make_atom(env, "foreign_conflict");
  case DB_NOTFOUND:
    return ATOM_NOTFOUND;
  case DB_KEYEMPTY:
    return ATOM_KEYEMPTY;
  case EAGAIN: 
    return enif_make_atom(env, "eagain");
  case EINVAL: 
    return enif_make_atom(env, "einval");
  case ENOSPC: 
    return enif_make_atom(env, "enospc");
  case ENOENT: 
    return enif_make_atom(env, "enoent");
  case ENOMEM: 
    return enif_make_atom(env, "enomem");
  case EACCES: 
    return enif_make_atom(env, "eacces");
  }
  return enif_make_int(env, err);
}


ERL_NIF_TERM make_error_tuple(ErlNifEnv* env, int err) {
  return enif_make_tuple2(env, ATOM_ERROR, describe_error(env, err));
}


ERL_NIF_TERM ebdb_nifs_db_open_env(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  DB_ENV *db_env;
  char dirname[4096];
  ebdb_env_handle* handle;
  u_int32_t flags;
  int err;

  if (enif_get_string(env, argv[0], 
		      &dirname[0], sizeof(dirname), ERL_NIF_LATIN1) <= 0) {
    return enif_make_badarg(env);
  }

  if (!decode_flags(env, argv[1], &flags)) {
    return enif_make_badarg(env);
  }

  err=db_env_create(&db_env, 0);
  if ( err != 0 ) {
    return make_error_tuple(env, err);
  }

  handle = enif_alloc_resource(ebdb_env_RESOURCE,
			       sizeof(ebdb_env_handle));
  if (handle == NULL) {
    db_env->close(db_env, 0);
    return make_error_tuple(env, ENOMEM);
  }

  handle->envp = db_env;
  
  err=handle->envp->open( handle->envp, dirname, flags, 0 );
  if (err != 0) {
    enif_release_resource(handle);
    return make_error_tuple(env, err);
  }

  ERL_NIF_TERM result = enif_make_resource(env, handle);
  enif_release_resource(handle);
  return enif_make_tuple2(env, ATOM_OK, result);  
}

static int get_env_handle(ErlNifEnv* env, ERL_NIF_TERM arg, ebdb_env_handle **handlep) 
{
  if (enif_is_identical(arg, ATOM_UNDEFINED)) {
    *handlep = NULL;
  } else {
    if (!enif_get_resource(env, arg, ebdb_env_RESOURCE, (void**)handlep)) {
      return 0;
    }
  }
  return 1;
}

static int get_cursor_handle(ErlNifEnv* env, ERL_NIF_TERM arg, ebdb_cursor_handle **handlep) 
{
  if (!enif_get_resource(env, arg, ebdb_cursor_RESOURCE, (void**)handlep)) {
    return 0;
  }
  return 1;
}

static int get_db_handle(ErlNifEnv* env, ERL_NIF_TERM arg, ebdb_db_handle **handlep) 
{
  if (!enif_get_resource(env, arg, ebdb_db_RESOURCE, (void**)handlep)) {
    return 0;
  }
  return 1;
}

static int get_txn_handle(ErlNifEnv* env, ERL_NIF_TERM arg, ebdb_txn_handle **handlep) 
{
  if (enif_is_identical(arg, ATOM_UNDEFINED)) {
    *handlep = NULL;
  } else {
    if (!enif_get_resource(env, arg, ebdb_txn_RESOURCE, (void**)handlep)) {
      return 0;
    }
  }
  return 1;
}


static ERL_NIF_TERM ebdb_nifs_db_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  char filename[4096];
  ebdb_env_handle* env_handle;
  ebdb_db_handle* db_handle;
  ebdb_txn_handle* txn_handle;
  u_int32_t flags;
  int err;
  DB *db;

  if (!get_env_handle(env, argv[0], &env_handle)) {
    return enif_make_badarg(env);
  }

  if (!get_txn_handle(env, argv[1], &txn_handle)) {
    return enif_make_badarg(env);
  }

  if (enif_get_string(env, argv[2], 
		      &filename[0], sizeof(filename), ERL_NIF_LATIN1) <= 0) {
    return enif_make_badarg(env);
  }

  DBTYPE type;

  if (enif_is_identical(argv[3], ATOM_BTREE)) {
    type = DB_BTREE;
  } else if (enif_is_identical(argv[3], ATOM_HASH)) {
    type = DB_HASH;
  } else if (enif_is_identical(argv[3], ATOM_QUEUE)) {
    type = DB_QUEUE;
  } else if (enif_is_identical(argv[3], ATOM_RECNO)) {
    type = DB_RECNO;
  } else if (enif_is_identical(argv[3], ATOM_UNKNOWN)) {
    type = DB_UNKNOWN;
  } else {
    return enif_make_badarg(env);
  }

  // argv[4] decoded below...

  if (!decode_flags(env, argv[5], &flags)) {
    return enif_make_badarg(env);
  }

  err = db_create(&db, 
                  env_handle==NULL ? NULL : env_handle->envp, 
                  0);
  if (err != 0 ) {
    return make_error_tuple(env, err);
  }

  db_handle = enif_alloc_resource(ebdb_db_RESOURCE,
			       sizeof(ebdb_db_handle));
  if (db_handle == NULL) {
    db->close(db, 0);
    return make_error_tuple(env, ENOMEM);
  }
  db_handle->dbp = db;
  
  if (enif_is_identical(argv[4], ATOM_TRUE)) {
    db->set_flags(db, DB_DUP);
  }

  err = db->open( db,
                  txn_handle == NULL ? NULL : txn_handle->tid,
                  filename,
                  NULL,
                  type,
                  flags,
                  0 );
  if (err != 0) {
    enif_release_resource(db_handle);
    return make_error_tuple(env, err);
  }

  // make db_handle reference the env, so it is not
  // garbage collected before the db handle
  db_handle->env_handle = env_handle;
  if (env_handle != NULL) {
    enif_keep_resource(env_handle);
  }

  ERL_NIF_TERM result = enif_make_resource(env, db_handle);
  enif_release_resource(db_handle);
  return enif_make_tuple2(env, ATOM_OK, result);  
}


ERL_NIF_TERM ebdb_nifs_db_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  ebdb_db_handle *db_handle;
  u_int32_t flags;
  int err;

  if (   !get_db_handle(env, argv[0], &db_handle) 
         || !decode_flags(env, argv[1], &flags)) {
    return enif_make_badarg(env);
  }

  err = db_handle->dbp->close(db_handle->dbp, flags);
  db_handle->dbp = NULL;
  if (err != 0) {
    return make_error_tuple(env, err);
  }

  return ATOM_OK;
}

ERL_NIF_TERM ebdb_nifs_db_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  ebdb_db_handle *db_handle;
  ebdb_txn_handle *txn_handle;
  ErlNifBinary key_bin, value_bin;
  u_int32_t flags;
  int err;
  DBT key, value;

  if (!get_db_handle(env, argv[0], &db_handle) 
      || !get_txn_handle(env, argv[1], &txn_handle)
      || !enif_inspect_binary(env, argv[2], &key_bin)
      || !decode_flags(env, argv[3], &flags)) 
    {
      return enif_make_badarg(env);
    }

  key.data = key_bin.data;
  key.size = key_bin.size;
  key.ulen = key_bin.size;
  key.flags = DB_DBT_USERMEM;

  char *buffer = enif_tsd_get(ebdb_buffer_TSD);
  if (buffer == NULL) {
    buffer = enif_alloc(BUFFER_SIZE);
    enif_tsd_set(ebdb_buffer_TSD, buffer);
  }

  value.data = buffer;
  value.ulen = BUFFER_SIZE;
  value.flags = DB_DBT_USERMEM;

  err = db_handle->dbp->get(db_handle->dbp, 
                            txn_handle == NULL ? NULL : txn_handle->tid,
                            &key,
                            &value,
                            flags);

  if (err == 0) {

    // everything worked, copy from buffer into new binary result 
    if (!enif_alloc_binary(value.size, &value_bin)) {
      return make_error_tuple(env, ENOMEM);
    }
    
    memcpy(value_bin.data, value.data, value.size);
    return enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &value_bin));

  } else if (err == DB_BUFFER_SMALL) {

    if (!enif_alloc_binary(value.size, &value_bin)) {
      return make_error_tuple(env, ENOMEM);
    }

    value.data = value_bin.data;
    value.ulen = value_bin.size;
    value.flags = DB_DBT_USERMEM;

    err = db_handle->dbp->get(db_handle->dbp, 
                              txn_handle == NULL ? NULL : txn_handle->tid,
                              &key,
                              &value,
                              flags);

    if (err == 0) {
      return enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &value_bin));
    }

    enif_release_binary(&value_bin);

  }

  return make_error_tuple(env, err);
}

ERL_NIF_TERM ebdb_nifs_db_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  ebdb_db_handle *db_handle;
  ebdb_txn_handle *txn_handle;
  ErlNifBinary key_bin, value_bin;
  u_int32_t flags;
  int err;
  DBT key, value;
  int is_append = 0;

  if (!get_db_handle(env, argv[0], &db_handle) 
      || !get_txn_handle(env, argv[1], &txn_handle)
      || !decode_flags(env, argv[4], &flags)
      || !(   (is_append = ((flags&DB_APPEND)==DB_APPEND))
           || enif_inspect_binary(env, argv[2], &key_bin))
      || !enif_inspect_binary(env, argv[3], &value_bin)
      ) 
    {
      return enif_make_badarg(env);
    }

  if (is_append) {
    
    if (!enif_alloc_binary(sizeof(db_recno_t), &key_bin)) {
      return make_error_tuple(env, ENOMEM);
    }

    key.data = key_bin.data;
    key.ulen = sizeof(db_recno_t);
    key.flags = DB_DBT_USERMEM;

  } else {
    key.data = key_bin.data;
    key.size = key_bin.size;
    key.ulen = key_bin.size;
    key.flags = DB_DBT_USERMEM;
  }

  value.data = value_bin.data;
  value.size = value_bin.size;
  value.ulen = value_bin.size;
  value.flags = DB_DBT_USERMEM;

  err = db_handle->dbp->put(db_handle->dbp, 
                            txn_handle == NULL ? NULL : txn_handle->tid,
                            &key,
                            &value,
                            flags);

  if (err == 0) {
    if (is_append) {
      ERL_NIF_TERM key = enif_make_binary(env, &key_bin);
      return enif_make_tuple2(env, ATOM_OK, key);
    } else {
      return ATOM_OK;
    }
  }

  return make_error_tuple(env, err);
}

ERL_NIF_TERM ebdb_nifs_close_cursor(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  ebdb_cursor_handle *cursor_handle;
  int err;

  if ( !get_cursor_handle(env, argv[0], &cursor_handle) ) {
    return enif_make_badarg(env);
  }

  err = cursor_handle->cursor->close (cursor_handle->cursor);
  cursor_handle->cursor = NULL;
  if (err != 0) {
    return make_error_tuple(env, err);
  }

  return ATOM_OK;
}

ERL_NIF_TERM ebdb_nifs_open_cursor(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  ebdb_db_handle *db_handle;
  ebdb_txn_handle *txn_handle;
  ebdb_cursor_handle *cursor_handle;
  u_int32_t flags;
  int err;
  DBC *cursor;

  if (   !get_db_handle(env, argv[0], &db_handle) 
         || !get_txn_handle(env, argv[1], &txn_handle)
         || txn_handle == NULL // txn must be given
         || !decode_flags(env, argv[2], &flags)) {
    return enif_make_badarg(env);
  }

  err = db_handle->dbp->cursor(db_handle->dbp,
                               txn_handle->tid,
                               &cursor,
                               flags);

  if (err != 0) {
    return make_error_tuple(env, err);
  }

  cursor_handle = enif_alloc_resource(ebdb_cursor_RESOURCE,
                                      sizeof(ebdb_cursor_handle));
  if (cursor_handle == NULL) {
    cursor->close(cursor);
    return make_error_tuple(env, ENOMEM);
  }

  cursor_handle->cursor = cursor;

  ERL_NIF_TERM result = enif_make_resource(env, cursor_handle);
  enif_release_resource(cursor_handle);
  return enif_make_tuple2(env, ATOM_OK, result);  
}

ERL_NIF_TERM ebdb_nifs_cursor_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  ErlNifBinary key_bin, value_bin;
  DBT key, value;
  ebdb_cursor_handle *cursor_handle;
  u_int32_t flags;
  int err;

  if (!get_cursor_handle(env, argv[0], &cursor_handle)
      || !enif_inspect_binary(env, argv[1], &key_bin)
      || !decode_flags(env, argv[2], &flags)) {
    return enif_make_badarg(env);
  }

  
  char *buffer = enif_tsd_get(ebdb_buffer_TSD);
  if (buffer == NULL) {
    buffer = enif_alloc(BUFFER_SIZE);
    enif_tsd_set(ebdb_buffer_TSD, buffer);
  }

  key.data = buffer;
  key.size = key_bin.size;
  key.ulen = MAX_KEY_SIZE;
  key.flags = DB_DBT_USERMEM;
  memcpy(key.data, key_bin.data, key_bin.size);

  value.data = buffer+MAX_KEY_SIZE;
  value.size = 0;
  value.ulen = BUFFER_SIZE-MAX_KEY_SIZE;
  value.flags = DB_DBT_USERMEM;

  err = cursor_handle->cursor->get(cursor_handle->cursor,
                                   &key,
                                   &value,
                                   flags);

  if (err != 0) {
    return make_error_tuple(env, err);
  }

  if (!enif_alloc_binary(value.size, &value_bin)
      || !enif_alloc_binary(key.size, &key_bin)) {
    return make_error_tuple(env, ENOMEM);
  }
    
  return enif_make_tuple3(env, 
                          ATOM_OK, 
                          enif_make_binary(env, &key_bin),
                          enif_make_binary(env, &value_bin));  
}



ERL_NIF_TERM ebdb_nifs_txn_begin(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  ebdb_env_handle *env_handle;
  ebdb_txn_handle *parent_txn_handle;
  ebdb_txn_handle *txn_handle;
  u_int32_t flags;
  int err;
  DB_TXN *tx;

  if (   !get_env_handle(env, argv[0], &env_handle) 
         || env_handle==NULL // env must be given 
         || !get_txn_handle(env, argv[1], &parent_txn_handle)
         || !decode_flags(env, argv[2], &flags)) {
    return enif_make_badarg(env);
  }

  err = env_handle->envp->txn_begin(env_handle->envp,
                                    parent_txn_handle == NULL
                                    ? NULL
                                    : parent_txn_handle->tid,
                                    &tx,
                                    flags);

  if (err != 0) {
    return make_error_tuple(env, err);
  }

  txn_handle = enif_alloc_resource(ebdb_txn_RESOURCE,
                                   sizeof(ebdb_txn_handle));

  if (txn_handle == NULL) {
    tx->abort(tx);
    return make_error_tuple(env, ENOMEM);
  }

  txn_handle->tid = tx;

  ERL_NIF_TERM result = enif_make_resource(env, txn_handle);
  enif_release_resource(txn_handle);
  return enif_make_tuple2(env, ATOM_OK, result);  
}

ERL_NIF_TERM ebdb_nifs_txn_commit(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  ebdb_txn_handle *txn_handle;
  u_int32_t flags;
  int err;

  if (   !get_txn_handle(env, argv[0], &txn_handle) || txn_handle == NULL
      || !decode_flags(env, argv[1], &flags)) {
    return enif_make_badarg(env);
  }

  err = txn_handle->tid->commit(txn_handle->tid, flags);
  txn_handle->tid = NULL;
  if (err != 0) {
    return make_error_tuple(env, err);
  }

  return ATOM_OK;
}

ERL_NIF_TERM ebdb_nifs_txn_abort(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  ebdb_txn_handle *txn_handle;
  int err;

  if (   !get_txn_handle(env, argv[0], &txn_handle) || txn_handle == NULL) {
    return enif_make_badarg(env);
  }

  err = txn_handle->tid->abort(txn_handle->tid);
  txn_handle->tid = NULL;
  if (err != 0) {
    return make_error_tuple(env, err);
  }

  return ATOM_OK;
}

static void ebdb_db_resource_cleanup(ErlNifEnv* env, void* arg)
{
  ebdb_db_handle* handle = (ebdb_db_handle*)arg;

  if (handle->dbp != 0) {
    handle->dbp->close( handle->dbp, 0);
    handle->dbp = 0;
  }

  if (handle->env_handle != NULL) {
    enif_release_resource( handle->env_handle );
    handle->env_handle = NULL;
  }
  
}

static void ebdb_cursor_resource_cleanup(ErlNifEnv* env, void* arg)
{
  ebdb_cursor_handle* handle = (ebdb_cursor_handle*)arg;

  if (handle->cursor != 0) {
    handle->cursor->close( handle->cursor );
    handle->cursor = 0;
  }

}



static void ebdb_env_resource_cleanup(ErlNifEnv* env, void* arg)
{
  ebdb_env_handle* handle = (ebdb_env_handle*)arg;
  
  if (handle->envp != 0) {
    handle->envp->close( handle->envp, DB_FORCESYNC);
    handle->envp = 0;
  }
  
}

static void ebdb_txn_resource_cleanup(ErlNifEnv* env, void* arg)
{
  ebdb_txn_handle* handle = (ebdb_txn_handle*)arg;
  
  if (handle->tid != NULL) {
    handle->tid->abort( handle->tid );
    handle->tid = NULL;
  }
  
}

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
  printf("Initializing EBDB - Erlang API for Berkeley DB\n");
  printf("Copyright (c) 2011 by Trifork.  All rights reserved.\n");

  ebdb_db_RESOURCE = enif_open_resource_type
    (env, 
     "ebdb",
     "db_resource",
     &ebdb_db_resource_cleanup,
     ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER,
     0);

  ebdb_cursor_RESOURCE = enif_open_resource_type
    (env, 
     "ebdb",
     "cursor_resource",
     &ebdb_cursor_resource_cleanup,
     ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER,
     0);

  ebdb_env_RESOURCE = enif_open_resource_type
    (env, 
     "ebdb",
     "env_resource",
     &ebdb_env_resource_cleanup,
     ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER,
     0);

  ebdb_txn_RESOURCE = enif_open_resource_type
    (env, 
     "ebdb",
     "txn_resource",
     &ebdb_txn_resource_cleanup,
     ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER,
     0);

  enif_tsd_key_create("ebdb data buffer", &ebdb_buffer_TSD);

  // Initialize atoms that we use throughout the NIF.
  ATOM_INIT_CDB = enif_make_atom(env, "init_cdb");
  ATOM_INIT_LOCK = enif_make_atom(env, "init_lock");
  ATOM_INIT_LOG = enif_make_atom(env, "init_log");
  ATOM_INIT_MPOOL = enif_make_atom(env, "init_mpool");
  ATOM_INIT_REP = enif_make_atom(env, "init_rep");
  ATOM_INIT_TXN = enif_make_atom(env, "init_txn");

  ATOM_RECOVER = enif_make_atom(env, "recover");
  ATOM_RECOVER_FATAL = enif_make_atom(env, "recover_fatal");
  ATOM_USE_ENVIRON = enif_make_atom(env, "use_environ");
  ATOM_USE_ENVIRON_ROOT = enif_make_atom(env, "use_environ_root");
  
  ATOM_CREATE = enif_make_atom(env, "create");
  ATOM_LOCKDOWN = enif_make_atom(env, "lockdown");
  ATOM_PRIVATE = enif_make_atom(env, "private");
  ATOM_REGISTER = enif_make_atom(env, "register");
  ATOM_SYSTEM_MEM = enif_make_atom(env, "system_mem");
  ATOM_THREAD = enif_make_atom(env, "thread");
  ATOM_AUTO_COMMIT = enif_make_atom(env, "auto_commit");
  ATOM_EXCL = enif_make_atom(env, "excl");
  ATOM_MULTIVERSION = enif_make_atom(env, "multiversion");
  ATOM_NOMMAP = enif_make_atom(env, "nommap");
  ATOM_RDONLY = enif_make_atom(env, "rdonly");
  ATOM_READ_UNCOMMITTED = enif_make_atom(env, "read_uncommitted");
  ATOM_TRUNCATE = enif_make_atom(env, "truncate");
  ATOM_NOSYNC = enif_make_atom(env, "nosync");

  ATOM_CONSUME = enif_make_atom(env, "consume");
  ATOM_CONSUME_WAIT = enif_make_atom(env, "consume_wait");
  ATOM_RMW = enif_make_atom(env, "rmw");

  ATOM_APPEND = enif_make_atom(env, "append");
  ATOM_NODUPDATA = enif_make_atom(env, "nodupdata");
  ATOM_NOOVERWRITE = enif_make_atom(env, "nooverwrite");
  ATOM_OVERWRITE_DUP = enif_make_atom(env, "overwrite_dup");

  ATOM_NEXT = enif_make_atom(env, "next");
  ATOM_NEXT_DUP = enif_make_atom(env, "next_dup");
  ATOM_SET = enif_make_atom(env, "set");
  ATOM_SET_RANGE = enif_make_atom(env, "set_range");

  ATOM_NOTFOUND = enif_make_atom(env, "notfound");
  ATOM_KEYEMPTY = enif_make_atom(env, "keyempty");

  ATOM_ERROR = enif_make_atom(env, "error");
  ATOM_FALSE = enif_make_atom(env, "false");
  ATOM_OK = enif_make_atom(env, "ok");
  ATOM_TRUE = enif_make_atom(env, "true");
  ATOM_UNDEFINED = enif_make_atom(env, "undefined");

  ATOM_BTREE = enif_make_atom(env, "btree");
  ATOM_HASH = enif_make_atom(env, "hash");
  ATOM_QUEUE = enif_make_atom(env, "queue");
  ATOM_RECNO = enif_make_atom(env, "recno");
  ATOM_UNKNOWN = enif_make_atom(env, "unknown");

  ATOM_CURSOR_BULK = enif_make_atom(env, "cursor_bulk");
  ATOM_WRITECURSOR = enif_make_atom(env, "writecursor");

  ATOM_READ_COMMITTED = enif_make_atom(env, "read_committed");
  ATOM_TXN_BULK = enif_make_atom(env, "txn_bulk");
  ATOM_TXN_SYNC = enif_make_atom(env, "txn_sync");
  ATOM_TXN_NOSYNC = enif_make_atom(env, "txn_nosync");
  ATOM_TXN_WRITE_NOSYNC = enif_make_atom(env, "txn_write_nosync");
  ATOM_TXN_WAIT = enif_make_atom(env, "txn_wait");
  ATOM_TXN_NOWAIT = enif_make_atom(env, "txn_nowait");
  ATOM_TXN_SNAPSHOT = enif_make_atom(env, "txn_snapshot");

  return 0;
}

static ErlNifFunc nif_funcs[] =
{
    {"db_open_env", 2, ebdb_nifs_db_open_env},

    {"db_open", 6, ebdb_nifs_db_open},
    {"db_close", 2, ebdb_nifs_db_close},
    {"db_get", 4, ebdb_nifs_db_get},
    {"db_put", 5, ebdb_nifs_db_put},

    {"cursor_open", 3, ebdb_nifs_open_cursor},
    {"cursor_close", 1, ebdb_nifs_close_cursor},
    {"cursor_get", 3, ebdb_nifs_cursor_get},

    {"txn_begin", 3, ebdb_nifs_txn_begin},
    {"txn_commit", 2, ebdb_nifs_txn_commit},
    {"txn_abort", 1, ebdb_nifs_txn_abort},
};

ERL_NIF_INIT(ebdb_nifs, nif_funcs, &on_load, NULL, NULL, NULL);



