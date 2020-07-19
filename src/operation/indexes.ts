import { IndexOptions, IndexSpecification } from "mongodb";
import { Db, DB_CONST } from "../db";
import { Collection } from "../collection";
import { MongoError } from "../error";
import { stringify } from "../utils";
import { isString, keys } from "mingo/util";

export interface IndexInfo {
  v: number;
  key: { [key: string]: number };
  name: string;
  ns: string;
  unique?: boolean;
}

function keyStringify(index_key: any) {
  if (isString(index_key)) return index_key + "_1";
  return keys(index_key)
    .map((key) => `${key}_${index_key[key]}`)
    .join("_");
}

function keyify(index_key: any) {
  if (isString(index_key)) return { [index_key]: 1 };
  return index_key;
}

export function idIndex(coll: Collection) {
  return {
    v: 2,
    name: "_id_",
    key: { _id: 1 },
    ns: coll.namespace,
  };
}

export function opCreateIndex(this: Db, coll_name: string, spec: string | any, options: IndexOptions) {
  const index_info: IndexInfo = {
    v: 2,
    key: keyify(spec),
    name: options.name || keyStringify(spec),
    ns: `${this.databaseName}.${coll_name}`,
  };
  if (options.unique) index_info.unique = true;

  const indexes_coll = this._indexes_coll;
  // check if index already exists
  let prev_index =
    indexes_coll.findOne({ ns: index_info.ns, key: { $eq: index_info.key } }) ||
    indexes_coll.findOne({ ns: index_info.ns, name: index_info.name });
  if (prev_index) return prev_index.name;

  // insert index
  indexes_coll.insertOne(index_info);

  // create simple index in $loki
  for (const key of keys(index_info.key)) {
    this._loki.getCollection(coll_name).ensureIndex(key, true);
  }

  return index_info.name;
}

// $----- check unique index ----$

function obj_from_index(obj: any, key: any) {
  return keys(key).reduce((result, keyName) => {
    result[keyName] = obj[keyName];
    return result;
  }, {});
}

// suppose obj has been serialized
function _checkUniqueIndex(coll: Collection, uniqueIndexes: IndexInfo[], obj: any) {
  // check unique indexes
  for (const index of uniqueIndexes) {
    const filter = obj_from_index(obj, index.key);
    const prev_obj = coll._loki_coll.findOne(filter);
    if (prev_obj && prev_obj.$loki !== obj.$loki) {
      throw new MongoError(
        `E11000 duplicate key error collection: ${index.ns} index: ${index.name} dup key: ` +
          stringify(filter),
        11000
      );
    }
  }
}

export function checkUniqueIndexes(coll: Collection, obj: any | any[]) {
  const uniqueIndexes = coll._uniqueIndexes();
  if (Array.isArray(obj)) for (const item of obj) _checkUniqueIndex(coll, uniqueIndexes, item);
  else _checkUniqueIndex(coll, uniqueIndexes, obj);
}
