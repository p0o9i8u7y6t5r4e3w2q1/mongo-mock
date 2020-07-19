import {
  MongoCallback,
  FindOneOptions,
  FilterQuery,
  InsertOneWriteOpResult,
  InsertWriteOpResult,
  OptionalId,
  WithId,
  CollectionInsertOneOptions,
  CollectionInsertManyOptions,
  DeleteWriteOpResultObject,
  CommonOptions,
  IndexOptions,
  WriteOpResult,
  UpdateQuery,
  UpdateOneOptions,
  UpdateManyOptions,
  UpdateWriteOpResult,
  FindOneAndUpdateOption,
  FindAndModifyWriteOpResultObject,
} from "mongodb";
import { Collection } from "../collection";
import { serialize, deserialize } from "../utils";
import { MongoError } from "../error";
import { checkUniqueIndexes } from "./indexes";
import { modify } from "./modify";
import { filterToObj } from "./filter_to_obj";

export function getPreprocess(coll: Collection) {
  return function preprocess(input: any | any[]) {
    serialize(input);
    checkUniqueIndexes(coll, input);
  };
}

export function opInsertOne<T>(
  this: Collection<T>,
  doc: OptionalId<T>,
  options: CollectionInsertOneOptions
): InsertOneWriteOpResult<WithId<T>> {
  if (Array.isArray(doc)) throw new MongoError("doc parameter must be an object");
  const insert_doc = this._loki_coll.insertOne(doc);

  return {
    insertedId: insert_doc._id,
    insertedCount: 1,
    result: { ok: 1, n: 1 },
    connection: {},
    ops: [deserialize(insert_doc)],
  };
}

export function opInsertMany<T>(
  this: Collection<T>,
  docs: OptionalId<T>[],
  options: CollectionInsertOneOptions
): InsertWriteOpResult<WithId<T>> {
  const insert_docs = docs.map((item) => this._loki_coll.insertOne(item));

  return {
    insertedIds: insert_docs.map((item) => item._id),
    insertedCount: insert_docs.length,
    result: { ok: 1, n: insert_docs.length },
    connection: {},
    ops: deserialize(insert_docs),
  };
}

function _update<T>(
  data_to_update: any[],
  filter: FilterQuery<T>,
  update: UpdateQuery<T>,
  options?: UpdateManyOptions | UpdateOneOptions
) {
  if (!data_to_update) data_to_update = [];
  else if (!Array.isArray(data_to_update)) data_to_update = [data_to_update];

  // ---- update doc -----
  let update_count = data_to_update.length;

  // @ts-ignore
  let result: InnerUpdateOpResult = {
    result: { n: update_count, nModified: update_count, ok: 1 },
    modifiedCount: update_count,
    matchedCount: update_count,
    _origin: data_to_update,
  };

  if (update_count) {
    const modified_data = modify(data_to_update, update);
    // console.log({ modified_data });
    for (const item of modified_data) {
      this._loki_coll.update(item);
    }
    result._modified_data = deserialize(modified_data);
  }

  // ---- insert doc -----
  if (options.upsert && !update_count) {
    let data_to_insert = filterToObj(filter);
    data_to_insert = modify(data_to_insert, update);

    if (update["$setOnInsert"]) {
      data_to_insert = modify(data_to_insert, { $set: update["$setOnInsert"] });
    }
    const inserted_data = this._loki_coll.insertOne(data_to_insert);
    result.result.n = 1;
    result.upsertedId = { _id: inserted_data._id };
    result.upsertedCount = 1;
    result._upserted_data = deserialize(inserted_data);
  }

  return result;
}
