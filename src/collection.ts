import {
  Collection as _Collection,
  CollectionInsertManyOptions,
  CollectionInsertOneOptions,
  CommonOptions,
  DeleteWriteOpResultObject,
  FilterQuery,
  FindAndModifyWriteOpResultObject,
  FindOneAndUpdateOption,
  FindOneOptions,
  IndexOptions,
  InsertOneWriteOpResult,
  InsertWriteOpResult,
  MongoCallback,
  MongoCountPreferences,
  OptionalId,
  UpdateManyOptions,
  UpdateOneOptions,
  UpdateQuery,
  UpdateWriteOpResult,
  WithId,
  WriteOpResult,
} from "mongodb";
import { isFunction, cloneDeep, isObject, each, isString } from "mingo/util";
import { asyncish, deserialize, IdUtil, executeOperation } from "./utils";
import * as Loki from "lokijs";
import { Db } from "./db";
import { Cursor } from "./cursor";
import { modify } from "./operation/modify";
import { filterToObj } from "./operation/filter_to_obj";
import { idIndex, IndexInfo } from "./operation/indexes";
import { aggregateFn } from "./operation/aggregate";
import { getPreprocess, opInsertOne, opInsertMany } from "./operation/collection_ops";

function NotImplemented() {
  throw Error("Not Implemented.");
}

function wrapFilter(query: any) {
  if (query && query._id) query = Object.assign({}, query, { _id: IdUtil.wrap(query._id) });
  return query;
}

interface CollectionState {
  name: string;
  loki: Loki;
}

export class Collection<T = OptionalId<any>> {
  _db: Db;
  readonly collectionName: string;
  readonly namespace: string;
  _loki: Loki;
  _preprocess = getPreprocess(this);

  constructor(db: Db, state: CollectionState) {
    this._db = db;
    this.collectionName = state.name;
    this.namespace = db.databaseName + "." + state.name;

    const _loki_coll = db._loki.getCollection(state.name);
    this._loki = state.loki;

    _loki_coll.on(["pre-insert", "pre-update"], this._preprocess);
  }

  get _loki_coll() {
    let coll = this._loki.getCollection(this.collectionName);
    if (!coll) {
      coll = this._loki.addCollection(this.collectionName);
      coll.on(["pre-insert", "pre-update"], this._preprocess);
    }
    return coll;
  }

  aggregate(pipeline: any[]) {
    return {
      toArray: () => {
        return aggregateFn(this._db, this, pipeline);
      },
    };
  }

  /** @deprecated Use insertOne, insertMany or bulkWrite */
  // @ts-ignore
  insert(
    docs: OptionalId<T> | Array<OptionalId<T>>,
    callback: MongoCallback<InsertWriteOpResult<WithId<T>>>
  ): void;
  /** @deprecated Use insertOne, insertMany or bulkWrite */
  insert(
    docs: OptionalId<T> | Array<OptionalId<T>>,
    options?: CollectionInsertOneOptions
  ): Promise<InsertWriteOpResult<WithId<T>>>;
  /** @deprecated Use insertOne, insertMany or bulkWrite */
  insert(
    docs: OptionalId<T> | Array<OptionalId<T>>,
    options: CollectionInsertOneOptions,
    callback: MongoCallback<InsertWriteOpResult<WithId<T>>>
  ): void;
  insert(
    docs: OptionalId<T> | Array<OptionalId<T>>,
    options?: CollectionInsertOneOptions,
    callback?: MongoCallback<InsertWriteOpResult<WithId<T>>>
  ): void {
    if (!Array.isArray(docs)) docs = [docs];
    return this.insertMany(docs, options, callback);
  }

  // @ts-ignore
  insertOne(
    docs: OptionalId<T>,
    options?: CollectionInsertOneOptions
  ): Promise<InsertOneWriteOpResult<WithId<T>>>;
  insertOne(docs: OptionalId<T>, callback: MongoCallback<InsertOneWriteOpResult<WithId<T>>>): void;
  insertOne(
    docs: OptionalId<T>,
    options: CollectionInsertOneOptions,
    callback: MongoCallback<InsertOneWriteOpResult<WithId<T>>>
  ): void;
  insertOne(docs: OptionalId<T>, options?: any, callback?: MongoCallback<InsertOneWriteOpResult<WithId<T>>>) {
    if (isFunction(options)) (callback = options), (options = {});
    else if (!isObject(options)) options = {};

    return executeOperation<InsertOneWriteOpResult<WithId<T>>>(opInsertOne, [docs, options], this, callback);
  }

  // @ts-ignore
  insertMany(docs: Array<OptionalId<T>>, callback: MongoCallback<InsertWriteOpResult<WithId<T>>>): void;
  insertMany(
    docs: Array<OptionalId<T>>,
    options?: CollectionInsertManyOptions
  ): Promise<InsertWriteOpResult<WithId<T>>>;
  insertMany(
    docs: Array<OptionalId<T>>,
    options: CollectionInsertManyOptions,
    callback: MongoCallback<InsertWriteOpResult<WithId<T>>>
  ): void;
  insertMany(
    docs: Array<OptionalId<T>>,
    options?: CollectionInsertManyOptions,
    callback?: MongoCallback<InsertWriteOpResult<WithId<T>>>
  ) {
    if (isFunction(options)) (callback = options as any), (options = {});
    else if (!isObject(options)) options = {};
    if (!Array.isArray(docs)) docs = [docs];

    return executeOperation<InsertWriteOpResult<WithId<T>>>(opInsertMany, [docs, options], this, callback);
  }

  find<K = T>(query?: FilterQuery<K>, options?: FindOneOptions) {
    return new Cursor<K>(this._loki_coll.find(wrapFilter(query)), options);
  }

  // @ts-ignore
  findOne<K = T>(filter: FilterQuery<K>, callback: MongoCallback<K | null>): void;
  findOne<K = T>(filter: FilterQuery<K>, options?: FindOneOptions): Promise<K | null>;
  findOne<K = T>(filter: FilterQuery<K>, options: FindOneOptions, callback: MongoCallback<K | null>): void;
  findOne<K = T>(filter: FilterQuery<K>, options?: FindOneOptions, callback?: MongoCallback<K | null>) {
    if (isFunction(options)) (callback = options as any), (options = {});
    else if (!isObject(options)) options = {};

    return asyncish<K>(() => {
      return deserialize(this.find(filter, options).limit(1)._all()[0]);
    }, callback);
  }

  /** @deprecated use updateOne, updateMany or bulkWrite */
  // @ts-ignore
  update(
    filter: FilterQuery<T>,
    update: UpdateQuery<T> | Partial<T>,
    callback: MongoCallback<WriteOpResult>
  ): void;
  /** @deprecated use updateOne, updateMany or bulkWrite */
  update(
    filter: FilterQuery<T>,
    update: UpdateQuery<T> | Partial<T>,
    options?: UpdateOneOptions & { multi?: boolean }
  ): Promise<WriteOpResult>;
  /** @deprecated use updateOne, updateMany or bulkWrite */
  update(
    filter: FilterQuery<T>,
    update: UpdateQuery<T> | Partial<T>,
    options: UpdateOneOptions & { multi?: boolean },
    callback: MongoCallback<WriteOpResult>
  ): void;
  update(
    filter: FilterQuery<T>,
    update: UpdateQuery<T> | Partial<T>,
    options?: UpdateOneOptions & { multi?: boolean },
    callback?: MongoCallback<WriteOpResult>
  ) {
    if (isFunction(options)) (callback = options as any), (options = {});
    else if (!isObject(options)) options = {};
    // @ts-ignore
    if (options.multi) return this.updateMany(filter, update, options, callback);
    // @ts-ignore
    else return this.updateOne(filter, update, options, callback);
  }

  private _update(
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

  // @ts-ignore
  updateOne(
    filter: FilterQuery<T>,
    update: UpdateQuery<T> | Partial<T>,
    callback: MongoCallback<UpdateWriteOpResult>
  ): void;
  updateOne(
    filter: FilterQuery<T>,
    update: UpdateQuery<T> | Partial<T>,
    options?: UpdateOneOptions
  ): Promise<UpdateWriteOpResult>;
  updateOne(
    filter: FilterQuery<T>,
    update: UpdateQuery<T> | Partial<T>,
    options: UpdateOneOptions,
    callback: MongoCallback<UpdateWriteOpResult>
  ): void;
  updateOne(
    filter: FilterQuery<T>,
    update: UpdateQuery<T> | Partial<T>,
    options?: UpdateOneOptions,
    callback?: MongoCallback<UpdateWriteOpResult>
  ) {
    if (isFunction(options)) (callback = options as any), (options = {});
    else if (!isObject(options)) options = {};

    return asyncish<UpdateWriteOpResult>(() => {
      // console.log({ oneAll: this._loki_coll.find() });
      const data_to_update = this._loki_coll.findOne(wrapFilter(filter));
      // console.log({ data_to_update, filter, update, options });
      const result = this._update(data_to_update, filter, update, options);
      delete result._modified_data;
      delete result._upserted_data;
      delete result._origin;
      return result as UpdateWriteOpResult;
    }, callback);
  }

  // @ts-ignore
  updateMany(
    filter: FilterQuery<T>,
    update: UpdateQuery<T> | Partial<T>,
    callback: MongoCallback<UpdateWriteOpResult>
  ): void;
  updateMany(
    filter: FilterQuery<T>,
    update: UpdateQuery<T> | Partial<T>,
    options?: UpdateManyOptions
  ): Promise<UpdateWriteOpResult>;
  updateMany(
    filter: FilterQuery<T>,
    update: UpdateQuery<T> | Partial<T>,
    options: UpdateManyOptions,
    callback: MongoCallback<UpdateWriteOpResult>
  ): void;
  updateMany(
    filter: FilterQuery<T>,
    update: UpdateQuery<T> | Partial<T>,
    options?: UpdateManyOptions,
    callback?: MongoCallback<UpdateWriteOpResult>
  ) {
    if (isFunction(options)) (callback = options as any), (options = {});
    else if (!isObject(options)) options = {};

    return asyncish<UpdateWriteOpResult>(() => {
      // console.log({ manyAll: this._loki_coll.find() });
      const data_to_update = this._loki_coll.find(wrapFilter(filter));
      // console.log({ data_to_update, filter, update, options });
      const result = this._update(data_to_update, filter, update, options);
      delete result._modified_data;
      delete result._upserted_data;
      delete result._origin;
      return result as UpdateWriteOpResult;
    }, callback);
  }

  /** @deprecated Use insertOne, insertMany, updateOne or updateMany */
  // @ts-ignore
  save(doc: OptionalId<T>, callback: MongoCallback<WriteOpResult>): void;
  /** @deprecated Use insertOne, insertMany, updateOne or updateMany */
  save(doc: OptionalId<T>, options?: CommonOptions): Promise<WriteOpResult>;
  /** @deprecated Use insertOne, insertMany, updateOne or updateMany */
  save(doc: OptionalId<T>, options: CommonOptions, callback: MongoCallback<WriteOpResult>): void;
  save(doc: OptionalId<T>, options?: CommonOptions, callback?: MongoCallback<WriteOpResult>) {
    if (isFunction(options)) (callback = options as any), (options = {});
    else if (!isObject(options)) options = {};

    if (doc._id) {
      (options as UpdateOneOptions).upsert = true;
      // @ts-ignore
      return this.updateOne({ _id: doc._id }, { $set: doc }, options, callback);
    } else return this.insertOne(doc, options, callback);
  }

  /** @deprecated Use use deleteOne, deleteMany or bulkWrite */
  // @ts-ignore
  remove(selector: object, callback: MongoCallback<WriteOpResult>): void;
  /** @deprecated Use use deleteOne, deleteMany or bulkWrite */
  remove(selector: object, options?: CommonOptions & { single?: boolean }): Promise<WriteOpResult>;
  /** @deprecated Use use deleteOne, deleteMany or bulkWrite */
  remove(
    selector: object,
    options: CommonOptions & { single?: boolean },
    callback?: MongoCallback<WriteOpResult>
  ): void;
  remove(
    selector: object,
    options?: CommonOptions & { single?: boolean },
    callback?: MongoCallback<WriteOpResult>
  ) {
    if (isFunction(options)) (callback = options as any), (options = {});
    else if (!isObject(options)) options = {};

    if (options.single) return this.deleteOne(selector, options, callback);
    else return this.deleteMany(selector, options, callback);
  }

  // @ts-ignore
  deleteOne(filter: FilterQuery<T>, callback: MongoCallback<DeleteWriteOpResultObject>): void;
  deleteOne(
    filter: FilterQuery<T>,
    options?: CommonOptions & { bypassDocumentValidation?: boolean }
  ): Promise<DeleteWriteOpResultObject>;
  deleteOne(
    filter: FilterQuery<T>,
    options: CommonOptions & { bypassDocumentValidation?: boolean },
    callback: MongoCallback<DeleteWriteOpResultObject>
  ): void;
  deleteOne(
    filter: FilterQuery<T>,
    options?: CommonOptions & { bypassDocumentValidation?: boolean },
    callback?: MongoCallback<DeleteWriteOpResultObject>
  ): void {
    if (isFunction(options)) (callback = options as any), (options = {});
    else if (!isObject(options)) options = {};

    return asyncish<DeleteWriteOpResultObject>(() => {
      const data_to_remove = this._loki_coll.chain().find(filter).limit(1).data();
      this._loki_coll.remove(data_to_remove);

      return {
        result: { n: data_to_remove.length, ok: 1 },
        deletedCount: data_to_remove.length,
        connection: this._db,
      };
    }, callback);
  }

  // @ts-ignore
  deleteMany(filter: FilterQuery<T>, callback: MongoCallback<DeleteWriteOpResultObject>): void;
  deleteMany(filter: FilterQuery<T>, options?: CommonOptions): Promise<DeleteWriteOpResultObject>;
  deleteMany(
    filter: FilterQuery<T>,
    options: CommonOptions,
    callback: MongoCallback<DeleteWriteOpResultObject>
  ): void;
  deleteMany(
    filter: FilterQuery<T>,
    options?: CommonOptions,
    callback?: MongoCallback<DeleteWriteOpResultObject>
  ): void {
    if (isFunction(options)) (callback = options as any), (options = {});
    else if (!isObject(options)) options = {};

    return asyncish<DeleteWriteOpResultObject>(() => {
      const data_to_remove = this._loki_coll.chain().find(filter).data();
      this._loki_coll.remove(data_to_remove);

      return {
        result: { n: data_to_remove.length, ok: 1 },
        deletedCount: data_to_remove.length,
        connection: this._db,
      };
    }, callback);
  }

  // @ts-ignore
  findOneAndUpdate(
    filter: FilterQuery<T>,
    update: UpdateQuery<T> | T,
    callback: MongoCallback<FindAndModifyWriteOpResultObject<T>>
  ): void;
  findOneAndUpdate(
    filter: FilterQuery<T>,
    update: UpdateQuery<T> | T,
    options?: FindOneAndUpdateOption
  ): Promise<FindAndModifyWriteOpResultObject<T>>;
  findOneAndUpdate(
    filter: FilterQuery<T>,
    update: UpdateQuery<T> | T,
    options: FindOneAndUpdateOption,
    callback: MongoCallback<FindAndModifyWriteOpResultObject<T>>
  ): void;
  findOneAndUpdate(
    filter: FilterQuery<T>,
    update: UpdateQuery<T> | T,
    options?: FindOneAndUpdateOption,
    callback?: MongoCallback<FindAndModifyWriteOpResultObject<T>>
  ) {
    if (isFunction(options)) (callback = options as any), (options = {});
    else if (!isObject(options)) options = {};
    options = Object.assign({ returnOriginal: true }, options);

    return asyncish<FindAndModifyWriteOpResultObject<T>>(() => {
      // console.log({ oneAll: this._loki_coll.find() });
      const data_to_update = this._loki_coll.findOne(wrapFilter(filter));
      // console.log({ data_to_update, filter, update, options });
      const result = this._update(data_to_update, filter, update, options);
      return {
        value: options.returnOriginal
          ? result._origin && result._origin[0]
          : result._upserted_data || (result._modified_data && result._modified_data[0]),
        lastErrorObject: {
          upserted: result.upsertedCount,
          updatedExisting: Boolean(result._modified_data),
          n: result.result.n,
        },
        ok: 1,
      };
    }, callback);
  }

  // @ts-ignore
  drop(options?: any): Promise<any>;
  drop(callback: MongoCallback<any>): void;
  drop(options: any, callback: MongoCallback<any>): void;
  drop(options?: any, callback?: MongoCallback<any>) {
    if (isFunction(options)) (callback = options), (options = {});
    else if (!isObject(options)) options = {};

    return this._db.dropCollection(this.collectionName, options, callback);
  }

  /** @deprecated Use countDocuments or estimatedDocumentCount */
  // @ts-ignore
  count(callback: MongoCallback<number>): void;
  count(query: FilterQuery<T>, callback: MongoCallback<number>): void;
  count(query?: FilterQuery<T>, options?: MongoCountPreferences): Promise<number>;
  count(query: FilterQuery<T>, options: MongoCountPreferences, callback: MongoCallback<number>): void;
  count(query?: FilterQuery<T>, options?: MongoCountPreferences, callback?: MongoCallback<number>) {
    return this.countDocuments(query, options, callback);
  }

  // @ts-ignore
  countDocuments(callback: MongoCallback<number>): void;
  countDocuments(query: FilterQuery<T>, callback: MongoCallback<number>): void;
  countDocuments(query?: FilterQuery<T>, options?: MongoCountPreferences): Promise<number>;
  countDocuments(
    query: FilterQuery<T>,
    options: MongoCountPreferences,
    callback: MongoCallback<number>
  ): void;
  countDocuments(query: FilterQuery<T>, options: MongoCountPreferences, callback: MongoCallback<number>) {
    if (isFunction(query)) (callback = query as any), (query = null);
    else if (isFunction(options)) callback = options as any;
    if (!isObject(options)) options = {};

    let result = this.find(query);
    if (options.skip) result = result.skip(options.skip);
    if (options.limit) result = result.limit(options.limit);
    return result.count(callback);
  }

  // @ts-ignore
  estimatedDocumentCount(callback: MongoCallback<number>): void;
  estimatedDocumentCount(options?: MongoCountPreferences): Promise<number>;
  estimatedDocumentCount(options: MongoCountPreferences, callback: MongoCallback<number>): void;
  estimatedDocumentCount(options: MongoCountPreferences, callback: MongoCallback<number>) {
    return this.countDocuments(null, options, callback);
  }

  // @ts-ignore
  createIndex(fieldOrSpec: string | any, callback: MongoCallback<string>): void;
  createIndex(fieldOrSpec: string | any, options?: IndexOptions): Promise<string>;
  createIndex(fieldOrSpec: string | any, options: IndexOptions, callback: MongoCallback<string>): void;
  createIndex(fieldOrSpec: string | any, options?: IndexOptions, callback?: MongoCallback<string>) {
    return this._db.createIndex(this.collectionName, fieldOrSpec, options, callback);
  }

  indexInformation(options?: any, callback?: MongoCallback<IndexInfo[]>) {
    if (isFunction(options)) (callback = options), (options = {});
    else if (!isObject(options)) options = {};
    return executeOperation(this.indexes, [], this, callback);
    /*
    const result = { _id_: this._loki_coll.uniqueNames.map((unique) => [unique, 1]) };
    each(this._loki_coll.binaryIndices, (value, key: string) => {
      result[key + "_1"] = [[key, 1]];
    });
    return;
    */
  }

  indexes(): IndexInfo[] {
    const result = this._db._indexes_coll.find({ ns: this.namespace });
    result.unshift(idIndex(this));
    return result;
  }

  _uniqueIndexes(): IndexInfo[] {
    const result = this._db._indexes_coll.find({ ns: this.namespace, unique: true });
    result.unshift(idIndex(this));
    return result;
  }

  // @ts-ignore
  distinct = NotImplemented;
  // @ts-ignore
  writeConcern = NotImplemented;
  // @ts-ignore
  bulkWrite = NotImplemented;
}
