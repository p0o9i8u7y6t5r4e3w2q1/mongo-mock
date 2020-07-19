import { Db as _Db, MongoCallback, CollectionCreateOptions, IndexOptions } from "mongodb";
import { MongoClient } from "./mongo_client";
import * as Loki from "lokijs";
import { isFunction, keys, isObject } from "mingo/util";
import { asyncish, executeOperation } from "./utils";
import { EventEmitter } from "events";
import { Collection } from "./collection";
import { Cursor } from "./cursor";
import { opCreateIndex, idIndex } from "./operation/indexes";

function NotImplemented() {
  throw Error("Not Implemented.");
}

export const DB_CONST = {
  SYSTEM_INDEX_COLLECTION: "system.indexes",
};

export class Db extends EventEmitter {
  readonly databaseName: string;
  readonly _loki: Loki;
  _colls: { [key: string]: Collection } = {};
  _indexes_coll: Loki.Collection;

  constructor(dbname: string, client: MongoClient) {
    super();
    const badguy = /[ .$\/\\]/.exec(dbname);
    if (badguy) throw new Error(`database names cannot contain the character '${badguy[0]}'`);
    this.databaseName = dbname;
    this._loki = new Loki(dbname + "." + client.persist || "loki.db", { autosave: true });
    this._indexes_coll = this._loki.addCollection(DB_CONST.SYSTEM_INDEX_COLLECTION);

    for (const coll of this._loki.collections) {
      if (coll.name !== DB_CONST.SYSTEM_INDEX_COLLECTION)
        this._colls[name] = new Collection(this, {
          name: coll.name,
          loki: this._loki,
        });
    }
  }

  addUser = NotImplemented;
  admin = NotImplemented;
  authenticate = NotImplemented;

  collection<T>(name: string, options: any = {}, callback?: MongoCallback<Collection<T>>) {
    if (isFunction(options)) (callback = options), (options = {});
    else if (!isObject(options)) options = {};

    // strict mode
    if (options.strict) {
      if (!isFunction(callback))
        throw Error(`A callback is required in strict mode. While getting collection ${name}`);
      if (!this._colls[name])
        callback(
          // @ts-ignore
          Error(`Collection ${name} does not exist. Currently in strict mode.`),
          null
        );
      else callback(null, this._colls[name]);
    } else {
      if (!this._colls[name]) this.createCollection(name, callback);
      if (isFunction(callback)) callback(null, this._colls[name]);
      else return this._colls[name];
    }
  }

  collections(options?: any): Promise<Array<Collection>>;
  collections(callback: MongoCallback<Array<Collection>>): void;
  collections(options: any, callback: MongoCallback<Array<Collection>>): void;
  collections(options: any, callback?: MongoCallback<Array<Collection>>) {
    if (isFunction(options)) (callback = options), (options = {});
    else if (!isObject) options = {};

    const colls = Object.values(this._colls);
    if (isFunction(callback)) callback(null, colls);
    else return Promise.resolve(colls);
  }

  listCollections(filter?: any, options?: any) {
    return new Cursor(
      Object.values(this._colls).map((coll) => {
        // TODO: make it real
        return {
          name: coll.collectionName,
          options: {},
          type: "collection",
          info: { readOnly: false },
          idIndex: idIndex(coll),
        };
      })
    );
  }

  // only for test
  private collectionNames() {
    return keys(this._colls);
  }

  // @ts-ignore
  createIndex(name: string, fieldOrSpec: string | object, callback: MongoCallback<any>): void;
  createIndex(name: string, fieldOrSpec: string | object, options?: IndexOptions): Promise<any>;
  createIndex(
    name: string,
    fieldOrSpec: string | object,
    options: IndexOptions,
    callback: MongoCallback<any>
  ): void;
  createIndex(
    name: string,
    fieldOrSpec: string | object,
    options?: IndexOptions,
    callback?: MongoCallback<any>
  ) {
    if (isFunction(options)) (callback = options as any), (options = {});
    else if (!isObject) options = {};
    if (!this._colls[name]) throw Error(`Collection ${name} not existed`);

    return executeOperation<any>(opCreateIndex, [name, fieldOrSpec, options], this, callback);
  }

  createCollection<T = any>(name: string, callback: MongoCallback<Collection<T>>): void;
  createCollection<T = any>(name: string, options?: CollectionCreateOptions): Promise<Collection<T>>;
  createCollection<T = any>(
    name: string,
    options: CollectionCreateOptions,
    callback: MongoCallback<Collection<T>>
  ): void;
  createCollection<T = any>(name: string, options?: any, callback?: MongoCallback<Collection<T>>) {
    if (isFunction(options)) (callback = options), (options = {});
    else if (!isObject) options = {};
    if (!name) throw Error("name is mandatory");

    if (this._colls[name]) {
      if (options.strict) {
        const error = new Error(`Collection ${name} already exists. Currently in strict mode.`);
        // @ts-ignore
        if (isFunction(callback)) return callback(error, null);
        else return Promise.reject(error);
      }
    } else {
      this._loki.addCollection(name, { unique: ["_id"] });
      this._colls[name] = new Collection(this, { name, loki: this._loki });
    }
    if (callback) callback(null, this._colls[name]);
    else return Promise.resolve(this._colls[name]);
  }

  /**
   * @return
   * true when successfully drops a collection.
   * false when collection to drop does not exist.
   */
  // @ts-ignore
  dropCollection(name: string, options?: any): Promise<boolean>;
  dropCollection(name: string, callback: MongoCallback<boolean>): void;
  dropCollection(name: string, options: any, callback: MongoCallback<boolean>): void;
  dropCollection(name: string, options?: any, callback?: MongoCallback<boolean>) {
    if (isFunction(options)) (callback = options), (options = null);

    return asyncish(() => {
      if (this._colls[name]) {
        this._loki.removeCollection(name);
        delete this._colls[name];
        return true;
      } else return false;
    }, callback);
  }

  command = NotImplemented;
  dropDatabase = NotImplemented;

  _close(force: boolean) {
    this._loki.close();
    this.emit("close");
    this.removeAllListeners("close");
  }
}
