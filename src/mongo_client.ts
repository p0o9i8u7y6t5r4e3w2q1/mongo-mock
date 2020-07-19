import {
  MongoClient as _MongoClient,
  MongoClientOptions,
  MongoCallback,
  MongoClientCommonOption,
} from "mongodb";
import { isFunction, each } from "mingo/util";
import { parse as urlparse, UrlWithStringQuery } from "url";
import { Db } from "./db";
import { asyncish } from "./utils";
import { EventEmitter } from "events";
import { deprecate } from "util";

function NotImplemented() {
  throw Error("Not Implemented.");
}

export class MongoClient extends EventEmitter {
  static persist: string;
  readonly persist: string;
  private _dbs: { [key: string]: Db } = {};
  private _uri: UrlWithStringQuery;

  private _connected: boolean = false;

  constructor(uri: string, options?: MongoClientOptions) {
    super();
    this._uri = urlparse(uri);
    this.persist = MongoClient.persist;
  }

  static connect(uri: string, callback: MongoCallback<MongoClient>): void;
  // @ts-ignore
  static connect(uri: string, options?: MongoClientOptions): Promise<MongoClient>;
  static connect(uri: string, options: MongoClientOptions, callback: MongoCallback<MongoClient>): void;
  static connect(uri: string, arg2?: any, arg3?: any) {
    if (isFunction(arg2)) return new MongoClient(uri).connect(arg2);
    else return new MongoClient(uri, arg2).connect(arg3);
  }

  connect(): Promise<MongoClient>;
  connect(callback: MongoCallback<MongoClient>): void;
  connect(callback?: MongoCallback<MongoClient>) {
    if (callback && !isFunction(callback)) {
      throw new TypeError("`connect` only accepts a callback");
    }

    this._connected = true;
    if (callback) callback(null, this);
    else return Promise.resolve(this);
  }

  db(dbname?: string) {
    if (!dbname) dbname = this._uri.pathname.slice(1);
    if (!this._dbs[dbname]) this._dbs[dbname] = new Db(dbname, this);
    return this._dbs[dbname];
  }

  isConnected(options?: MongoClientCommonOption) {
    return this._connected;
  }

  close(force?: boolean): Promise<void>;
  close(callback: MongoCallback<void>): void;
  close(force: boolean, callback: MongoCallback<void>): void;
  close(force?: any, callback?: any): Promise<void> {
    if (isFunction(force)) (callback = force), (force = false);

    this._connected = false;
    this.emit("close", this);
    this.removeAllListeners("close");
    each(this._dbs, (val: Db) => val._close(force));
    if (callback) process.nextTick(callback, null);
    else return Promise.resolve();
  }

  startSession = NotImplemented;
  withSession = NotImplemented;
  watch = NotImplemented;

  /*
  watch<T = any>(
    pipeline?: object[],
    options?: ChangeStreamOptions & { session?: ClientSession }
  ): ChangeStream<TSchema> {
    NotImplemented;
  }
  */

  logout = deprecate((options: any, callback: Function) => {
    if (isFunction(options)) (callback = options), (options = null);
    if (typeof callback === "function") callback(null, true);
  }, "Multiple authentication is prohibited on a connected client, please only authenticate once per MongoClient");
}
