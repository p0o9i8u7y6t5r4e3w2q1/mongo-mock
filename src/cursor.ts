require("mingo/init/system");
import {
  Cursor as _Cursor,
  MongoCallback,
  CursorCommentOptions,
  IteratorCallback,
  EndCallback,
} from "mongodb";
import { isFunction, isObject } from "mingo/util";
import { asyncish, deserialize } from "./utils";
import { modify } from "./operation/modify";
import { Cursor as MingoCursor } from "mingo/cursor";
import { EventEmitter } from "events";
import mingo from "mingo";

function NotImplemented() {
  throw Error("Not Implemented.");
}

const CursorState = {
  INIT: 0,
  OPEN: 1,
  CLOSED: 2,
};

export class Cursor<T = any> extends EventEmitter {
  private _closed: boolean = false;
  private _s: { idx: number; state: number };
  private _cursor: MingoCursor;
  private _doc: any[];

  constructor(doc: any[], options?: any) {
    super();
    if (!isObject(options)) options = {};
    if (!doc) doc = [doc];

    this._cursor = new MingoCursor(doc, () => true, options.projection || {}, { idKey: "_id" });
    this._doc = doc;
    this._s = { idx: 0, state: CursorState.INIT };

    process.nextTick(() => this._triggerStream());
  }

  private _triggerStream() {
    for (const item of this._all()) {
      this.emit("data", item);
    }
    this.emit("end");
  }

  sort(keyOrList: string | object[] | object, direction?: number): Cursor<T> {
    this._cursor = this._cursor.sort(keyOrList);
    return this;
  }

  project(value: object): Cursor<T> {
    return new Cursor(this._cursor.all(), { projection: value });
  }

  // @ts-ignore
  toArray(): Promise<T[]>;
  toArray(callback: MongoCallback<T[]>): void;
  toArray(callback?: MongoCallback<T[]>) {
    return asyncish(() => {
      return deserialize(this._cursor.all());
    }, callback);
  }

  // @ts-ignore
  batchSize = NotImplemented;
  // @ts-ignore
  clone = NotImplemented;

  isClosed() {
    return this._closed;
  }

  limit(value: number): Cursor<T> {
    this._cursor = this._cursor.limit(value);
    return this;
  }

  // @ts-ignore
  hasNext(): Promise<boolean>;
  hasNext(callback: MongoCallback<boolean>): void;
  hasNext(callback?: MongoCallback<boolean>) {
    return asyncish(() => this._cursor.hasNext(), callback);
  }

  skip(value: number): Cursor<T> {
    this._cursor = this._cursor.skip(value);
    return this;
  }

  map<U>(transform: (document: T) => U): Cursor<U> {
    return new Cursor<U>(this._cursor.map(transform));
  }

  // @ts-ignore
  next(): Promise<T | null>;
  next(callback: MongoCallback<T | null>): void;
  next(callback?: MongoCallback<T | null>) {
    return asyncish(() => this._cursor.next(), callback);
  }

  // @ts-ignore
  forEach(iterator: IteratorCallback<T>): Promise<void>;
  forEach(iterator: IteratorCallback<T>, callback: EndCallback): void;
  forEach(iterator: IteratorCallback<T>, callback?: EndCallback) {
    return asyncish<void>(() => this._cursor.all().forEach(iterator), callback);
  }

  filter(filter: object): Cursor<T> {
    this._cursor = mingo.find(this._cursor.all(), filter);
    return this;
  }

  each(callback: Function) {
    this.rewind();
    this._s.state = CursorState.INIT;
    for (const item of this._cursor.all()) callback(null, item);
  }

  _all() {
    return this._cursor.all();
  }

  // @ts-ignore
  count(callback: MongoCallback<number>): void;
  count(applySkipLimit: boolean, callback: MongoCallback<number>): void;
  count(options: CursorCommentOptions, callback: MongoCallback<number>): void;
  count(applySkipLimit: boolean, options: CursorCommentOptions, callback: MongoCallback<number>): void;
  count(applySkipLimit?: boolean, options?: CursorCommentOptions): Promise<number>;
  count(applySkipLimit?: boolean, options?: CursorCommentOptions, callback?: MongoCallback<number>) {
    if (isFunction(applySkipLimit)) (callback = applySkipLimit as any), (applySkipLimit = true);
    else if (isFunction(options)) callback = options as any;
    if (isObject(applySkipLimit)) (options = applySkipLimit as any), (applySkipLimit = true);
    else if (!isObject(options)) options = {};

    return asyncish(() => {
      return applySkipLimit ? this._cursor.count() : this._doc.length;
    }, callback);
  }

  stream(options?: { transform?: (document: T) => any }): Cursor<T> {
    return this;
  }

  rewind() {
    this._s.idx = 0;
  }

  // @ts-ignore
  close(options?: any): Promise<this>;
  close(callback: MongoCallback<this>): void;
  close(options: any, callback: MongoCallback<this>): void;
  close(options?: any, callback?: MongoCallback<this>) {
    if (isFunction(options)) (callback = options), (options = null);
    this._s.state = CursorState.CLOSED;

    return asyncish(() => this, callback);
  }
}
