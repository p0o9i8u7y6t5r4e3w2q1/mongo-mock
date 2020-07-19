import { isString, isObject, isObjectLike, keys, isFunction } from "mingo/util";
import { MongoCallback } from "mongodb";
import ObjectID from "bson-objectid";

export function stringify(obj: any) {
  if (isString(obj)) return obj;
  return (
    "{ " +
    keys(obj)
      .map((key) => `${key}: ${obj[key]}`)
      .join(", ") +
    " }"
  );
}

export function asyncish<T>(fn: () => Promise<T> | T): Promise<T>;
export function asyncish<T>(fn: () => Promise<T> | T, callback: MongoCallback<T>): void;
export function asyncish<T>(fn: () => Promise<T> | T, callback?: MongoCallback<T>) {
  return executeOperation(fn, [], null, callback);
}

function _runFn<T>(fn: (...args: any[]) => Promise<T> | T, args: any[], context: any) {
  if (context) return fn.call(context, ...args);
  else return fn(...args);
}

export function executeOperation<T>(
  fn: (...args: any[]) => Promise<T> | T,
  args: any[],
  context: any,
  callback?: MongoCallback<T>
) {
  if (!args) args = [];
  else if (!Array.isArray(args)) args = [args];

  if (isFunction(callback)) {
    new Promise(function (resolve) {
      setTimeout(resolve, 0);
    })
      .then(() => _runFn(fn, args, context))
      .then(
        (val: T) => callback(null, val),
        (err) => callback(err, null)
      );
    return;
  } else {
    return new Promise(function (resolve) {
      setTimeout(resolve, 0);
    }).then(() => _runFn(fn, args, context));
  }
}

export const IdUtil = {
  wrap(id: any) {
    if (isObjectLike(id) && id._bsontype && id._bsontype.toLowerCase() === "objectid") {
      return `objectid$${id.str}`;
    } else return id;
  },
  unwrap(id: any) {
    if (isString(id) && id.startsWith("objectid$")) return new ObjectID(id.slice(9));
    else return id;
  },
};

function _serialize(item: any) {
  if (!item._id) item._id = new ObjectID();
  item._id = IdUtil.wrap(item._id);
}

export function serialize(data: any) {
  if (Array.isArray(data)) {
    for (const item of data) _serialize(item);
  } else _serialize(data);
  return data;
}

// $----- deserialize -----$
export function _deserialize(item: any) {
  if (!isObjectLike(item)) return item;

  const newObj = Object.assign({}, item);
  if (newObj._id) newObj._id = IdUtil.unwrap(newObj._id);
  delete newObj.$loki;
  delete newObj.meta;
  return newObj;
}

export function deserialize(data: any | any[]) {
  if (Array.isArray(data)) return data.map(_deserialize);
  else if (isObject(data)) return _deserialize(data);
  else return data;
}
