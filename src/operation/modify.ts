require("mingo/init/system");
import {
  isNumber,
  isString,
  isOperator,
  isObject,
  each,
  setValue,
  resolve,
  cloneDeep,
  assert,
  keys,
  removeValue,
  unique,
} from "mingo/util";
import { Options } from "mingo/core";
import * as mingo_array_ops from "mingo/operators/expression/array";
import * as mingo_compare_ops from "mingo/operators/query/comparison";
import { $set, $sort } from "mingo/operators/pipeline";
import { Lazy, Iterator } from "mingo/lazy";

// $------ custom operator ------$
const pull_ops = Object.assign({}, mingo_compare_ops, mingo_array_ops);
/** aggregate operator */
function $pull(obj: any, expr: any, options: Options) {
  if (isNumber(expr) || isString(expr)) {
    assert(Array.isArray(obj), "$pull only allow for array");
    return obj.filter((item) => item !== expr);
  } else if (!isObject(expr)) return obj;

  let newFields = keys(expr);
  if (newFields.length === 0) return obj;

  if (isOperator(newFields[0])) {
    assert(Array.isArray(obj), "$pull only allow for array");
    const key = newFields[0];
    assert(pull_ops[key], "unknown array operator: " + key);
    return obj.filter((item) => !pull_ops[key](obj, expr[key], options));
  } else {
    let newObj = Object.assign({}, obj);
    each(newFields, (field) => {
      /** @type {string} */
      const oldValue = resolve(obj, field);
      setValue(newObj, field, $pull(oldValue, expr[field], options));
    });
    return newObj;
  }
}

/** aggregate operator */
function $unset(obj: object, expr: any, options: Options) {
  let newFields = keys(expr);
  if (newFields.length === 0) return obj;

  each(newFields, (field) => removeValue(obj, field));
  return obj;
}

function _$push(arr: any[], expr: any, options: Options) {
  if (isObject(expr)) {
    if (expr.$each) {
      // case 1. has modifier
      assert(Array.isArray(expr.$each), "$each only allow for array");

      // 1. $position
      let start_idx = expr.$position || 0;
      assert(isNumber(start_idx), "$position only allow for Number");
      // 2. $each
      arr.splice(start_idx, 0, ...expr.$each);
      // 3. $sort
      if (expr.$sort) arr = $sort(Lazy(arr), expr.$sort, options).value();
      // 4. $slice
      if (expr.$slice) {
        assert(isNumber(expr.$slice), "$each only allow for array");
        arr = arr.slice(expr.$slice);
      }

      return arr;
    }
  }

  // case 2. no modifier
  arr.push(expr);
  return arr;
}

/** aggregate operator */
function $push(obj: any, expr: any, options: Options) {
  const newObj = cloneDeep(obj);
  each(expr, (val, field) => {
    const oldValue = resolve(obj, field) || [];
    assert(Array.isArray(oldValue), "$push only allow for array");

    const newValue = _$push(oldValue, val, options);
    setValue(newObj, field, newValue);
  });
  return newObj;
}

/*
function $currentDate(collection, expr, options) {
  let newFields = keys(expr);
  if (newFields.length === 0) return collection
  return collection.map((obj) => {
    let newObj = cloneDeep(obj);
    each(newFields, (field) => {
      setValue(newObj, field, new Date());
    });
    return newObj;
  });
}
*/

/** accumulator operator */
function $inc(collection: Array<any>, expr: object, options: Options) {
  let newFields = keys(expr);
  if (newFields.length === 0) return collection;

  // if expr[field] not number then throw error
  each(newFields, (field) => {
    assert(typeof expr[field] === "number", "Modifier $inc allowed for numbers only");
  });

  return collection.map((obj) => {
    let newObj = cloneDeep(obj);
    each(newFields, (field) => {
      // TODO: check if typeof obj[field]  == "number" or "undefined"

      const value = resolve(obj, field) || 0;
      setValue(newObj, field, expr[field] + value);
    });
    return newObj;
  });
}

/** aggregate operator */
function $addToSet(obj: any, expr: object, options: Options) {
  const newObj = cloneDeep(obj);
  each(expr, (val, field) => {
    // TODO: to support $each
    const oldValue = resolve(obj, field) || [];
    assert(Array.isArray(oldValue), "$addToSet only allow for array");

    oldValue.push(val);
    const newValue = unique(oldValue);
    setValue(newObj, field, newValue);
  });
  return newObj;
}

// ------ adapter ------
function pl_adapter(operator: Function) {
  return (collection: Iterator, expr: object, options: Options) => {
    if (Array.isArray(collection)) return operator(Lazy(collection), expr, options).value();
    else return operator(Lazy([collection]), expr, options).value()[0];
  };
}

function agg_adapter(operator: Function) {
  return (collection: object | any[], expr: object, options: Options) => {
    if (Array.isArray(collection)) return collection.map((obj) => operator(obj, expr, options));
    else return operator(collection, expr, options);
  };
}

function acc_adapter(operator: Function) {
  return (collection: object | any[], expr: object, options: Options) => {
    if (Array.isArray(collection)) return operator(collection, expr, options);
    else return operator([collection], expr, options)[0];
  };
}

// $------ real implementation ------$
const update_ops = {
  // $currentDate: $currentDate,
  $setOnInsert: (doc) => doc,
  $pull: agg_adapter($pull),
  $push: agg_adapter($push),
  $addToSet: agg_adapter($addToSet),
  $set: pl_adapter($set),
  $unset: agg_adapter($unset),
  $inc: acc_adapter($inc),
};
const defaultOptions = { config: { idKey: "_id" } };

export function modify<T>(docs: T, updates: object): T;
export function modify<T>(docs: T[], updates: object): T[];
export function modify<T>(docs: T[] | T, updates: object) {
  if (!docs) return;
  if (Array.isArray(docs) && docs.length === 0) return docs;

  let result = docs;
  for (const key in updates) {
    if (!key.startsWith("$")) return modify(docs, { $set: updates });

    assert(update_ops[key] != null, `${key} not in mongo-update-operators`);
    const expr = updates[key];
    result = update_ops[key](result, expr, defaultOptions);
  }

  return result;
}
