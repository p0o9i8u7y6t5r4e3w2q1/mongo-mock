// @ts-check
/** @typedef {import('mingo/lazy').Iterator} Iterator */
/** @typedef {import('mingo/core').Options} Options */
require("mingo/init/system");
const modifyjs = require("modifyjs");
const mingo = require("mingo");
const { Lazy } = require("mingo/lazy");
const {
  isOperator,
  isNumber,
  isString,
  removeValue,
  isObject,
  cloneDeep,
  keys,
  resolve,
  assert,
  setValue,
  each,
} = require("mingo/util");
const array_ops = require("mingo/operators/expression/array");

/**
 * aggregate operator
 * @param {Object} obj
 * @param {Object} expr
 * @param {Options} options
 */
function $unset(obj, expr, options) {
  let newFields = keys(expr);
  if (newFields.length === 0) return obj;

  each(newFields, (field) => removeValue(obj, field));
  return obj;
}

/**
 * aggregate operator
 * @param {Object} obj
 * @param {any} expr
 * @param {Options} options
 */
function $pull(obj, expr, options) {
  if (isNumber(expr) || isString(expr)) {
    assert(Array.isArray(obj), "$pull only allow for array");
    return obj.filter((item) => item !== expr);
  } else if (!isObject(expr)) return obj;

  let newFields = keys(expr);
  if (newFields.length === 0) return obj;
  if (isOperator(newFields[0])) {
    assert(Array.isArray(obj), "$pull only allow for array");
    const key = newFields[0];
    // TODO: allow $gte, ... compare operators
    assert(array_ops[key], "unknown array operator: " + key);
    return obj.filter((item) => !array_ops[key](obj, expr[key], options));
  } else {
    each(newFields, (field) => {
      /** @type {string} */
      const oldValue = resolve(obj, field);
      setValue(obj, field, $pull(oldValue, expr[field], options));
    });
  }

  return obj;
}

function pl_adapter(operator) {
  /**
   * @param {Iterator} collection
   * @param {Object} expr
   * @param {Options} options
   */
  return (collection, expr, options) => {
    if (Array.isArray(collection))
      return operator(Lazy(collection), expr, options).value();
    else return operator(Lazy([collection]), expr, options).value()[0];
  };
}

function agg_adapter(operator) {
  /**
   * @param {Object} collection
   * @param {Object} expr
   * @param {Options} options
   */
  return (collection, expr, options) => {
    if (Array.isArray(collection))
      return collection.map((obj) => operator(obj, expr, options));
    else return operator(collection, expr, options);
  };
}

const mongoUpdateOps = {
  $pull: agg_adapter($pull),
};

const log = console.log;
const defaultOptions = { config: { idKey: "_id" } };
module.exports = function mingo_modify(original, updates, options) {
  const key = keys(updates)[0];
  // assert(mongoUpdateOps[key] != null, `${key} not in mongo-update-operators`);
  if (mongoUpdateOps[key] == null) return modifyjs(original, updates, options);
  const expr = updates[key];

  if (Array.isArray(original))
    return mongoUpdateOps[key](original, expr, defaultOptions);
  else return mongoUpdateOps[key]([original], expr, defaultOptions)[0];
};
