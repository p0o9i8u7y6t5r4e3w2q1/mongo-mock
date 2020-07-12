// @ts-check
/** @typedef {import('mingo/lazy').Iterator} Iterator */
require("mingo/init/system");
const mingo = require("mingo");
const { Lazy } = require("mingo/lazy");
const { resolve, setValue, cloneDeep, each, keys } = require("mingo/util");
const ops = require("mingo/operators/pipeline");
const defaultOptions = { config: { idKey: "_id" } };

/**
 * @param {import('mongodb').Db} db
 * @param {import('mongodb').Collection} coll
 * @param {any[]} pipelines
 */
async function aggregateFn(db, coll, pipelines) {
  const data = await coll.find().toArray();
  let iterator = Lazy(data);
  for (const pipeline of pipelines) {
    iterator = await parseAggregate(db, coll, iterator, pipeline);
  }

  return iterator.value();
}

/**
 * @param {import('mongodb').Db} db
 * @param {import('mongodb').Collection} coll
 * @param {Iterator} prev_result
 * @param {object} pipeline
 */
async function parseAggregate(db, coll, prev_result, pipeline) {
  const key = Object.keys(pipeline)[0];
  const $op = pipeline[key];

  switch (key) {
    case "$lookup":
      if ($op.pipeline) {
        const result = [];
        for (const item of prev_result.value()) {
          const newObj = cloneDeep(item);
          each($op.let, (val, key) => {
            newObj["$$" + key] = resolve(item, val);
          });
          const val = await aggregateFn(
            db,
            db.collection($op.from),
            $op.pipeline
          );
          if (val.length > 0) setValue(item, $op.as, val);
          result.push(item);
        }
        return Lazy(result);
      } else {
        const foreign_objects = await db.collection($op.from).find().toArray();
        return prev_result.map((item) => {
          const localFieldValue = resolve(item, $op.localField);
          const criteria = Array.isArray(localFieldValue)
            ? { [$op.foreignField]: { $in: localFieldValue } }
            : { [$op.foreignField]: localFieldValue };
          const val = mingo.find(foreign_objects, criteria).all();
          if (val.length > 0) setValue(item, $op.as, val);
          return item;
        });
      }
    default:
      return ops[key](prev_result, $op, defaultOptions);
  }
}

module.exports = aggregateFn;
