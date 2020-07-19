require("mingo/init/system");
import { Lazy, Iterator } from "mingo/lazy";
import { resolve, setValue, cloneDeep, each, keys } from "mingo/util";
import { Db } from "../db";
import { Collection } from "../collection";
import * as mingo from "mingo";
import * as ops from "mingo/operators/pipeline";
const defaultOptions = { config: { idKey: "_id" } };

const log = console.log
export async function aggregateFn(db: Db, coll: Collection, pipelines: any[]) {
  const data = await coll.find().toArray();
  let iterator = Lazy(data);
  for (const pipeline of pipelines) {
    iterator = await parseAggregate(db, coll, iterator, pipeline);
  }

  return iterator.value();
}

async function parseAggregate(db: Db, coll: Collection, prev_result: Iterator, pipeline: object) {
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
          const val = await aggregateFn(db, db.collection($op.from), $op.pipeline);
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
