import { FilterQuery } from "mongodb";
import { modify } from "./modify";
import { assert, cloneDeep } from "mingo/util";
import * as logical_ops from "mingo/operators/query/logical";

function $and(obj: any, expr: any[]) {
  for (const sub_filter of expr) {
    try {
      obj = modify(obj, { $set: sub_filter });
    } catch (err) {
      // console.log(err);
      continue;
    }
  }
  return obj;
}

function $or(obj: any, expr: any) {
  console.info({ obj, expr, info: "not support" });
  // ignore
  return obj;
}

const filter_ops = {
  $and,
  $or,
};

export function filterToObj(filter: FilterQuery<any>) {
  let result = {};
  for (const key in filter) {
    if (!key.startsWith("$")) return filter;

    assert(filter_ops[key] != null, `${key} not in mongo-filter-operators`);
    const expr = filter[key];
    result = filter_ops[key](result, expr);
  }
  return result;
}
