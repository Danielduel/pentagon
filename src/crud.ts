// CRUD operations
import { z } from "../deps.ts";
import { PentagonCreateItemError, PentagonDeleteItemError } from "./errors.ts";
import {
  keysToIndexes,
  schemaToKeys,
  selectFromEntries,
  whereToKeys,
} from "./keys.ts";
import { isToManyRelation } from "./relation.ts";
import {
  DatabaseValue,
  QueryArgs,
  QueryKvOptions,
  TableDefinition,
  WithMaybeVersionstamp,
  WithVersionstamp,
} from "./types.ts";
import { mergeValueAndVersionstamp } from "./util.ts";

function chainAccessKeyCheck(
  op: Deno.AtomicOperation,
  key: Deno.KvKey,
  versionstamp: string | null = null,
) {
  return op.check({ key: key, versionstamp });
}

function chainSet<T>(op: Deno.AtomicOperation, key: Deno.KvKey, item: T) {
  return op.set(key, item);
}

function chainDelete<T>(op: Deno.AtomicOperation, key: Deno.KvKey) {
  return op.delete(key);
}

export async function listTable<T>(kv: Deno.Kv, tableName: string) {
  const items = new Array<Deno.KvEntry<T>>();
  for await (const item of kv.list<T>({ prefix: [tableName] })) {
    items.push(item);
  }
  return items;
}

export async function read<T extends readonly unknown[]>(
  kv: Deno.Kv,
  keys: readonly [...{ [K in keyof T]: Deno.KvKey }],
  kvOptions?: QueryKvOptions,
) {
  const res = await kv.getMany<T>(keys, kvOptions);
  return res;
}

export async function remove(
  kv: Deno.Kv,
  keys: Deno.KvKey[],
): Promise<WithVersionstamp<Record<string, DatabaseValue>>> {
  let res = kv.atomic();

  // @todo: do we need these checks here for delete ops?
  /* for (let i = 0; i < keys.length; i++) {
    res = chainAccessKeyCheck(res, keys[i], null);
  } */

  for (let i = 0; i < keys.length; i++) {
    res = chainDelete(res, keys[i]);
  }

  const commitResult = await res.commit();

  if (commitResult.ok) {
    return {
      versionstamp: commitResult.versionstamp,
    };
  }
  throw new PentagonDeleteItemError(`Could not delete item.`);
}

export async function update<T extends Record<string, DatabaseValue>>(
  kv: Deno.Kv,
  items: T[],
  keys: Deno.KvKey[],
): Promise<WithVersionstamp<T>[]> {
  let res = kv.atomic();

  // Iterate through items
  for (let i = 0; i < items.length; i++) {
    // Checks (through keys)
    for (let j = 0; j < keys.length; j++) {
      res = chainAccessKeyCheck(
        res,
        keys[j],
        (items[j].versionstamp as string) ?? null,
      );
    }
    // Sets (through keys)
    for (let j = 0; j < keys.length; j++) {
      res = chainSet(res, keys[j], items[i]);
    }
  }

  const commitResult = await res.commit();

  if (commitResult.ok) {
    return items.map((i) => ({
      ...i,
      versionstamp: commitResult.versionstamp,
    }));
  }
  throw new PentagonCreateItemError(`Could not update item.`);
}

export async function create<T extends Record<string, DatabaseValue>>(
  kv: Deno.Kv,
  item: T,
  keys: Deno.KvKey[],
): Promise<WithVersionstamp<T>> {
  let res = kv.atomic();

  // Checks
  for (let i = 0; i < keys.length; i++) {
    res = chainAccessKeyCheck(res, keys[i]);
  }

  // Sets
  for (let i = 0; i < keys.length; i++) {
    res = chainSet(res, keys[i], item);
  }

  const commitResult = await res.commit();

  if (commitResult.ok) {
    return {
      ...item,
      versionstamp: commitResult.versionstamp,
    };
  }
  throw new PentagonCreateItemError(`Could not create item.`);
}

export async function findMany<T extends TableDefinition>(
  kv: Deno.Kv,
  tableName: string,
  tableDefinition: T,
  queryArgs: QueryArgs<T>,
) {
  const keys = schemaToKeys(tableDefinition.schema, queryArgs.where ?? []);
  const indexKeys = keysToIndexes(tableName, keys);
  const foundItems = await whereToKeys(
    kv,
    tableName,
    indexKeys,
    queryArgs.where ?? {},
  );

  if (queryArgs.include) {
    for (
      const [relationName, relationValue] of Object.entries(queryArgs.include)
    ) {
      // Relation name
      const relationDefinition = tableDefinition.relations?.[relationName];
      if (!relationDefinition) {
        throw new Error(
          `No relation found for relation name "${relationName}", make sure it's ∂efined in your Pentagon configuration.`,
        );
      }
      const tableName = relationDefinition[0];
      const localKey = relationDefinition[2];
      const foreignKey = relationDefinition[3];

      for (let i = 0; i < foundItems.length; i++) {
        const foundRelationItems = await findMany(
          kv,
          tableName,
          tableDefinition,
          {
            select: relationValue === true ? undefined : relationValue,
            where: {
              [foreignKey]: foundItems[i].value[localKey],
            } as Partial<WithMaybeVersionstamp<z.infer<T["schema"]>>>,
          },
        );

        // Add included relation value
        foundItems[i].value[relationName] = isToManyRelation(relationDefinition)
          ? foundRelationItems
          : foundRelationItems?.[0];
      }
    }
  }

  // Select
  const selectedItems = queryArgs.select
    ? selectFromEntries(foundItems, queryArgs.select)
    : foundItems;

  return selectedItems.map((item) => mergeValueAndVersionstamp(item));
}
