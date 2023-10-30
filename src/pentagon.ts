import { ZodRawShape } from "../deps.ts";
import { create, createMany, upsertMany, findMany, remove, update } from "./crud.ts";
import { PentagonUpdateError } from "./errors.ts";
import { getIndexPrefixes, keysToItems, schemaToKeys } from "./keys.ts";
import type {
  PentagonMethods,
  PentagonResult,
  TableDefinition,
} from "./types.ts";

export function createPentagon<
  PentagonRawShape extends ZodRawShape,
  T extends Record<string, TableDefinition<PentagonRawShape>>>(
  kv: Deno.Kv,
  schema: T,
) {
  // @todo(skoshx): Run through schemas, validate `description`
  // @todo(skoshx): Run through schemas, validate `relations`
  // @todo(skoshx): Add all properties
  const result = Object.fromEntries(
    Object.entries(schema).map(([tableName, tableDefinition]) => {
      const methods: PentagonMethods<PentagonRawShape, typeof tableDefinition> = {
        create: (createArgs) =>
          createImpl(kv, tableName, tableDefinition, createArgs),
        createMany: (createManyArgs) =>
          createManyImpl(kv, tableName, tableDefinition, createManyArgs),
        upsertMany: (upsertManyArgs) =>
          upsertManyImpl(kv, tableName, tableDefinition, upsertManyArgs),
        delete: (queryArgs) =>
          // @ts-ignore
          deleteImpl(kv, tableName, tableDefinition, queryArgs),
        deleteMany: (queryArgs) =>
          // @ts-ignore
          deleteManyImpl(kv, tableName, tableDefinition, queryArgs),
        update: (queryArgs) =>
          updateImpl(kv, tableName, tableDefinition, queryArgs),
        updateMany: (queryArgs) =>
          updateManyImpl(kv, tableName, tableDefinition, queryArgs),
        findMany: (queryArgs) =>
          // @ts-ignore
          findManyImpl(kv, tableName, tableDefinition, queryArgs),
        findFirst: (queryArgs) =>
          // @ts-ignore
          findFirstImpl(kv, tableName, tableDefinition, queryArgs),
      };

      return [tableName, methods];
    }),
  );
  // @ts-ignore: todo: add this without losing the inferred types
  result.getKv = () => kv;

  return result as PentagonResult<PentagonRawShape, T>;
}

export function getKvInstance<T>(db: T): Deno.Kv {
  // @ts-ignore: same as above
  return db.getKv();
}

async function createImpl<PentagonRawShape extends ZodRawShape, T extends TableDefinition<PentagonRawShape>>(
  kv: Deno.Kv,
  tableName: string,
  tableDefinition: T,
  createArgs: Parameters<PentagonMethods<PentagonRawShape, T>["create"]>[0],
): ReturnType<PentagonMethods<PentagonRawShape, T>["create"]> {
  return await create(
    kv,
    tableName,
    tableDefinition,
    createArgs,
  );
}

async function createManyImpl<PentagonRawShape extends ZodRawShape, T extends TableDefinition<PentagonRawShape>>(
  kv: Deno.Kv,
  tableName: string,
  tableDefinition: T,
  createManyArgs: Parameters<PentagonMethods<PentagonRawShape, T>["createMany"]>[0],
): ReturnType<PentagonMethods<PentagonRawShape, T>["createMany"]> {
  return await createMany(
    kv,
    tableName,
    tableDefinition,
    createManyArgs,
  );
}

async function upsertManyImpl<PentagonRawShape extends ZodRawShape, T extends TableDefinition<PentagonRawShape>>(
  kv: Deno.Kv,
  tableName: string,
  tableDefinition: T,
  createManyArgs: Parameters<PentagonMethods<PentagonRawShape, T>["createMany"]>[0],
): ReturnType<PentagonMethods<PentagonRawShape, T>["createMany"]> {
  return await upsertMany(
    kv,
    tableName,
    tableDefinition,
    createManyArgs,
  );
}

async function deleteImpl<PentagonRawShape extends ZodRawShape, T extends TableDefinition<PentagonRawShape>>(
  kv: Deno.Kv,
  tableName: string,
  tableDefinition: T,
  queryArgs: Parameters<PentagonMethods<PentagonRawShape, T>["delete"]>[0],
) {
  const keys = schemaToKeys(
    tableName,
    tableDefinition.schema,
    queryArgs.where ?? [],
  );
  const items = await keysToItems(
    kv,
    tableName,
    keys,
    queryArgs.where ?? {},
    getIndexPrefixes(tableName, tableDefinition.schema),
  );
  return await remove(kv, items.map((i) => i.key));
}

async function deleteManyImpl<PentagonRawShape extends ZodRawShape, T extends TableDefinition<PentagonRawShape>>(
  kv: Deno.Kv,
  tableName: string,
  tableDefinition: T,
  queryArgs: Parameters<PentagonMethods<PentagonRawShape, T>["deleteMany"]>[0],
) {
  const keys = schemaToKeys(
    tableName,
    tableDefinition.schema,
    queryArgs.where ?? [],
  );
  const items = await keysToItems(
    kv,
    tableName,
    keys,
    queryArgs.where,
    getIndexPrefixes(tableName, tableDefinition.schema),
  );

  return await remove(kv, items.map((i) => i.key));
}

async function updateManyImpl<PentagonRawShape extends ZodRawShape, T extends TableDefinition<PentagonRawShape>>(
  kv: Deno.Kv,
  tableName: string,
  tableDefinition: T,
  updateArgs: Parameters<PentagonMethods<PentagonRawShape, T>["updateMany"]>[0],
): ReturnType<PentagonMethods<PentagonRawShape, T>["updateMany"]> {
  const keys = schemaToKeys(
    tableName,
    tableDefinition.schema,
    updateArgs.where ?? [],
  );
  const items = await keysToItems(
    kv,
    tableName,
    keys,
    updateArgs.where,
    getIndexPrefixes(tableName, tableDefinition.schema),
  );

  if (items.length === 0) {
    // @todo: should we throw?
    throw new PentagonUpdateError(`Updating zero elements.`);
  }

  try {
    const updatedItems = items
      .map((existingItem) => ({
        key: existingItem.key,
        value: tableDefinition.schema.parse({
          ...existingItem.value,
          ...updateArgs.data,
        }),
        versionstamp: updateArgs.data.versionstamp ?? existingItem.versionstamp,
      }));

    return await update(
      kv,
      updatedItems,
    );
  } catch {
    throw new PentagonUpdateError(`An error occurred while updating items`);
  }
}

async function updateImpl<PentagonRawShape extends ZodRawShape, T extends TableDefinition<PentagonRawShape>>(
  kv: Deno.Kv,
  tableName: string,
  tableDefinition: T,
  updateArgs: Parameters<PentagonMethods<PentagonRawShape, T>["update"]>[0],
): ReturnType<PentagonMethods<PentagonRawShape, T>["update"]> {
  return (await updateManyImpl(kv, tableName, tableDefinition, updateArgs))
    ?.[0];
}

async function findManyImpl<PentagonRawShape extends ZodRawShape, T extends TableDefinition<PentagonRawShape>>(
  kv: Deno.Kv,
  tableName: string,
  tableDefinition: T,
  queryArgs: Parameters<PentagonMethods<PentagonRawShape, T>["findMany"]>[0],
) {
  return await findMany(
    kv,
    tableName,
    tableDefinition,
    queryArgs,
  ) as Awaited<ReturnType<PentagonMethods<PentagonRawShape, T>["findMany"]>>;
}

async function findFirstImpl<PentagonRawShape extends ZodRawShape, T extends TableDefinition<PentagonRawShape>>(
  kv: Deno.Kv,
  tableName: string,
  tableDefinition: T,
  queryArgs: Parameters<PentagonMethods<PentagonRawShape, T>["findFirst"]>[0],
) {
  return (await findMany(kv, tableName, tableDefinition, queryArgs))
    ?.[0] as Awaited<ReturnType<PentagonMethods<PentagonRawShape, T>["findFirst"]>>;
}
