import { z, ZodRawShape } from "../deps.ts";
import { listTableWithIndexPrefixes, read } from "./crud.ts";
import { PentagonKeyError } from "./errors.ts";
import { filterEntries } from "./search.ts";
import {
  AccessKey,
  KeyProperty,
  PentagonKey,
  QueryArgs,
  TableDefinition,
} from "./types.ts";
import { isKeyOf } from "./util.ts";

export const KeyPropertySchema = z.enum(["primary", "unique", "index"]);

export function parseKeyProperties(
  tableName: string,
  property: string,
  keyPropertyString: string,
): KeyProperty | undefined {
  const parsedProperties = keyPropertyString
    .split(",")
    .map((key) => key.trim())
    .map((key) => {
      try {
        return KeyPropertySchema.parse(key);
      } catch {
        throw new PentagonKeyError(
          `Error parsing property string '${keyPropertyString}'. Your schema has invalid properties. Properties ${
            KeyPropertySchema.options.join(
              ", ",
            )
          } are supported, you passed in '${key}'`,
        );
      }
    });

  if (parsedProperties.length > 1) {
    throw new Error(
      `Table '${tableName}' can't have more than one type of index for property '${property}'. You are using indexes ${
        parsedProperties.map((p) => `'${p}'`).join(" and ")
      }. Use only one of the index values 'primary', 'unique' or 'index'.`,
    );
  }

  return parsedProperties[0];
}

export function getKeysFromTableDefinition<
  PentagonRawShape extends ZodRawShape,
  T extends TableDefinition<PentagonRawShape>,
>(tableDefinition: T) {
  // this works on primary keys, I need examples what to handle here

  const values = tableDefinition.schema.shape;
  const schemaKeys = tableDefinition.schema.keyof().options as Array<
    keyof PentagonRawShape
  >;
  for (const k of schemaKeys) {
    if (values[k]._def.description === "primary") {
      return k;
    }
  }
}

export function schemaToKeys<
  PentagonRawShape extends ZodRawShape,
  T extends TableDefinition<PentagonRawShape>,
>(
  tableName: string,
  schema: T["schema"],
  values: QueryArgs<PentagonRawShape, T>["where"],
): PentagonKey[] {
  const accessKeys = schemaToAccessKeys(tableName, schema, values);
  const denoKeysArr = keysToIndexes(tableName, accessKeys);
  const pentagonKeys: PentagonKey[] = [];

  for (let i = 0; i < accessKeys.length; i++) {
    denoKeysArr[i].forEach((denoKey) => {
      pentagonKeys.push({
        accessKey: accessKeys[i],
        denoKey: denoKey,
      });
    });
  }

  return pentagonKeys;
}

export function schemaToAccessKeys<
  PentagonRawShape extends ZodRawShape,
  T extends TableDefinition<PentagonRawShape>,
>(
  tableName: string,
  schema: T["schema"],
  values: QueryArgs<PentagonRawShape, T>["where"],
): AccessKey[] {
  const accessKeys = Object.entries(schema.shape).reduce(
    (current, [key, value]) => {
      const inputValue = values[key];

      if (!value.description || inputValue === undefined) {
        return current;
      }

      const keyType = parseKeyProperties(tableName, key, value.description);

      switch (keyType) {
        case "primary":
          current.push({
            value: inputValue,
            type: "primary",
          });
          break;
        case "unique":
          current.push({
            value: inputValue,
            type: "unique",
            suffix: `_by_unique_${key}`,
          });
          break;
        case "index":
          current.push({
            value: inputValue,
            type: "index",
            suffix: `_by_${key}`,
          });
          break;
      }

      return current;
    },
    [] as AccessKey[],
  );

  const primaryKeys = accessKeys.filter(({ type }) => type === "primary");

  if (primaryKeys.length > 1) {
    throw new Error(
      `Table '${tableName}' can't have more than one primary key`,
    );
  }

  return accessKeys;
}

/**
 * Transforms `AccessKey` to `Deno.KvKey[]` used to filter items
 * @param tableName Name of the "table" (eg. "users")
 * @param accessKeys The `AccessKey[]` returned by `schemaToKeys()`
 */
function keysToIndexes(
  tableName: string,
  accessKeys: AccessKey[],
): Deno.KvKey[][] {
  const primaryKey = accessKeys.find(({ type }) => type === "primary");

  return accessKeys.map((accessKey) => {
    const accessKeyValueArr = (accessKey.value instanceof Array)
      ? accessKey.value
      : [accessKey.value];

    // Primary key
    if (accessKey.type === "primary") {
      return accessKeyValueArr.map(
        (accessKeyValue) => [tableName, accessKeyValue],
      );
    }

    // Unique indexed key
    if (accessKey.type === "unique") {
      return accessKeyValueArr.map(
        (accessKeyValue) => [`${tableName}${accessKey.suffix}`, accessKeyValue],
      );
    }

    // Non-unique indexed key
    if (accessKey.type === "index") {
      if (!primaryKey) {
        throw new Error(
          `Table '${tableName}' can't use a non-unique index without a primary index`,
        );
      }
      const primaryKeyValueArr = (primaryKey.value instanceof Array)
        ? primaryKey.value
        : [primaryKey.value];
      return accessKeyValueArr.map((accessKeyValue) =>
        primaryKeyValueArr.map(
          (primaryKeyValue) => [
            `${tableName}${accessKey.suffix}`,
            accessKeyValue,
            primaryKeyValue,
          ],
        )
      ).flat();
    }

    throw new Error("Invalid access key");
  });
}

export async function keysToItems<
  PentagonRawShape extends ZodRawShape,
  T extends TableDefinition<PentagonRawShape>,
>(
  kv: Deno.Kv,
  tableName: string,
  keys: PentagonKey[],
  where: QueryArgs<ZodRawShape, T>["where"],
  indexPrefixes: Deno.KvKey,
) {
  const entries = keys.length > 0
    ? await read<PentagonRawShape, T>(kv, keys)
    : await listTableWithIndexPrefixes(kv, ...indexPrefixes);

  // Sort using `where`
  return filterEntries(entries, where);
}

export function selectFromEntries<
  PentagonRawShape extends ZodRawShape,
  T extends TableDefinition<PentagonRawShape>,
  Q extends QueryArgs<PentagonRawShape, T>,
  S extends NonNullable<Q["select"]>,
>(
  items: Deno.KvEntry<z.output<T["schema"]>>[],
  select: S,
): Deno.KvEntry<Pick<z.output<T["schema"]>, keyof S & string>>[] {
  return items.map((item) => {
    item.value = Object.keys(select).reduce(
      (previous, current) =>
        !isKeyOf(current, item.value) ? previous : {
          ...previous,
          [current]: item.value[current],
        },
      {} as Partial<z.output<T["schema"]>>,
    );

    return item;
  });
}

export function getIndexPrefixes<
  PentagonRawShape extends ZodRawShape,
  T extends TableDefinition<PentagonRawShape>,
>(
  tableName: string,
  schema: T["schema"],
): Deno.KvKey {
  const indexPrefixes = Object.entries(schema.shape).reduce(
    (current, [indexKey, indexValue]) => {
      if (!indexValue.description) {
        return current;
      }

      const keyType = parseKeyProperties(
        tableName,
        indexKey,
        indexValue.description,
      );

      switch (keyType) {
        case "primary":
          current.push(tableName);
          break;
        case "unique":
          current.push(`${tableName}_by_unique_${indexKey}`);
          break;
        case "index":
          current.push(`${tableName}_by_${indexKey}`);
          break;
      }

      return current;
    },
    [] as string[],
  );
  return indexPrefixes;
}
