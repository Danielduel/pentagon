/// <reference lib="deno.unstable" />
import { z, ZodRawShape } from "../deps.ts";
import { KeyPropertySchema } from "./keys.ts";
export interface PentagonMethods<
  PentagonRawShape extends ZodRawShape,
  T extends TableDefinition<PentagonRawShape>,
> {
  findFirst: <Args extends QueryArgs<PentagonRawShape, T>>(
    args: Args,
  ) => Promise<QueryResponse<PentagonRawShape, T, Args>>;

  // findFirstOrThrow: (
  //   args: QueryArgs<T>,
  // ) => QueryResponse<T, typeof args>;

  findMany: <Args extends QueryArgs<PentagonRawShape, T>>(
    args: Args,
  ) => Promise<Array<QueryResponse<PentagonRawShape, T, Args>>>;

  // findUnique: (args: QueryArgs<T>) => QueryResponse<T, typeof args>;

  // findUniqueOrThrow: (
  //   args: QueryArgs<T>,
  // ) => QueryResponse<T, typeof args>;

  create: <Args extends CreateArgs<PentagonRawShape, T>>(
    args: Args,
  ) => Promise<CreateAndUpdateResponse<PentagonRawShape, T>>;

  createMany: <Args extends CreateManyArgs<PentagonRawShape, T>>(
    args: Args,
  ) => Promise<CreateAndUpdateResponse<PentagonRawShape, T>[]>;

  upsertMany: <Args extends CreateManyArgs<PentagonRawShape, T>>(
    args: Args,
  ) => Promise<CreateAndUpdateResponse<PentagonRawShape, T>[]>;

  update: <Args extends UpdateArgs<PentagonRawShape, T>>(
    args: Args,
  ) => Promise<CreateAndUpdateResponse<PentagonRawShape, T>>;

  updateMany: <Args extends UpdateArgs<PentagonRawShape, T>>(
    args: Args,
  ) => Promise<Array<CreateAndUpdateResponse<PentagonRawShape, T>>>;

  // upsert: (args: CreateAndUpdateArgs<T>) => CreateAndUpdateResponse<T>;

  // count: (args: QueryArgs<T>) => number;

  // @ts-ignore TODO: delete should not use QueryArgs or QueryResponse
  delete: <Args extends QueryArgs<T>>(
    args: Args,
  ) => Promise<QueryResponse<PentagonRawShape, T, Args>>;

  // @ts-ignore TODO: deleteMany should not use QueryArgs or QueryResponse
  deleteMany: <Args extends QueryArgs<T>>(
    args: Args,
  ) => Promise<QueryResponse<PentagonRawShape, T, Args>>;

  // aggregate: (args: QueryArgs<T>) => QueryResponse<T, typeof args>;
}

export type PentagonResult<
  PentagonRawShape extends ZodRawShape,
  T extends Record<string, TableDefinition<PentagonRawShape>>
> = {
  [K in keyof T]: PentagonMethods<PentagonRawShape, T[K]>;
};
/*  & {
  // Built-in functions
  close: () => Promise<void>;
  getKv: () => Deno.Kv;
}; */

// @todo rename to something like WithVersionstamp
export type WithVersionstamp<T> = T & {
  versionstamp: string;
};
export type WithMaybeVersionstamp<T> = T & {
  versionstamp?: string | null | undefined;
};

export type LocalKey = string;
export type ForeignKey = string;

/**
 * [relation name, schema, local key, foreign key]
 */
export type RelationDefinition = [
  relationSchemaName: string,
  /**
   * If you provide this as an array, the relation is treated as a
   * to-many relation, if it's not an array, then its treated as a
   * to-one relation.
   */
  relationSchema: [ReturnType<typeof z.object>] | ReturnType<typeof z.object>,
  /**
   * LocalKey is a string if this schema is the one defining the relation,
   * undefined if this schema is the target of the relation.
   */
  localKey: LocalKey,
  foreignKey: ForeignKey,
];

export type TableDefinition<PentagonRawShape extends ZodRawShape> = {
  schema: ReturnType<typeof z.object<PentagonRawShape>>;
  relations?: Record<string, RelationDefinition>;
};

export type QueryResponse<
  PentagonRawShape extends ZodRawShape,
  T extends TableDefinition<PentagonRawShape>,
  PassedInArgs extends QueryArgs<PentagonRawShape, T>,
> = WithVersionstamp<
  & Select<PentagonRawShape, T, PassedInArgs["select"]>
  & Include<PentagonRawShape, T["relations"], PassedInArgs["include"]>
>;

type Nothing = {};

type Select<
  PentagonRawShape extends ZodRawShape,
  T extends TableDefinition<PentagonRawShape>,
  Selected extends QueryArgs<PentagonRawShape, T>["select"] | undefined,
> = Selected extends Partial<Record<string, unknown>>
  ? Pick<z.output<T["schema"]>, keyof Selected & string>
  : z.output<T["schema"]>;
type Include<
  PentagonRawShape extends ZodRawShape,
  Relations extends TableDefinition<PentagonRawShape>["relations"],
  ToBeIncluded extends IncludeDetails<PentagonRawShape, Relations> | undefined,
> = Relations extends Record<string, RelationDefinition>
  ? ToBeIncluded extends Record<string, unknown> ? {
      [Rel in keyof Relations]: Relations[Rel][1] extends
        [{ _output: infer OneToManyRelatedSchema }]
        ? ToBeIncluded extends
          Record<Rel, infer DetailsToInclude extends Record<string, unknown>>
          ? MatchAndSelect<OneToManyRelatedSchema, DetailsToInclude>[]
        : ToBeIncluded extends Record<Rel, true> ? OneToManyRelatedSchema[]
        : Nothing
        : Relations[Rel][1] extends { _output: infer OneToOneRelatedSchema }
          ? ToBeIncluded extends
            Record<Rel, infer DetailsToInclude extends Record<string, unknown>>
            ? ToBeIncluded extends Record<Rel, true>
              ? MatchAndSelect<OneToOneRelatedSchema, DetailsToInclude>
            : Nothing
          : OneToOneRelatedSchema
        : Nothing;
    }
  : Nothing
  : Nothing;

type MatchAndSelect<SourceSchema, ToBeIncluded> = {
  [Key in Extract<keyof SourceSchema, keyof ToBeIncluded>]:
    ToBeIncluded[Key] extends infer ToInclude
      ? SourceSchema[Key] extends infer Source ? ToInclude extends true ? Source
        : MatchAndSelect<Source, ToInclude>
      : never
      : never;
};

export type DeleteResponse = { versionstamp: string };
export type CreateAndUpdateResponse<
  PentagonRawShape extends ZodRawShape,
  T extends TableDefinition<PentagonRawShape>,
> = WithVersionstamp<
  z.output<
    T["schema"]
  >
>;

export type CreateArgs<
  PentagonRawShape extends ZodRawShape,
  T extends TableDefinition<PentagonRawShape>,
> =
  & Pick<QueryArgs<PentagonRawShape, T>, "select">
  & {
    data: z.input<T["schema"]>;
  };
export type CreateManyArgs<
  PentagonRawShape extends ZodRawShape,
  T extends TableDefinition<PentagonRawShape>,
> =
  & Pick<QueryArgs<PentagonRawShape, T>, "select">
  & {
    data: z.input<T["schema"]>[];
  };

export type UpsertManyArgs<
  PentagonRawShape extends ZodRawShape,
  T extends TableDefinition<PentagonRawShape>,
> =
  & Pick<QueryArgs<PentagonRawShape, T>, "where">
  & {
    data: z.input<T["schema"]>[];
  };

export type UpdateArgs<
  PentagonRawShape extends ZodRawShape,
  T extends TableDefinition<PentagonRawShape>,
> = QueryArgs<PentagonRawShape, T> & {
  data: Partial<WithMaybeVersionstamp<z.input<T["schema"]>>>;
};

export type QueryKvOptions = Parameters<Deno.Kv["get"]>[1];

type IncludeDetails<
  PentagonRawShape extends ZodRawShape,
  Relations extends TableDefinition<PentagonRawShape>["relations"],
> = Relations extends Record<string, RelationDefinition> ? {
    [Rel in keyof Relations]?:
      | true
      | (Relations[Rel][1] extends [{ _output: infer OneToManyRelatedSchema }]
        ? Includable<OneToManyRelatedSchema>
        : Relations[Rel][1] extends { _output: infer OneToOneRelatedSchema }
          ? Includable<OneToOneRelatedSchema>
        : never);
  }
  : never;

type Includable<T> = T extends Record<string, unknown>
  ? { [K in keyof T]?: true | Includable<T[K]> }
  : never;

export type QueryArgs<PentagonRawShape extends ZodRawShape, T extends TableDefinition<PentagonRawShape>> = {
  where?: Partial<WithMaybeVersionstamp<z.output<T["schema"]>>>;
  take?: number;
  skip?: number;
  select?: Partial<Record<keyof z.output<T["schema"]>, true>>;
  orderBy?: Partial<z.output<T["schema"]>>;
  include?: IncludeDetails<PentagonRawShape, T["relations"]>;
  distinct?: Array<keyof z.output<T["schema"]>>;
  kvOptions?: QueryKvOptions;
};

export type AccessKey =
  & (
    | { value: Deno.KvKeyPart }
    | { value: Deno.KvKeyPart[] }
  )
  & (
    | { type: "primary" }
    | { type: "index"; suffix: string }
    | { type: "unique"; suffix: string }
  );

export type PentagonKey = {
  accessKey: AccessKey;
  denoKey: Deno.KvKey;
};

export type KeyProperty = z.infer<typeof KeyPropertySchema>;

export type DatabaseValue<T = unknown> =
  | undefined
  | null
  | boolean
  | number
  | string
  | bigint
  | Uint8Array
  | Array<T>
  | Record<string | number | symbol, T>
  | Map<unknown, unknown>
  | Set<T>
  | Date
  | RegExp;
