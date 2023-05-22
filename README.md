<p align="center">
	<img src="https://github.com/skoshx/pentagon/raw/main/docs/pentagon-banner.png" />
</p>

# pentagon

[![Github Actions][github-actions-src]][github-actions-href]

> Prisma like ORM built on top of Deno KV. Allows you to write your database
> schemas and relations using Zod schemas, and run queries using familiar syntax
> from Prisma.

## Features

- No codegen required, everything is inferred using Zod and TypeScript
- All same functions as Prisma supported (not all yet implemented)
- Support for `include`
- Support for `select`
- ~~Pagination~~ (todo)

[📖 &nbsp;Read more](https://docs.useflytrap.com/features)

## 💻 Example usage

```typescript
import { z } from "...";

export const User = z.object({
  id: z.string().uuid().describe("primary, unique"),
  createdAt: z.date(),
  name: z.string(),
});

export const Order = z.object({
  id: z.string().uuid().describe("primary, unique"),
  createdAt: z.date(),
  name: z.string(),
  userId: z.string().uuid(),
});

const db = createPentagon(kv, {
  users: {
    schema: User,
    relations: {
      myOrders: ["orders", [Order], undefined, "userId"],
    },
  },
  orders: {
    schema: Order,
    relations: {
      user: ["users", User, "userId", "id"],
    },
  },
});

// Now we have unlocked the magic of Pentagon
const user = await db.users.findFirst({
  where: { name: "John Doe" },
  select: { name: true, id: true },
});

// We can also do `include` queries, fully typed!
const userWithOrders = await db.users.findFirst({
  where: { name: "John Doe" },
  include: {
    myOrders: true, // if we want the whole object
    /* myOrders: { 👈 if we want just some parts to be included
			id: true,
			name: true
		} */
  },
});
```

## 💻 Development

Help is always appreciated, especially with getting the types right! Here's how
you can contribute:

- Clone this repository
- Fix types / add feature
- Run the tests using `deno test --unstable`
- Open PR

## Running tests

```bash
$ deno test --unstable
```

## License

Made with ❤️ in Helsinki, Finland.

Published under [MIT License](./LICENSE).

<!-- Links -->

[github-actions-href]: https://github.com/skoshx/pentagon/actions/workflows/ci.yml

<!-- Badges -->

[github-actions-src]: https://github.com/skoshx/pentagon/actions/workflows/ci.yml/badge.svg
