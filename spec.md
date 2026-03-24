# gmeta spec

This file is the entry point to the gmeta specification.

The canonical spec now lives in the `spec/` directory so there is a single source of truth and each concern can evolve independently.

## Canonical spec

Start here:

- [spec/README.md](./spec/README.md)

## Project-level spec docs

- [spec/exchange-format/targets.md](./spec/exchange-format/targets.md)
- [spec/exchange-format/exchange.md](./spec/exchange-format/exchange.md)
- [spec/exchange-format/materialization.md](./spec/exchange-format/materialization.md)
- [spec/exchange-format/output.md](./spec/exchange-format/output.md)
- [spec/implementation/storage.md](./spec/implementation/storage.md)
- [spec/implementation/cli.md](./spec/implementation/cli.md)

## Value-type spec docs

- [spec/exchange-format/strings.md](./spec/exchange-format/strings.md)
- [spec/exchange-format/lists.md](./spec/exchange-format/lists.md)
- [spec/exchange-format/sets.md](./spec/exchange-format/sets.md)

Ordered lists are intentionally not specified yet.

## Scope of this file

This file intentionally does **not** duplicate the detailed spec text from `spec/`.

That avoids drift between:

- a monolithic spec document
- the modular per-topic and per-type docs

If behavior is changed, the change should be made in the relevant file under `spec/`.
