# Olist Raw Dataset

This directory contains the unmodified CSV files from the **Brazilian E-Commerce Public Dataset by Olist**. These files are the immutable source snapshot for the project and must not be edited in place.

## Source and attribution

- **Dataset:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- **Provider:** Olist
- **Coverage:** Approximately 100,000 anonymized Brazilian marketplace orders from 2016 to 2018
- **License:** [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/)
- **Acquisition date:** Not recorded; add it here if the original download date is known
- **Local validation date:** 2026-07-28

The Kaggle archive version was not retained with the extracted files. The SHA-256 checksums below therefore identify the exact source snapshot used by this repository. The repository's code license does not relicense the dataset; the source dataset remains governed by its own license.

## File inventory

Logical row counts were calculated with an RFC-compliant CSV parser and exclude the header row. They must not be derived with `wc -l`, because quoted review text can contain embedded newlines.

| File | Logical rows | Columns | Size in bytes | Expected grain or key |
| --- | ---: | ---: | ---: | --- |
| `olist_customers_dataset.csv` | 99,441 | 5 | 9,033,957 | One row per `customer_id` |
| `olist_geolocation_dataset.csv` | 1,000,163 | 5 | 61,273,883 | Geolocation observation; ZIP prefix is not unique |
| `olist_order_items_dataset.csv` | 112,650 | 7 | 15,438,671 | One row per `order_id + order_item_id` |
| `olist_order_payments_dataset.csv` | 103,886 | 5 | 5,777,138 | One row per `order_id + payment_sequential` |
| `olist_order_reviews_dataset.csv` | 99,224 | 7 | 14,451,670 | One row per `review_id + order_id` |
| `olist_orders_dataset.csv` | 99,441 | 8 | 17,654,914 | One row per `order_id` |
| `olist_products_dataset.csv` | 32,951 | 9 | 2,379,446 | One row per `product_id` |
| `olist_sellers_dataset.csv` | 3,095 | 4 | 174,703 | One row per `seller_id` |
| `product_category_name_translation.csv` | 71 | 2 | 2,613 | One row per Portuguese category name |
| **Total** | **1,550,922** |  | **126,186,995** |  |

## SHA-256 checksums

```text
983a422239e1712ded753b3bf9ecf47dc73f144d306029dcfa99e70a226883d2  olist_customers_dataset.csv
b514f6fc991b9566aeba02aa5d67e2c3630f034b60a0e05aa0d082a3b66d88d6  olist_geolocation_dataset.csv
0bc4d068c4fe38cbb01bd90e8746e3c613fe7b4baef75fab7b0e329701c3e279  olist_order_items_dataset.csv
4f713964f2815dbbaa40b9488268c55aac3627bfce5aa96cf58d1f3616de3cc0  olist_order_payments_dataset.csv
012b61c7593e34f51fa614efdf802b9c7056ce6aae5307ddb93236e7cfc797d7  olist_order_reviews_dataset.csv
8df58ef3d2d7e9944010f7beecd9b75367f5588ec6e3c91cec19ae3345ef9ecf  olist_orders_dataset.csv
3e6569628a17fbc75fd206ee357b59e20364b9afa90f5b6cd5b4d624c58aa9cc  olist_products_dataset.csv
1f643d2b950373b85735e7794b20986f528d7a000432e7c6f9bcbb44d0846a0e  olist_sellers_dataset.csv
a81f0d1f27b27e7293f761bc79e3ce8f348ee39c4b3ed3e49bde38f478586278  product_category_name_translation.csv
```

Run the implemented verification command from the repository root:

```bash
make verify-raw
```

The command validates expected filenames, byte sizes, SHA-256 checksums and CSV headers against `config/raw_dataset_manifest.json`. For a manual checksum-only check, compare the output of `sha256sum data/raw/*.csv` with the values above.

## Initial integrity observations

The following observations were measured from this local snapshot and are validated by `make profile-source`. The generated report is available at [docs/data_contracts/00_source_profile.md](../../docs/data_contracts/00_source_profile.md).

The versioned database mapping, types and seed rules are defined in [docs/data_contracts/02_source_contracts.md](../../docs/data_contracts/02_source_contracts.md) and `config/source_contract.json`.

- Primary keys for customers, orders, products and sellers are non-null and unique.
- Composite keys for order items and payments are non-null and unique.
- The checked order, customer, product, seller, payment and review foreign-key relationships have no orphan records in the clean source snapshot.
- `customer_unique_id` is a cross-order customer business key and is intentionally not unique in the customer source table.
- `review_id` alone is not unique: 99,224 rows contain 98,410 distinct review IDs. The observed unique grain is `review_id + order_id`.
- Geolocation contains 1,000,163 observations for 19,015 ZIP prefixes, with as many as 1,146 rows for one prefix. It must be aggregated before joining to analytical models.
- Two non-null product categories have no English translation: `pc_gamer` and `portateis_cozinha_e_preparadores_de_alimentos`. Downstream logic should retain the Portuguese value and emit a warning rather than reject the product.

## Parsing notes

- Read all files as UTF-8. Use `utf-8-sig` or equivalent for `product_category_name_translation.csv`, which contains a UTF-8 BOM and CRLF line endings.
- Use a standards-compliant CSV parser with quoted-field and embedded-newline support, especially for `olist_order_reviews_dataset.csv`.
- Preserve source column names in the raw layer. The product source intentionally contains the misspelled columns `product_name_lenght` and `product_description_lenght`; rename them only in Silver or dbt staging models.
- Preserve empty values, timestamp text and numeric text exactly in the raw layer. Type casting and timezone normalization belong downstream.

## Raw-data handling rules

1. Do not manually edit, normalize, deduplicate or overwrite these CSV files.
2. Do not commit the CSV files to Git. Only this README and an optional `.gitkeep` are allowed under `data/raw/`.
3. Treat a checksum change as a new source snapshot. Re-profile it, document the reason and update counts and checksums in the same change.
4. Copy or envelope source data into immutable Bronze storage before applying business transformations.
5. Keep generated fixtures and simulator batches outside this directory.
