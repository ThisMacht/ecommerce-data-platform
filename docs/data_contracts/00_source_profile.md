# Olist Source Profile

> Được tự động sinh bởi `make profile-source`. Không chỉnh sửa thủ công các giá trị đo lường.

## Snapshot

| Field | Value |
| --- | --- |
| Dataset | Brazilian E-Commerce Public Dataset by Olist |
| Snapshot ID | `olist-kaggle-local-v1` |
| Manifest validation date | 2026-07-28 |
| Profile status | **PASS** |
| Files | 9 |
| Logical rows | 1,550,922 |
| Source bytes | 126,186,995 |
| Null field values | 153,259 |
| Non-enforced warnings | 1 |

Machine-readable report được sinh tại `build/reports/source_profile.json` và đã được loại khỏi Git.

## File and key validation

| File | Rows | Columns | Key | Null key rows | Duplicate key rows | Status |
| --- | ---: | ---: | --- | ---: | ---: | --- |
| `olist_customers_dataset.csv` | 99,441 | 5 | `customer_id` | 0 | 0 | PASS |
| `olist_geolocation_dataset.csv` | 1,000,163 | 5 | Not declared | — | — | PASS |
| `olist_order_items_dataset.csv` | 112,650 | 7 | `order_id` + `order_item_id` | 0 | 0 | PASS |
| `olist_order_payments_dataset.csv` | 103,886 | 5 | `order_id` + `payment_sequential` | 0 | 0 | PASS |
| `olist_order_reviews_dataset.csv` | 99,224 | 7 | `review_id` + `order_id` | 0 | 0 | PASS |
| `olist_orders_dataset.csv` | 99,441 | 8 | `order_id` | 0 | 0 | PASS |
| `olist_products_dataset.csv` | 32,951 | 9 | `product_id` | 0 | 0 | PASS |
| `olist_sellers_dataset.csv` | 3,095 | 4 | `seller_id` | 0 | 0 | PASS |
| `product_category_name_translation.csv` | 71 | 2 | `product_category_name` | 0 | 0 | PASS |

## Null values

Chỉ liệt kê các column có ít nhất một source value null hoặc rỗng.

| File | Column | Null rows | Null rate |
| --- | --- | ---: | ---: |
| `olist_order_reviews_dataset.csv` | `review_comment_title` | 87,656 | 88.34% |
| `olist_order_reviews_dataset.csv` | `review_comment_message` | 58,247 | 58.70% |
| `olist_orders_dataset.csv` | `order_approved_at` | 160 | 0.16% |
| `olist_orders_dataset.csv` | `order_delivered_carrier_date` | 1,783 | 1.79% |
| `olist_orders_dataset.csv` | `order_delivered_customer_date` | 2,965 | 2.98% |
| `olist_products_dataset.csv` | `product_category_name` | 610 | 1.85% |
| `olist_products_dataset.csv` | `product_name_lenght` | 610 | 1.85% |
| `olist_products_dataset.csv` | `product_description_lenght` | 610 | 1.85% |
| `olist_products_dataset.csv` | `product_photos_qty` | 610 | 1.85% |
| `olist_products_dataset.csv` | `product_weight_g` | 2 | 0.01% |
| `olist_products_dataset.csv` | `product_length_cm` | 2 | 0.01% |
| `olist_products_dataset.csv` | `product_height_cm` | 2 | 0.01% |
| `olist_products_dataset.csv` | `product_width_cm` | 2 | 0.01% |

## Numeric profiles

| File | Column | Type | Non-null | Parse errors | Min | Max | Negative |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `olist_geolocation_dataset.csv` | `geolocation_lat` | decimal | 1,000,163 | 0 | -36.6053744107061 | 45.06593318269697 | 998,827 |
| `olist_geolocation_dataset.csv` | `geolocation_lng` | decimal | 1,000,163 | 0 | -101.46676644931476 | 121.10539381057764 | 1,000,160 |
| `olist_order_items_dataset.csv` | `order_item_id` | integer | 112,650 | 0 | 1 | 21 | 0 |
| `olist_order_items_dataset.csv` | `price` | decimal | 112,650 | 0 | 0.85 | 6735.00 | 0 |
| `olist_order_items_dataset.csv` | `freight_value` | decimal | 112,650 | 0 | 0.00 | 409.68 | 0 |
| `olist_order_payments_dataset.csv` | `payment_sequential` | integer | 103,886 | 0 | 1 | 29 | 0 |
| `olist_order_payments_dataset.csv` | `payment_installments` | integer | 103,886 | 0 | 0 | 24 | 0 |
| `olist_order_payments_dataset.csv` | `payment_value` | decimal | 103,886 | 0 | 0.00 | 13664.08 | 0 |
| `olist_order_reviews_dataset.csv` | `review_score` | integer | 99,224 | 0 | 1 | 5 | 0 |
| `olist_products_dataset.csv` | `product_name_lenght` | integer | 32,341 | 0 | 5 | 76 | 0 |
| `olist_products_dataset.csv` | `product_description_lenght` | integer | 32,341 | 0 | 4 | 3992 | 0 |
| `olist_products_dataset.csv` | `product_photos_qty` | integer | 32,341 | 0 | 1 | 20 | 0 |
| `olist_products_dataset.csv` | `product_weight_g` | integer | 32,949 | 0 | 0 | 40425 | 0 |
| `olist_products_dataset.csv` | `product_length_cm` | integer | 32,949 | 0 | 7 | 105 | 0 |
| `olist_products_dataset.csv` | `product_height_cm` | integer | 32,949 | 0 | 2 | 105 | 0 |
| `olist_products_dataset.csv` | `product_width_cm` | integer | 32,949 | 0 | 6 | 118 | 0 |

## Timestamp profiles

Source timestamps được profile dưới dạng naive values. Timezone interpretation được áp dụng ở downstream theo data contract.

| File | Column | Non-null | Null | Parse errors | Min | Max |
| --- | --- | ---: | ---: | ---: | --- | --- |
| `olist_order_items_dataset.csv` | `shipping_limit_date` | 112,650 | 0 | 0 | 2016-09-19 00:15:34 | 2020-04-09 22:35:08 |
| `olist_order_reviews_dataset.csv` | `review_creation_date` | 99,224 | 0 | 0 | 2016-10-02 00:00:00 | 2018-08-31 00:00:00 |
| `olist_order_reviews_dataset.csv` | `review_answer_timestamp` | 99,224 | 0 | 0 | 2016-10-07 18:32:28 | 2018-10-29 12:27:35 |
| `olist_orders_dataset.csv` | `order_purchase_timestamp` | 99,441 | 0 | 0 | 2016-09-04 21:15:19 | 2018-10-17 17:30:18 |
| `olist_orders_dataset.csv` | `order_approved_at` | 99,281 | 160 | 0 | 2016-09-15 12:16:38 | 2018-09-03 17:40:06 |
| `olist_orders_dataset.csv` | `order_delivered_carrier_date` | 97,658 | 1,783 | 0 | 2016-10-08 10:34:01 | 2018-09-11 19:48:28 |
| `olist_orders_dataset.csv` | `order_delivered_customer_date` | 96,476 | 2,965 | 0 | 2016-10-11 13:46:32 | 2018-10-17 13:22:46 |
| `olist_orders_dataset.csv` | `order_estimated_delivery_date` | 99,441 | 0 | 0 | 2016-09-30 00:00:00 | 2018-11-12 00:00:00 |

## Relationship validation

| Relationship | From | To | Null key rows | Orphan rows | Enforcement | Status |
| --- | --- | --- | ---: | ---: | --- | --- |
| `orders_customer` | `olist_orders_dataset.csv` (customer_id) | `olist_customers_dataset.csv` (customer_id) | 0 | 0 | Required | PASS |
| `order_items_order` | `olist_order_items_dataset.csv` (order_id) | `olist_orders_dataset.csv` (order_id) | 0 | 0 | Required | PASS |
| `order_items_product` | `olist_order_items_dataset.csv` (product_id) | `olist_products_dataset.csv` (product_id) | 0 | 0 | Required | PASS |
| `order_items_seller` | `olist_order_items_dataset.csv` (seller_id) | `olist_sellers_dataset.csv` (seller_id) | 0 | 0 | Required | PASS |
| `order_payments_order` | `olist_order_payments_dataset.csv` (order_id) | `olist_orders_dataset.csv` (order_id) | 0 | 0 | Required | PASS |
| `order_reviews_order` | `olist_order_reviews_dataset.csv` (order_id) | `olist_orders_dataset.csv` (order_id) | 0 | 0 | Required | PASS |
| `products_category_translation` | `olist_products_dataset.csv` (product_category_name) | `product_category_name_translation.csv` (product_category_name) | 610 | 13 | Warning only | WARNING |

## Selected value profiles

| File | Column | Distinct non-null | Maximum frequency | Top values |
| --- | --- | ---: | ---: | --- |
| `olist_customers_dataset.csv` | `customer_unique_id` | 96,096 | 17 | 8d50f5eadf50201ccdcedfb9e2ac8455=17, 3e43e6105506432c953e165fb2acf44c=9, 1b6c7548a2a1f9037c1fd3ddfed95f33=7, 6469f99c1f9dfae7733b25662e7f1782=7, ca77025e7201e3b30c44b472ff346268=7, 12f5d6e1cbf93dafd9dcc19095df0b3d=6, 47c1a3033b8b77b3ab6e109eb4d5fdf3=6, 63cfc61cee11cbe306bff5857d00bfe4=6, dc813062e0fc23409cd255f7f53c7074=6, de34b16117594161a6a89c50b289d35a=6 |
| `olist_customers_dataset.csv` | `customer_state` | 27 | 41,746 | SP=41,746, RJ=12,852, MG=11,635, RS=5,466, PR=5,045, SC=3,637, BA=3,380, DF=2,140, ES=2,033, GO=2,020 |
| `olist_geolocation_dataset.csv` | `geolocation_zip_code_prefix` | 19,015 | 1,146 | 24220=1,146, 24230=1,102, 38400=965, 35500=907, 11680=879, 22631=832, 30140=810, 11740=788, 38408=773, 28970=743 |
| `olist_geolocation_dataset.csv` | `geolocation_state` | 27 | 404,268 | SP=404,268, MG=126,336, RJ=121,169, RS=61,851, PR=57,859, SC=38,328, BA=36,045, GO=20,139, ES=16,748, PE=16,432 |
| `olist_order_payments_dataset.csv` | `payment_type` | 5 | 76,795 | credit_card=76,795, boleto=19,784, voucher=5,775, debit_card=1,529, not_defined=3 |
| `olist_order_reviews_dataset.csv` | `review_id` | 98,410 | 3 | 08528f70f579f0c830189efc523d2182=3, 0c76e7a547a531e7bf9f0b99cba071c1=3, 1fb4ddc969e6bea80e38deec00393a6f=3, 2172867fd5b1a55f98fe4608e1547b4b=3, 2d6ac45f859465b5c185274a1c929637=3, 308316408775d1600dad81bd3184556d=3, 32415bbf6e341d5d517080a796f79b5c=3, 3415c9f764e478409e8e0660ae816dd2=3, 38821b5c496b678cf91acc34892805ad=3, 39b4603793c1c7f5f36d809b4a218664=3 |
| `olist_order_reviews_dataset.csv` | `review_score` | 5 | 57,328 | 5=57,328, 4=19,142, 1=11,424, 3=8,179, 2=3,151 |
| `olist_orders_dataset.csv` | `order_status` | 8 | 96,478 | delivered=96,478, shipped=1,107, canceled=625, unavailable=609, invoiced=314, processing=301, created=5, approved=2 |
| `olist_products_dataset.csv` | `product_category_name` | 73 | 3,029 | cama_mesa_banho=3,029, esporte_lazer=2,867, moveis_decoracao=2,657, beleza_saude=2,444, utilidades_domesticas=2,335, automotivo=1,900, informatica_acessorios=1,639, brinquedos=1,411, relogios_presentes=1,329, telefonia=1,134 |
| `olist_sellers_dataset.csv` | `seller_state` | 23 | 1,849 | SP=1,849, PR=349, MG=244, SC=190, RJ=171, RS=129, GO=40, DF=30, ES=23, BA=19 |

## Non-enforced findings

- `products_category_translation` có 13 orphan row(s). Sample keys: pc_gamer, portateis_cozinha_e_preparadores_de_alimentos.

## Interpretation

- Profile `PASS` nghĩa là snapshot khớp với row counts, key constraints, parse expectations và enforced relationships đã khai báo.
- Relationship `WARNING` được thiết kế là non-blocking nhưng phải được xử lý bởi downstream data-quality policy.
- Profile này mô tả clean source snapshot. Dirty fixtures và simulator batches sử dụng expected-error manifests riêng.
