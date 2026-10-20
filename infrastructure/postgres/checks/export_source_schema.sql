-- Catalog-only report; safe to run with the source reader role.
WITH table_catalog AS (
    SELECT
        namespace.nspname AS schema_name,
        relation.relname AS table_name,
        jsonb_build_object(
            'columns', (
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'name', attribute.attname,
                        'postgres_type', format_type(attribute.atttypid, attribute.atttypmod),
                        'nullable', NOT attribute.attnotnull,
                        'identity', attribute.attidentity
                    ) ORDER BY attribute.attnum
                )
                FROM pg_attribute attribute
                WHERE attribute.attrelid = relation.oid
                  AND attribute.attnum > 0
                  AND NOT attribute.attisdropped
            ),
            'primary_key', (
                SELECT COALESCE(jsonb_agg(attribute.attname ORDER BY key.ordinality), '[]'::jsonb)
                FROM pg_constraint constraint_definition
                CROSS JOIN LATERAL unnest(constraint_definition.conkey)
                    WITH ORDINALITY AS key(attribute_number, ordinality)
                JOIN pg_attribute attribute
                  ON attribute.attrelid = relation.oid
                 AND attribute.attnum = key.attribute_number
                WHERE constraint_definition.conrelid = relation.oid
                  AND constraint_definition.contype = 'p'
            ),
            'unique_keys', (
                SELECT COALESCE(jsonb_agg(unique_key.columns), '[]'::jsonb)
                FROM (
                    SELECT jsonb_agg(attribute.attname ORDER BY key.ordinality) AS columns
                    FROM pg_constraint constraint_definition
                    CROSS JOIN LATERAL unnest(constraint_definition.conkey)
                        WITH ORDINALITY AS key(attribute_number, ordinality)
                    JOIN pg_attribute attribute
                      ON attribute.attrelid = relation.oid
                     AND attribute.attnum = key.attribute_number
                    WHERE constraint_definition.conrelid = relation.oid
                      AND constraint_definition.contype IN ('p', 'u')
                    GROUP BY constraint_definition.oid
                ) unique_key
            )
        ) AS definition
    FROM pg_class relation
    JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
    WHERE relation.relkind IN ('r', 'p')
      AND namespace.nspname !~ '^pg_'
      AND namespace.nspname <> 'information_schema'
), schema_catalog AS (
    SELECT
        schema_name,
        jsonb_object_agg(table_name, definition ORDER BY table_name) AS tables
    FROM table_catalog
    GROUP BY schema_name
)
SELECT jsonb_build_object(
    'schemas',
    COALESCE(jsonb_object_agg(schema_name, tables ORDER BY schema_name), '{}'::jsonb)
)
FROM schema_catalog;
