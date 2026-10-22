-- Extend the existing BI group to the three published aggregate marts only.
-- dbt preserves these grants for new views and subsequent rebuilds.
DO $$ DECLARE model text; BEGIN
    FOREACH model IN ARRAY ARRAY['mart_product_daily', 'mart_seller_daily', 'mart_payment_type_daily'] LOOP
        IF to_regclass(format('analytics.%I', model)) IS NOT NULL THEN
            EXECUTE format('GRANT SELECT ON analytics.%I TO olist_bi_reader', model);
        END IF;
    END LOOP;
END $$;
