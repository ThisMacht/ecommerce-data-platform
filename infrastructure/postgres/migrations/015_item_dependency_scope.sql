-- Keep one quarantine row per physical item when several FK rules are missing.
ALTER TABLE audit.quarantine_records DROP CONSTRAINT quarantine_records_rule_id_check;
ALTER TABLE audit.quarantine_records ADD CONSTRAINT quarantine_records_rule_id_check
    CHECK (rule_id IN ('missing_customer', 'missing_order')
        OR (source_table = 'order_items' AND rule_id = 'missing_item_dependencies'));
