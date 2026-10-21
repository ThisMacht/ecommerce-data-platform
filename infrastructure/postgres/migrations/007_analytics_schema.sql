-- Local dbt schema, owned by the existing restricted loader role.
CREATE SCHEMA analytics AUTHORIZATION :"source_loader_user";
REVOKE ALL ON SCHEMA analytics FROM PUBLIC;
GRANT USAGE ON SCHEMA analytics TO :"source_reader_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"source_loader_user" IN SCHEMA analytics
    GRANT SELECT ON TABLES TO :"source_reader_user";
