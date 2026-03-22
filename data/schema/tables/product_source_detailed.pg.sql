
CREATE TABLE IF NOT EXISTS public.product_sources_detailed (
	sequence_number            INTEGER,
    record_type_identifier     INTEGER,
    parent_company_code        VARCHAR(10),
    product_source_code        VARCHAR(10),
    product_source_name        VARCHAR(255),
    product_source_indicator   CHAR(1),
    filler                     VARCHAR(50),
    record_update_date         DATE,
    record_update_time         TIME
);
