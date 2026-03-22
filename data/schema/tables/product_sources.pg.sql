CREATE TABLE public.product_sources (
  sequence_number integer NULL,
  record_type_identifier integer NULL,
  nappi_product_code integer NULL,
  nappi_suffix integer NULL,
  product_name text NULL,
  product_strength text NULL,
  dosage_form_code character varying(10) NULL,
  product_pack_size integer NULL,
  manufacturer_code character varying(10) NULL,
  ean_product_code bigint NULL,
  catalogue_number character varying(50) NULL,
  product_type character varying(10) NULL,
  number_of_uses integer NULL,
  filler text NULL,
  full_product_name text NULL,
  record_update_date date NULL,
  record_update_time time without time zone NULL
);
