
CREATE TABLE IF NOT EXISTS public.botswana_district_boundaries (
	hasc_id TEXT,
	geometry_type TEXT,
	polygon_idx INTEGER,
	ring_idx INTEGER,
	point_idx INTEGER,
    longitude NUMERIC(9, 6),
	latitude NUMERIC(9, 6)
);
