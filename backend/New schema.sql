-- WARNING: This schema is for context only and is not meant to be run.
-- Table order and constraints may not be valid for execution.

CREATE TABLE public.debug_log (
  id integer NOT NULL DEFAULT nextval('debug_log_id_seq'::regclass),
  message text,
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT debug_log_pkey PRIMARY KEY (id)
);
CREATE TABLE public.job_archive (
  work_order integer NOT NULL,
  site_name text,
  site_id integer,
  address text,
  site_city text,
  site_state text,
  site_zip text,
  site_latitude numeric,
  site_longitude numeric,
  due_date date,
  sow_1 text,
  sow_2 text,
  jp_status text,
  eligible_technicians text,
  archived_date timestamp without time zone DEFAULT now(),
  archive_reason text,
  archived_by text,
  CONSTRAINT job_archive_pkey PRIMARY KEY (work_order)
);
CREATE TABLE public.job_history (
  work_order bigint NOT NULL,
  site_id bigint,
  site_name text,
  scheduled_date date NOT NULL,
  technician_id bigint,
  sow_1 text,
  jp_priority text,
  duration numeric,
  latitude numeric,
  longitude numeric,
  region text,
  imported_at timestamp with time zone DEFAULT now(),
  CONSTRAINT job_history_pkey PRIMARY KEY (work_order)
);
CREATE TABLE public.job_pool (
  work_order bigint NOT NULL,
  site_name text,
  site_address text,
  site_city text,
  site_state text,
  latitude double precision,
  longitude double precision,
  jp_status text,
  jp_priority text,
  due_date date,
  sow_1 text,
  flag_missing_due_date boolean,
  flag_past_due boolean,
  tank_test_only boolean,
  is_recurring_site boolean,
  night_test boolean,
  days_til_due bigint,
  tech_count bigint,
  region text,
  duration double precision,
  cluster_id integer,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  geom USER-DEFINED,
  site_id bigint,
  is_night boolean,
  cluster_label text,
  CONSTRAINT job_pool_pkey PRIMARY KEY (work_order),
  CONSTRAINT job_pool_site_id_fkey FOREIGN KEY (site_id) REFERENCES public.sites(site_id)
);
CREATE TABLE public.job_technician_eligibility (
  work_order bigint NOT NULL,
  technician_id bigint NOT NULL,
  CONSTRAINT job_technician_eligibility_pkey PRIMARY KEY (work_order, technician_id),
  CONSTRAINT fk_work_order FOREIGN KEY (work_order) REFERENCES public.job_pool(work_order),
  CONSTRAINT fk_technician FOREIGN KEY (technician_id) REFERENCES public.technicians(technician_id)
);
CREATE TABLE public.regions (
  region_id integer NOT NULL DEFAULT nextval('regions_region_id_seq'::regclass),
  region_name text NOT NULL UNIQUE,
  boundary USER-DEFINED,
  properties jsonb,
  created_at timestamp with time zone DEFAULT now(),
  center_latitude double precision,
  center_longitude double precision,
  CONSTRAINT regions_pkey PRIMARY KEY (region_id)
);
CREATE TABLE public.reservations (
  tech_id bigint NOT NULL,
  date date NOT NULL,
  region text,
  cluster_id integer,
  is_night_week boolean DEFAULT false,
  note text,
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now(),
  CONSTRAINT reservations_pkey PRIMARY KEY (tech_id, date),
  CONSTRAINT reservations_tech_id_fkey FOREIGN KEY (tech_id) REFERENCES public.technicians(technician_id)
);
CREATE TABLE public.scheduled_job_additional_techs (
  work_order bigint NOT NULL,
  technician_id bigint NOT NULL,
  added_at timestamp with time zone DEFAULT now(),
  CONSTRAINT scheduled_job_additional_techs_pkey PRIMARY KEY (work_order, technician_id),
  CONSTRAINT fk_work_order FOREIGN KEY (work_order) REFERENCES public.scheduled_jobs(work_order),
  CONSTRAINT fk_technician FOREIGN KEY (technician_id) REFERENCES public.technicians(technician_id)
);
CREATE TABLE public.scheduled_jobs (
  work_order bigint NOT NULL,
  site_name text,
  site_city text,
  site_state text,
  technician_id bigint,
  date date,
  due_date date,
  duration double precision,
  assigned_tech_name text,
  sow_1 text,
  is_night_job boolean NOT NULL DEFAULT false,
  start_time timestamp with time zone,
  end_time timestamp with time zone,
  drive_time_hours numeric,
  cluster_id integer,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  site_id bigint,
  latitude numeric,
  longitude numeric,
  CONSTRAINT scheduled_jobs_pkey PRIMARY KEY (work_order),
  CONSTRAINT Scheduled_Jobs_work_order_fkey FOREIGN KEY (work_order) REFERENCES public.job_pool(work_order),
  CONSTRAINT Scheduled_Jobs_assigned_tech_id_fkey FOREIGN KEY (technician_id) REFERENCES public.technicians(technician_id),
  CONSTRAINT scheduled_jobs_site_id_fkey FOREIGN KEY (site_id) REFERENCES public.sites(site_id),
  CONSTRAINT Scheduled_Jobs_assigned_tech_name_fkey FOREIGN KEY (assigned_tech_name) REFERENCES public.technicians(name)
);
CREATE TABLE public.site_distance_matrix (
  from_site_name text NOT NULL,
  to_site_name text NOT NULL,
  distance_miles numeric,
  drive_time_hours numeric,
  from_region text,
  to_region text,
  CONSTRAINT site_distance_matrix_pkey PRIMARY KEY (from_site_name, to_site_name)
);
CREATE TABLE public.site_distances (
  from_site_id bigint NOT NULL,
  to_site_id bigint NOT NULL,
  distance_miles double precision,
  drive_time_hours double precision,
  from_region text,
  to_region text,
  CONSTRAINT site_distances_pkey PRIMARY KEY (from_site_id, to_site_id),
  CONSTRAINT site_distances_from_fk FOREIGN KEY (from_site_id) REFERENCES public.sites(site_id),
  CONSTRAINT site_distances_to_fk FOREIGN KEY (to_site_id) REFERENCES public.sites(site_id)
);
CREATE TABLE public.sites (
  site_id bigint NOT NULL,
  site_name text,
  site_address text,
  site_city text,
  site_state text,
  region text,
  latitude double precision,
  longitude double precision,
  geom USER-DEFINED,
  CONSTRAINT sites_pkey PRIMARY KEY (site_id)
);
CREATE TABLE public.spatial_ref_sys (
  srid integer NOT NULL CHECK (srid > 0 AND srid <= 998999),
  auth_name character varying,
  auth_srid integer,
  srtext character varying,
  proj4text character varying,
  CONSTRAINT spatial_ref_sys_pkey PRIMARY KEY (srid)
);
CREATE TABLE public.stg_job_history (
  work_order bigint,
  site_id bigint,
  site_name text,
  scheduled_date text,
  technician_id bigint,
  sow_1 text,
  jp_priority text,
  duration numeric,
  latitude numeric,
  longitude numeric
);
CREATE TABLE public.stg_job_pool (
  work_order bigint,
  siteid bigint,
  site_name text,
  site_address text,
  site_city text,
  site_state text,
  latitude double precision,
  longitude double precision,
  jp_status text,
  jp_priority text,
  due_date text,
  region text,
  sow_1 text,
  flag_missing_due_date boolean,
  duration double precision,
  night_test boolean,
  days_til_due_from_schedule bigint,
  tech_count bigint,
  cluster_id text,
  is_recurring_site boolean
);
CREATE TABLE public.stg_job_technician_eligibility (
  work_order bigint,
  technician_id bigint
);
CREATE TABLE public.stg_site_distances (
  from_site_id bigint,
  to_site_id bigint,
  distance_miles double precision,
  drive_time_hours double precision,
  from_region text,
  to_region text
);
CREATE TABLE public.stg_technicians (
  technician_id bigint,
  name text,
  home_location text,
  home_latitude double precision,
  home_longitude double precision,
  qualified_tests text,
  states_allowed text,
  states_excluded text,
  max_weekly_hours bigint,
  max_daily_hours bigint,
  active boolean
);
CREATE TABLE public.technicians (
  technician_id bigint NOT NULL,
  name text NOT NULL UNIQUE,
  home_location text,
  home_latitude double precision,
  home_longitude double precision,
  max_weekly_hours bigint,
  max_daily_hours bigint,
  active boolean,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  home_geom USER-DEFINED,
  qualified_tests text,
  states_allowed text,
  states_excluded text,
  CONSTRAINT technicians_pkey PRIMARY KEY (technician_id)
);
CREATE TABLE public.time_off_requests (
  id integer NOT NULL DEFAULT nextval('time_off_requests_id_seq'::regclass),
  technician_id integer,
  start_date date NOT NULL,
  end_date date NOT NULL,
  hours_per_day numeric DEFAULT 8,
  reason text,
  approved boolean DEFAULT false,
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT time_off_requests_pkey PRIMARY KEY (id),
  CONSTRAINT time_off_requests_technician_id_fkey FOREIGN KEY (technician_id) REFERENCES public.technicians(technician_id)
);
