\set ON_ERROR_STOP on
-- Supabase shell only. Legacy tables, constraints, policies and triggers below
-- are installed from the actual historical migrations without schema substitutes.
CREATE ROLE anon NOLOGIN;
CREATE ROLE authenticated NOLOGIN;
CREATE ROLE service_role NOLOGIN BYPASSRLS;
CREATE SCHEMA auth;
CREATE TABLE auth.users(id uuid PRIMARY KEY, email text UNIQUE, email_confirmed_at timestamptz, raw_user_meta_data jsonb DEFAULT '{}');
CREATE FUNCTION auth.uid() RETURNS uuid LANGUAGE sql STABLE AS $$
 SELECT nullif(current_setting('request.jwt.claim.sub', true), '')::uuid;
$$;
GRANT USAGE ON SCHEMA auth TO authenticated, anon;
CREATE SCHEMA storage;
CREATE TABLE storage.buckets(id text PRIMARY KEY, name text NOT NULL, public boolean DEFAULT false, file_size_limit bigint, allowed_mime_types text[]);
CREATE TABLE storage.objects(id uuid PRIMARY KEY DEFAULT gen_random_uuid(), bucket_id text REFERENCES storage.buckets(id), name text NOT NULL, metadata jsonb, UNIQUE(bucket_id,name));
ALTER TABLE storage.objects ENABLE ROW LEVEL SECURITY;
GRANT USAGE ON SCHEMA storage TO authenticated, anon;
GRANT SELECT, INSERT, UPDATE, DELETE ON storage.objects TO authenticated;
CREATE FUNCTION storage.foldername(name text) RETURNS text[] LANGUAGE sql IMMUTABLE AS $$
 SELECT (string_to_array(name, '/'))[1:greatest(array_length(string_to_array(name, '/'), 1)-1, 0)];
$$;
INSERT INTO storage.buckets(id,name) VALUES ('project-images','project-images');
CREATE PUBLICATION supabase_realtime;
\ir ../../supabase/migrations/20260731092749_94d73ddc-06f9-429f-b2d1-dc1b295699c7.sql
\ir ../../supabase/migrations/20260731092812_16b0b9e6-57ca-454c-bb3d-cd774ac1a93e.sql
\ir ../../supabase/migrations/20260731144916_1667afb3-63d9-4937-8d6c-3756d9945c0a.sql
\ir ../../supabase/migrations/20260731144937_b6822a91-a3bf-4de5-b2e5-9f3ba41d345a.sql
\ir ../../supabase/migrations/20260801120817_7f44f3a9-2035-4c74-a894-29cf6d563ed9.sql
\ir ../../supabase/migrations/20260801120917_2b61d6fd-a56b-44a1-b0ec-dabb5c769c0e.sql
\ir ../../supabase/migrations/20260802152043_a4f2a364-b0b5-4e6a-944e-7373daeab9bd.sql
\ir ../../supabase/migrations/20260804171041_e15af61f-09de-4584-9fa8-142bcfd7d140.sql
\ir ../../supabase/migrations/20260805111939_4937ff7f-c622-4c91-a4c3-94f8c2c934b6.sql
\ir ../../supabase/migrations/20260805114235_5ecf38ad-23e9-48f5-b683-8ae4485f1adf.sql
\ir ../../supabase/migrations/20260805115755_5045ef24-ed00-4667-bde1-93afb76a9ea8.sql
\ir ../../supabase/migrations/20260808073402_29a414e9-68d3-4a69-9b7a-31b3ab1963a8.sql
\ir ../../supabase/migrations/20260816043640_8f54bd64-322c-490e-9c75-b46b55bc1e0a.sql
\ir ../../supabase/migrations/20260816120453_18e1a6cf-a49e-4663-8ae8-1567ac1921d8.sql
\ir ../../supabase/migrations/20260816120543_b88720aa-0a4d-4e7c-92be-23701bca5e37.sql
\ir ../../supabase/migrations/20260816121324_1864067d-b806-4dcf-a41c-ecbe310b0e2a.sql
\ir ../../supabase/migrations/20260816121417_843381b2-3699-46df-a3ff-86f7041b146d.sql
\ir ../../supabase/migrations/20260816121657_65af9e3e-abdc-4c1e-b055-4f148fd762fa.sql
\ir ../../supabase/migrations/20260818135813_e5143663-258f-4e2b-bb33-28d50c3c3c89.sql
\ir ../../supabase/migrations/20260818140113_e2148e3b-c4eb-4a4f-9f3e-71e874bf376e.sql
\ir ../../supabase/migrations/20260818140416_9868f79d-64c6-4285-8284-b7351a8201c7.sql
\ir ../../supabase/migrations/20260818141353_b3ba9e9c-e993-4b7b-9d0c-23335887c580.sql
\ir ../../supabase/migrations/20260819071622_1f3104d8-c8b6-49fd-92d5-5f141fe2518c.sql
\ir ../../supabase/migrations/20260819071642_3ab5a456-8ec7-41cf-87e5-028039d2cfac.sql
\ir ../../supabase/migrations/20260819074950_af15153b-e9f8-4b7b-bf98-a1404685af57.sql
\ir ../../supabase/migrations/20260819075941_d9850e8f-9154-46e2-9a0e-e6c9be5b3dc3.sql
\ir ../../supabase/migrations/20260819080007_6961ff4f-405d-4fa1-a21d-cccb15e2a63b.sql
\ir ../../supabase/migrations/20260819081017_937d279b-797d-4b30-917e-bd44959f81fe.sql
\ir ../../supabase/migrations/20260819081359_cdda4b21-6b47-4a1e-87fb-56825b02fa47.sql
\ir ../../supabase/migrations/20260819084451_d10eb08c-4f75-43c6-84a6-e84068c731c0.sql
\ir ../../supabase/migrations/20260825090402_a887177f-2515-47ab-baef-45ea029f09a5.sql
\ir ../../supabase/migrations/20260825094000_23eeba97-b2d4-4fe3-8410-d6f14372cf9c.sql
\ir ../../supabase/migrations/20260825094029_7c6375a1-5c4b-40f3-9f80-59485e1ecc3a.sql
\ir ../../supabase/migrations/20260826120000_phase4b_liquidity_integrity.sql
\ir ../../supabase/migrations/20260826123000_economic_calendar.sql
\ir ../../supabase/migrations/20260826143000_template_safety_and_yoav_questions.sql
\ir ../../supabase/migrations/20260826150000_multi_trade_cycles.sql
