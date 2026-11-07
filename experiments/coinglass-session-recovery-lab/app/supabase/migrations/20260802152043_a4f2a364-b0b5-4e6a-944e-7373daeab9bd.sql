ALTER TABLE public.project_images
  ADD COLUMN IF NOT EXISTS caption text,
  ADD COLUMN IF NOT EXISTS description text,
  ADD COLUMN IF NOT EXISTS original_storage_path text;