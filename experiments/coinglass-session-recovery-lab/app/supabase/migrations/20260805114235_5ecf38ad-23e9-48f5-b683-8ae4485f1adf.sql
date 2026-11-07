ALTER TABLE public.project_images ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();
DROP TRIGGER IF EXISTS t6 ON public.project_images;
CREATE TRIGGER t6 BEFORE UPDATE ON public.project_images FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();