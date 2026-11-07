
CREATE POLICY "project_images_all" ON storage.objects FOR ALL TO anon, authenticated
USING (bucket_id = 'project-images') WITH CHECK (bucket_id = 'project-images');
