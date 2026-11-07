-- Exact function definition read from the existing database on 2026-09-08.
-- Historical dependency of the September 7 permission migration; test database only.
CREATE OR REPLACE FUNCTION public.clone_version_to_template(_source_version uuid, _target_template uuid)
 RETURNS TABLE(id uuid)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'app_private'
AS $function$
DECLARE
  _uid uuid:=auth.uid(); _next integer; _new_version uuid; _stage record; _group record; _question record; _new_stage uuid; _new_group uuid;
BEGIN
  IF _uid IS NULL OR NOT EXISTS(SELECT 1 FROM public.user_roles WHERE user_id=_uid AND role='admin') THEN
    RAISE EXCEPTION 'admin permission required';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM public.template_versions WHERE id=_source_version) THEN RAISE EXCEPTION 'source version not found'; END IF;
  IF NOT EXISTS(SELECT 1 FROM public.templates WHERE id=_target_template) THEN RAISE EXCEPTION 'target template not found'; END IF;
  SELECT COALESCE(max(version_number),0)+1 INTO _next FROM public.template_versions WHERE template_id=_target_template;
  INSERT INTO public.template_versions(template_id,version_number,status) VALUES(_target_template,_next,'draft') RETURNING public.template_versions.id INTO _new_version;
  FOR _stage IN SELECT * FROM public.template_stages WHERE template_version_id=_source_version ORDER BY position LOOP
    INSERT INTO public.template_stages(template_version_id,title,description,position,active,trade_action)
    VALUES(_new_version,_stage.title,_stage.description,_stage.position,_stage.active,COALESCE(_stage.trade_action,'none')) RETURNING public.template_stages.id INTO _new_stage;
    FOR _group IN SELECT * FROM public.template_groups WHERE stage_id=_stage.id ORDER BY position LOOP
      INSERT INTO public.template_groups(stage_id,title,description,position,collapsible,active,behavior)
      VALUES(_new_stage,_group.title,_group.description,_group.position,_group.collapsible,_group.active,_group.behavior) RETURNING public.template_groups.id INTO _new_group;
      FOR _question IN SELECT * FROM public.template_questions WHERE group_id=_group.id ORDER BY position LOOP
        INSERT INTO public.template_questions(group_id,title,description,question_type,position,required,active,options_json,visibility_rule_json,help_text,source_url,links_json,field_key,question_key,default_text_align,pinned_scope)
        VALUES(_new_group,_question.title,_question.description,_question.question_type,_question.position,_question.required,_question.active,_question.options_json,_question.visibility_rule_json,_question.help_text,_question.source_url,_question.links_json,_question.field_key,_question.question_key,_question.default_text_align,COALESCE(_question.pinned_scope,'none'));
      END LOOP;
    END LOOP;
  END LOOP;
  RETURN QUERY SELECT _new_version;
END;
$function$
;

