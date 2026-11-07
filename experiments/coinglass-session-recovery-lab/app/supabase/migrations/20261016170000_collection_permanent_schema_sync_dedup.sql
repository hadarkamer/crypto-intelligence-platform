begin;

-- A full editor save bumps the template once per field/section write. Deferred
-- row triggers must reconcile only the final committed shape, once per revision.
-- A private table is used rather than a client-settable session variable.
-- One row per source is bounded: the workspace lock serializes transactions,
-- and a rollback restores the previous transaction/revision receipt.
create table if not exists app_private.collection_permanent_sync_revisions (
 template_id uuid primary key references public.v2_templates(id) on delete cascade,
 transaction_id bigint not null,
 editor_revision bigint not null
);
alter table app_private.collection_permanent_sync_revisions enable row level security;
revoke all on app_private.collection_permanent_sync_revisions from public,anon,authenticated,service_role;

-- CREATE OR REPLACE retains the trigger function's identity, owner and ACL.
-- Keep the original authorization and workspace serialization, then re-read the
-- current row: queued NEW.editor_revision values describe intermediate writes.
create or replace function app_private.sync_collection_permanent_fields_after_template()
returns trigger language plpgsql security definer
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare source public.v2_templates%rowtype; current_transaction bigint:=txid_current();
 marked integer;
begin
 if new.kind<>'collection' or auth.uid() is null then return new; end if;
 select * into source from public.v2_templates
  where id=new.id and kind='collection' and active and is_default;
 if not found or not public.v2_can_edit_workspace(source.workspace_id) then return new; end if;
 perform pg_advisory_xact_lock(hashtextextended(source.workspace_id::text||'collection',0));
 select * into source from public.v2_templates
  where id=new.id and kind='collection' and active and is_default;
 if not found or not public.v2_can_edit_workspace(source.workspace_id) then return new; end if;
 insert into app_private.collection_permanent_sync_revisions(transaction_id,template_id,editor_revision)
  values(current_transaction,source.id,source.editor_revision)
  on conflict(template_id) do update
   set transaction_id=excluded.transaction_id,editor_revision=excluded.editor_revision
   where collection_permanent_sync_revisions.transaction_id is distinct from excluded.transaction_id
      or collection_permanent_sync_revisions.editor_revision is distinct from excluded.editor_revision
  returning 1 into marked;
 if not found then return new; end if;
 -- Mark and repair are atomic: a failed repair rolls the marker back as well.
 perform app_private.sync_collection_permanent_fields(source.id);
 return new;
end $$;
revoke all on function app_private.sync_collection_permanent_fields_after_template()
 from public,anon,authenticated,service_role;

commit;
