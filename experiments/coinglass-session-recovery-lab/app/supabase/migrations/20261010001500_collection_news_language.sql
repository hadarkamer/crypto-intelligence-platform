-- Null retains the account's default; existing content and preferences stay intact.
alter table public.user_preferences
  add column if not exists collection_news_language text;

alter table public.user_preferences
  drop constraint if exists user_preferences_collection_news_language_check;
alter table public.user_preferences
  add constraint user_preferences_collection_news_language_check
  check (collection_news_language is null or collection_news_language in ('he', 'en'));

-- Existing own-user RLS on user_preferences also protects this personal setting.
