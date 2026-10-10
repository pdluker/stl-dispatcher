-- GVP volcano RSS via Supabase (2026-10-10)
-- volcano.si.edu answers Cloudflare Workers with 403 (blocked by IP), but this
-- Postgres can fetch it with browser headers. pg_cron runs public.gvp_rss_refresh()
-- every 3 hours: it saves the previous pg_net response (if it was a good 200 RSS)
-- into public.gvp_rss_cache, then starts a new fetch. stl-dispatcher's earthIngest
-- reads the copy through PostgREST with the anon key (read-only RLS policy).
-- Safe to re-run. Project: baknyepgyjurarojdkhy. Run in Dashboard -> SQL Editor.

-- 1. Table + read-only policy (already created 2026-10-10; these are no-ops if so)
create table if not exists public.gvp_rss_cache (
  id int primary key default 1 check (id = 1),
  fetched_at timestamptz,
  last_status int,
  last_attempt_at timestamptz,
  body text,
  request_id bigint
);
insert into public.gvp_rss_cache (id) values (1) on conflict do nothing;
alter table public.gvp_rss_cache enable row level security;
drop policy if exists gvp_rss_cache_public_read on public.gvp_rss_cache;
create policy gvp_rss_cache_public_read on public.gvp_rss_cache
  for select to anon, authenticated using (true);
revoke insert, update, delete, truncate on public.gvp_rss_cache from anon, authenticated;

-- 2. Refresh function (collect previous response, start next fetch)
create or replace function public.gvp_rss_refresh() returns void
language plpgsql security invoker set search_path = public, extensions
as $$
declare
  prev bigint;
  r record;
begin
  select request_id into prev from public.gvp_rss_cache where id = 1;
  if prev is not null then
    select status_code, content, created into r from net._http_response where id = prev;
    if found then
      if r.status_code = 200 and length(r.content) > 1000 and position('<rss' in r.content) > 0 then
        update public.gvp_rss_cache
           set body = r.content, fetched_at = r.created, last_status = 200, last_attempt_at = r.created
         where id = 1;
      else
        update public.gvp_rss_cache set last_status = r.status_code, last_attempt_at = r.created where id = 1;
      end if;
    end if;
  end if;
  update public.gvp_rss_cache
     set request_id = net.http_get(
           url := 'https://volcano.si.edu/news/WeeklyVolcanoRSS.xml',
           headers := jsonb_build_object(
             'User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
             'Accept', 'application/rss+xml, application/xml;q=0.9, */*;q=0.8',
             'Accept-Language', 'en-US,en;q=0.9'),
           timeout_milliseconds := 20000)
   where id = 1;
end;
$$;
revoke execute on function public.gvp_rss_refresh() from public, anon, authenticated;

-- 3. Schedule every 3 hours at :07 (pg_net keeps responses ~6h, so each run collects the last)
select cron.unschedule('gvp-rss-refresh') where exists (select 1 from cron.job where jobname = 'gvp-rss-refresh');
select cron.schedule('gvp-rss-refresh', '7 */3 * * *', 'select public.gvp_rss_refresh()');

-- 4. Kick off the first fetch now
select public.gvp_rss_refresh();
