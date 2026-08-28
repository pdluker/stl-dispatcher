# apply-health-gate.ps1
# Run from C:\Users\pdluk\stl-dispatcher
#
# Gates /health behind the same checkAuth()/DISPATCH_SECRET pattern already
# used on /trigger. Unauthenticated callers get {ok:true}; authenticated
# callers get the full heartbeat detail, unchanged from today.
#
# Uses single-quoted (literal) here-strings and a plain .Contains()/.Replace()
# check — NOT -like/-notlike, since those treat [ ] * ? as wildcard pattern
# characters and this JS is full of [array, destructuring] syntax that would
# silently break wildcard matching.

$path = ".\dispatcher.js"

if (-not (Test-Path $path)) {
    Write-Error "dispatcher.js not found in current directory. cd to stl-dispatcher first."
    exit 1
}

$backupPath = ".\dispatcher.js.bak-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
Copy-Item $path $backupPath
Write-Host "Backed up current file to $backupPath"

$content = Get-Content -Path $path -Raw

$old = @'
    if (url.pathname === '/health' && request.method === 'GET') {
      const [keepaliveHb, syncHb, stlBucketHb, spaceHb, earthHb, intelHb, podcastHb, schoolsHb, musicHb, sportsHb] = await Promise.all([
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:keepalive'),
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:status-sync'),
        readHeartbeat(env.STATUS_KV, 'stl-bucket:refresh'),
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:space-ingest'),
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:earth-ingest'),
        readHeartbeat(env.STATUS_KV, 'intel:refresh'),
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:podcast-ingest'),
        readHeartbeat(env.STATUS_KV, 'schools:refresh'),
        readHeartbeat(env.STATUS_KV, 'stl-music:pulse'),
        readHeartbeat(env.STATUS_KV, 'stl-sports:pulse'),
      ]);

      // Surfaces the actual thrown error from the last podcastIngest failure,
      // if any -- written either by Task 7's catch block (a genuine thrown
      // exception) or by the ok:false else-branch below it (runPodcastIngest
      // returning a failure rather than throwing) -- cleared on next success.
      // Exists so a silent scheduled-run failure is diagnosable from /health
      // alone the next morning, without needing a wrangler tail session
      // running at the exact minute it happened (confirmed necessary after
      // three such failures in a row, Jul 30 - Aug 1).
      let podcastLastError = null;
      try {
        const raw = await env.STATUS_KV.get('podcast:last-error');
        if (raw) podcastLastError = JSON.parse(raw);
      } catch { /* absent is the normal, healthy case */ }

      return jsonResponse({
        ok: true,
        keepalive: evaluateHeartbeat(keepaliveHb, KEEPALIVE_MAX_AGE_HOURS),
        statusSync: evaluateHeartbeat(syncHb, 24),       // expect daily
        stlBucket: evaluateHeartbeat(stlBucketHb, 24 * 8), // expect weekly (Friday) + grace
        spaceIngest: evaluateHeartbeat(spaceHb, 24),     // expect daily
        earthIngest: evaluateHeartbeat(earthHb, 24),     // expect daily
        intelRefresh: evaluateHeartbeat(intelHb, 24 * 4), // expect Mon/Wed/Thu + grace over the Fri-Sun gap
        podcastIngest: evaluateHeartbeat(podcastHb, 30), // expect daily; small grace for TTS/upload time
        podcastLastError,
        schoolsRefresh: evaluateHeartbeat(schoolsHb, 24 * 4), // expect Mon/Wed/Fri + grace over the weekend gap
        musicRefresh: evaluateHeartbeat(musicHb, 24 * 8), // expect Sunday + grace
        sportsRefresh: evaluateHeartbeat(sportsHb, 24 * 8), // expect Sunday + grace
      });
    }
'@

$new = @'
    if (url.pathname === '/health' && request.method === 'GET') {
      // Gated Aug 6, 2026 -- this endpoint answered on dispatch.stluker.com
      // (and previously stl-dispatcher.pdluker.workers.dev) with zero auth,
      // returning per-task last-run timestamps and error detail to anyone.
      // Same checkAuth()/DISPATCH_SECRET pattern already used on /trigger
      // below -- reused rather than a second auth scheme for one Worker.
      // Unauthenticated callers still get a 200 {ok:true} so anything
      // polling this for a basic uptime check keeps working unchanged.
      const unauthorized = await checkAuth(request, env.DISPATCH_SECRET);
      if (unauthorized) {
        return jsonResponse({ ok: true });
      }

      const [keepaliveHb, syncHb, stlBucketHb, spaceHb, earthHb, intelHb, podcastHb, schoolsHb, musicHb, sportsHb] = await Promise.all([
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:keepalive'),
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:status-sync'),
        readHeartbeat(env.STATUS_KV, 'stl-bucket:refresh'),
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:space-ingest'),
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:earth-ingest'),
        readHeartbeat(env.STATUS_KV, 'intel:refresh'),
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:podcast-ingest'),
        readHeartbeat(env.STATUS_KV, 'schools:refresh'),
        readHeartbeat(env.STATUS_KV, 'stl-music:pulse'),
        readHeartbeat(env.STATUS_KV, 'stl-sports:pulse'),
      ]);

      // Surfaces the actual thrown error from the last podcastIngest failure,
      // if any -- written either by Task 7's catch block (a genuine thrown
      // exception) or by the ok:false else-branch below it (runPodcastIngest
      // returning a failure rather than throwing) -- cleared on next success.
      // Exists so a silent scheduled-run failure is diagnosable from /health
      // alone the next morning, without needing a wrangler tail session
      // running at the exact minute it happened (confirmed necessary after
      // three such failures in a row, Jul 30 - Aug 1).
      let podcastLastError = null;
      try {
        const raw = await env.STATUS_KV.get('podcast:last-error');
        if (raw) podcastLastError = JSON.parse(raw);
      } catch { /* absent is the normal, healthy case */ }

      return jsonResponse({
        ok: true,
        keepalive: evaluateHeartbeat(keepaliveHb, KEEPALIVE_MAX_AGE_HOURS),
        statusSync: evaluateHeartbeat(syncHb, 24),       // expect daily
        stlBucket: evaluateHeartbeat(stlBucketHb, 24 * 8), // expect weekly (Friday) + grace
        spaceIngest: evaluateHeartbeat(spaceHb, 24),     // expect daily
        earthIngest: evaluateHeartbeat(earthHb, 24),     // expect daily
        intelRefresh: evaluateHeartbeat(intelHb, 24 * 4), // expect Mon/Wed/Thu + grace over the Fri-Sun gap
        podcastIngest: evaluateHeartbeat(podcastHb, 30), // expect daily; small grace for TTS/upload time
        podcastLastError,
        schoolsRefresh: evaluateHeartbeat(schoolsHb, 24 * 4), // expect Mon/Wed/Fri + grace over the weekend gap
        musicRefresh: evaluateHeartbeat(musicHb, 24 * 8), // expect Sunday + grace
        sportsRefresh: evaluateHeartbeat(sportsHb, 24 * 8), // expect Sunday + grace
      });
    }
'@

if (-not $content.Contains($old)) {
    Write-Error "OLD BLOCK NOT FOUND -- dispatcher.js does not match the expected /health block exactly (whitespace, line endings, or the code itself may differ). No changes made. Open the file and apply the diff manually instead."
    exit 1
}

$updated = $content.Replace($old, $new)

# Write back without a BOM, matching a typical existing JS source file --
# a stray BOM at the top of a Worker's entry-adjacent module can occasionally
# cause tooling weirdness, so avoid it deliberately rather than relying on
# Set-Content's default (which adds a BOM in Windows PowerShell 5.1).
$utf8NoBom = New-Object System.Text.UTF8Encoding $false
[System.IO.File]::WriteAllText((Resolve-Path $path), $updated, $utf8NoBom)

Write-Host "Patch applied successfully to $path"
Write-Host "Backup remains at $backupPath in case you need to revert (Copy-Item $backupPath $path -Force)"
Write-Host ""
Write-Host "Next: wrangler deploy"
