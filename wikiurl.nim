# wikiurl - list page names and URLs that contain a domain
# Copyright (C) 2026 GreenC
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import std/[os, parsecfg, strutils, httpclient, json, uri, algorithm, streams, osproc, re], zip/gzipfiles

# Handle the standard library split introduced in Nim 2.0
when NimMajor >= 2:
  import db_connector/db_mysql
else:
  import db_mysql

import cligen

type
  OutputMethod = enum
    methodAuto, methodApi, methodDumpStream, methodDumpDownload, methodSql

  # The master configuration object that holds our merged state
  WikiUrlConfig = object
    userId: string
    emailFile: string
    replicaCnf: string
    outDir: string
    domain: string
    sites: seq[string]
    ns: string
    nsList: seq[string]
    regex: string
    compiledRegex: Regex
    runMethod: OutputMethod
    showProgress: bool
    verbose: bool
    generateAllWikis: bool
    # File generation toggles
    genRaw: bool
    genArticles: bool
    genTsv: bool
    genJson: bool

# Forward declarations for our future engines
proc runApiEngine(cfg: WikiUrlConfig)
proc runDumpStreamEngine(cfg: WikiUrlConfig)
proc runDumpDownloadEngine(cfg: WikiUrlConfig)
proc runSqlEngine(cfg: WikiUrlConfig)

# -------------------------------------------------------------------------
# Configuration Merging Logic
# -------------------------------------------------------------------------

proc loadConfigFile(filePath: string, cfg: var WikiUrlConfig) =
  if not fileExists(filePath):
    return
    
  var dict = loadConfig(filePath)
  
  let parsedUser = dict.getSectionValue("Identity", "userid")
  let parsedEmail = dict.getSectionValue("Identity", "email_file")
  
  if parsedUser != "": cfg.userId = parsedUser
  if parsedEmail != "": cfg.emailFile = parsedEmail
    
  if dict.getSectionValue("Authentication", "replica_cnf") != "":
    cfg.replicaCnf = dict.getSectionValue("Authentication", "replica_cnf")

  if dict.getSectionValue("Defaults", "output_dir") != "":
    cfg.outDir = dict.getSectionValue("Defaults", "output_dir")
    
  if dict.getSectionValue("Global", "generate_raw") != "":
    cfg.genRaw = parseBool(dict.getSectionValue("Global", "generate_raw"))
  if dict.getSectionValue("Global", "generate_articles") != "":
    cfg.genArticles = parseBool(dict.getSectionValue("Global", "generate_articles"))
  if dict.getSectionValue("Global", "generate_tsv") != "":
    cfg.genTsv = parseBool(dict.getSectionValue("Global", "generate_tsv"))
  if dict.getSectionValue("Global", "generate_json") != "":
    cfg.genJson = parseBool(dict.getSectionValue("Global", "generate_json"))

proc validateConfig(cfg: WikiUrlConfig) =
  if cfg.domain == "":
    quit("Error: Target domain (-d) is required.")
  if cfg.sites.len == 0:
    quit("Error: Target site (-s) is required.")
  if cfg.userId == "" or cfg.emailFile == "":
    quit("Error: WMF User-Agent requirements not met. Please define 'userid' and 'email_file' in ~/.wikiurlrc")

# -------------------------------------------------------------------------
# Shared Engine Helpers
# -------------------------------------------------------------------------

proc buildUserAgent(cfg: WikiUrlConfig): string =
  # Safely read the email from the secrets file
  var email = "unknown@example.com"
  if fileExists(cfg.emailFile):
    email = readFile(cfg.emailFile).strip()
  else:
    quit("Error: Could not read email file at " & cfg.emailFile)
    
  # Format: wikiurl/1.0 (https://github.com/greencardamom/wikiurl; User:GreenC; mailto:email@example.com)
  return "wikiurl/1.0 (https://github.com/greencardamom/wikiurl; " & cfg.userId & "; mailto:" & email & ")"

proc generateAllWikisTxt(cfg: WikiUrlConfig) =
  let outPath = cfg.outDir / "allwikis.txt"
  if cfg.showProgress:
    stderr.writeLine("[Setup] Fetching active wiki list from WMF NOC to " & outPath & " ...")
  
  let agent = buildUserAgent(cfg)
  var client = newHttpClient(userAgent = agent)
  defer: client.close()
  
  try:
    let allDb = client.getContent("https://noc.wikimedia.org/conf/dblists/all.dblist").splitLines()
    let closedDb = client.getContent("https://noc.wikimedia.org/conf/dblists/closed.dblist").splitLines()
    let privateDb = client.getContent("https://noc.wikimedia.org/conf/dblists/private.dblist").splitLines()
    
    var activeWikis: seq[string] = @[]
    for db in allDb:
      let cleanDb = db.strip()
      if cleanDb != "" and cleanDb notin closedDb and cleanDb notin privateDb:
        activeWikis.add(cleanDb)
        
    var f = open(outPath, fmWrite)
    for w in activeWikis:
      f.writeLine(w)
    f.close()
    
    if cfg.showProgress:
      stderr.writeLine("[Setup] Successfully created allwikis.txt with " & $activeWikis.len & " active wikis.")
  except CatchableError:
    let errMsg = getCurrentExceptionMsg()
    quit("Fatal Error: Could not generate allwikis.txt via HTTP. " & errMsg)

proc fetchWithRetries(client: HttpClient, baseUrl: string, cfg: WikiUrlConfig, maxRetries: int = 5): JsonNode =
  var attempt = 0
  var currentMaxlag = 5 # Start with the WMF recommended default of 5 seconds

  while attempt < maxRetries:
    # Dynamically append the ratcheting maxlag parameter to the URL
    let reqUrl = baseUrl & "&maxlag=" & $currentMaxlag
    
    if cfg.verbose:
      stderr.writeLine("[Verbose] GET " & reqUrl & " (Attempt " & $(attempt + 1) & "/" & $maxRetries & ")")

    try:
      let resp = client.request(reqUrl, HttpGet)
      
      # 1. Success Path
      if resp.code.is2xx:
        let jData = parseJson(resp.body)
        
        # Check for the deceptive WMF "Soft Maxlag" error inside a 200 OK payload
        if jData.hasKey("error") and jData["error"].hasKey("code") and jData["error"]["code"].getStr() == "maxlag":
          if cfg.showProgress:
            stderr.writeLine("[API] Maxlag hit. Server replication is lagging.")
          # Fall through to the backoff logic
        else:
          return jData # True success! Return the parsed JSON.

      # 2. Hard Rate Limit or Maxlag Path (429 Too Many Requests, 503 Service Unavailable)
      var waitTime = 5 # Default 5 second sleep
      
      # Always honor the Retry-After header if the WMF provides it
      let retryAfterStr = resp.headers.getOrDefault("Retry-After")
      if retryAfterStr != "":
        try:
          waitTime = parseInt(retryAfterStr)
        except ValueError:
          waitTime = 5

      if cfg.showProgress:
        stderr.writeLine("[API] Server returned " & $resp.code & ". Sleeping for " & $waitTime & " seconds...")
      
      sleep(waitTime * 1000) # Nim's sleep uses milliseconds

      # Ratchet up the maxlag for the next attempt so the server is more forgiving
      currentMaxlag += 5 
      attempt += 1

    # 3. Network Failure Path (DNS drop, physical disconnect)
    except CatchableError:
      let errMsg = getCurrentExceptionMsg()
      if cfg.showProgress:
        stderr.writeLine("[API] Network/Parse Error: " & errMsg & ". Retrying...")
      
      sleep(5000) # Default 5 second backoff for physical network drops
      attempt += 1

  # If we exhausted all 5 retries, gracefully abort instead of crashing
  quit("Fatal Error: WMF API completely exhausted after " & $maxRetries & " attempts. The servers are likely overloaded. Try again later.")

proc reverseDomain(urlStr: string): string =
  # Converts "https://www.archive.today/foo" to "today.archive.www."
  try:
    let parsed = parseUri(urlStr)
    var parts = parsed.hostname.split('.')
    parts.reverse()
    return parts.join(".") & "."
  except:
    return ""

proc stripSchemeFromIndex(elIndex: string): string =
  # Removes the "http://" or "https://" from the raw database tuple strings
  let schemeIdx = elIndex.find("://")
  if schemeIdx != -1:
    return elIndex[schemeIdx + 3 .. ^1]
  elif elIndex.startsWith("//"):
    return elIndex[2 .. ^1]
  return elIndex

proc unreverseDomain(elIndex: string): string =
  # Converts "http://com.cnn.arabic." cleanly back to "http://arabic.cnn.com"
  var scheme = ""
  var domainPart = elIndex
  
  let schemeIdx = elIndex.find("://")
  if schemeIdx != -1:
    scheme = elIndex[0 .. schemeIdx + 2]
    domainPart = elIndex[schemeIdx + 3 .. ^1]
  elif elIndex.startsWith("//"):
    scheme = "//"
    domainPart = elIndex[2 .. ^1]
  
  # Strip the trailing dot if it exists
  if domainPart.endsWith("."):
    domainPart = domainPart[0 .. ^2]
    
  var parts = domainPart.split('.')
  parts.reverse()
  return scheme & parts.join(".")

proc reverseDomainArg(domain: string): string =
  # Safely reverses a naked domain string from the CLI (e.g. "cnn.com" -> "com.cnn.")
  var parts = domain.split('.')
  parts.reverse()
  return parts.join(".") & "."

proc dbNameToHost(dbName: string): string =
  # Handles Toolforge DB names to API FQDN routing
  if dbName == "commonswiki": return "commons.wikimedia.org"
  if dbName == "wikidatawiki": return "www.wikidata.org"
  if dbName == "metawiki": return "meta.wikimedia.org"
  if dbName == "mediawikiwiki": return "www.mediawiki.org"
  if dbName == "specieswiki": return "species.wikimedia.org"

  # Standard language wikis
  if dbName.endsWith("wiki"): return dbName[0..^5] & ".wikipedia.org"
  if dbName.endsWith("wiktionary"): return dbName[0..^11] & ".wiktionary.org"
  if dbName.endsWith("wikibooks"): return dbName[0..^10] & ".wikibooks.org"
  if dbName.endsWith("wikiquote"): return dbName[0..^10] & ".wikiquote.org"
  if dbName.endsWith("wikisource"): return dbName[0..^11] & ".wikisource.org"
  if dbName.endsWith("wikinews"): return dbName[0..^9] & ".wikinews.org"
  if dbName.endsWith("wikiversity"): return dbName[0..^12] & ".wikiversity.org"
  if dbName.endsWith("wikivoyage"): return dbName[0..^11] & ".wikivoyage.org"
  
  return dbName # Fallback if it's already an FQDN or unknown

# -------------------------------------------------------------------------
# API Engine
# -------------------------------------------------------------------------

proc runApiEngine(cfg: WikiUrlConfig) =
  if cfg.showProgress:
    stderr.writeLine("[Engine] Starting API Engine...")
    
  if cfg.domain == "ALL":
    quit("Fatal Error: The API method cannot be used with '-d:ALL'. It would trigger millions of HTTP requests. Use -m:sql, -m:stream, or -m:download instead.")

  let agent = buildUserAgent(cfg)
  var client = newHttpClient(userAgent = agent)
  defer: client.close()

  # Process each requested wiki site
  for site in cfg.sites:
    if cfg.showProgress:
      stderr.writeLine("[API] Processing site: " & site)

    # Setup the output file routes
    let baseName = cfg.outDir / (cfg.domain & "." & site)
    var fJson, fTsv, fArt, fRaw: File
    
    if cfg.genJson: fJson = open(baseName & ".jsonl", fmWrite)
    if cfg.genTsv: fTsv = open(baseName & ".tsv", fmWrite)
    if cfg.genArticles: fArt = open(baseName & ".articles", fmWrite)
    if cfg.genRaw: fRaw = open(baseName & ".raw", fmWrite)

    # Ensure files are closed even if the loop crashes
    defer:
      if cfg.genJson: fJson.close()
      if cfg.genTsv: fTsv.close()
      if cfg.genArticles: fArt.close()
      if cfg.genRaw: fRaw.close()

    var eucontinue = ""
    # eulimit=max automatically uses 500 for regular users, 5000 for bot flags
    let fqdn = dbNameToHost(site)
    var apiUrl = "https://" & fqdn & "/w/api.php?action=query&list=exturlusage&euquery=" & cfg.domain & "&euprop=title|url|ids&format=json&eulimit=max"

    # API handles namespace filtering natively via parameter
    if cfg.nsList.len > 0:
      apiUrl &= "&eunamespace=" & cfg.nsList.join("|")

    while true:
      var reqUrl = apiUrl
      if eucontinue != "":
        reqUrl &= "&eucontinue=" & eucontinue

      if cfg.verbose: 
        stderr.writeLine("[Verbose] GET " & reqUrl)

      # Hardened HTTP fetch with retries and maxlag handling
      let jData = fetchWithRetries(client, reqUrl, cfg, 6) 

      # 1. Output the raw API response if requested
      if cfg.genRaw:
        fRaw.writeLine($jData)

      # 2. Parse and stream the structured data
      if jData.hasKey("query") and jData["query"].hasKey("exturlusage"):
        for item in jData["query"]["exturlusage"].getElems():
          let pageId = item["pageid"].getInt()
          let ns = item["ns"].getInt()
          let title = item["title"].getStr()
          let url = item["url"].getStr()
          
          # Regex Filter
          if cfg.regex != "":
            if url.find(cfg.compiledRegex) == -1:
              continue

          let revDomain = reverseDomain(url)

          # NDJSON Stream
          if cfg.genJson:
            let line = %*{
              "page_id": pageId,
              "namespace": ns,
              "title": title,
              "domain": revDomain,
              "url": url
            }
            fJson.writeLine($line)
          
          # Custom TSV Stream: <title> <tab> <ns> <tab> <revDomain> <tab> <url>
          if cfg.genTsv:
            fTsv.writeLine(title & "\t" & $ns & "\t" & revDomain & "\t" & url)

          if cfg.genArticles:
            fArt.writeLine(title)

      # 3. Handle Pagination
      if jData.hasKey("continue") and jData["continue"].hasKey("eucontinue"):
        eucontinue = jData["continue"]["eucontinue"].getStr()
      else:
        break # No more pages, exit the loop for this site


# -------------------------------------------------------------------------
# Dump Engine Components (Stream & Download)
# -------------------------------------------------------------------------

proc processSqlChunk(sqlChunk: string, cfg: WikiUrlConfig, site: string, fTsv, fJson, fRaw: File) =
  # Schema: (el_id, el_from, el_to_domain_index, el_to_path)
  
  let targetRevDomain = reverseDomainArg(cfg.domain)
  var currentPos = 0
  
  # Fast string scanning (no regex)
  while true:
    let startTuple = sqlChunk.find("(", currentPos)
    if startTuple == -1: break
    
    let endTuple = sqlChunk.find(")", startTuple)
    if endTuple == -1: break
    
    let tupleStr = sqlChunk[startTuple + 1 .. endTuple - 1]
    currentPos = endTuple + 1
    
    # If the chunk doesn't contain our domain (or we aren't pulling ALL), skip it instantly
    if cfg.domain != "ALL" and cfg.domain notin tupleStr and targetRevDomain notin tupleStr:
      continue

    # Split the tuple. We use maxsplit=3 so if the URL contains commas it won't break the array
    var parts = tupleStr.split(',', maxsplit = 3)
    if parts.len >= 4:
      try:
        let elId = parts[0].strip()
        let pageId = parts[1].strip() # This is the actual page ID (el_from)
        let revDomain = parts[2].strip().replace("'", "")
        let urlPath = parts[3].strip().replace("'", "")

        # Offline dumps lack namespace data entirely (it requires joining the `page` table).
        # We completely ignore cfg.nsList here to prevent breaking the dump engine.

        if cfg.domain == "ALL" or targetRevDomain in revDomain:
          let properDomain = unreverseDomain(revDomain)
          let cleanRevDomain = stripSchemeFromIndex(revDomain)
          let fullUrl = properDomain & urlPath

          # Regex Filter
          if cfg.regex != "":
            if fullUrl.find(cfg.compiledRegex) == -1:
              continue

          if cfg.genRaw:
            fRaw.writeLine(tupleStr)

          # Maintain 4 columns: <PageId (as Title)> <tab> <"-" (as Namespace)> <tab> <revDomain> <tab> <url>
          if cfg.genTsv:
            fTsv.writeLine(pageId & "\t-\t" & cleanRevDomain & "\t" & fullUrl)

          if cfg.genJson:
            # We use 0 or -1 as a null proxy for the missing namespace in JSON depending on preference, 
            # or just leave it out. We will use -1 here to indicate "unknown".
            let line = %*{
              "page_id": parseInt(pageId),
              "namespace": -1,
              "domain": cleanRevDomain,
              "url": fullUrl
            }
            fJson.writeLine($line)
            
      except ValueError:
        continue

proc runDumpDownloadEngine(cfg: WikiUrlConfig) =
  if cfg.showProgress:
    stderr.writeLine("[Engine] Starting Dump Download Engine (Disk Spooling)...")

  let agent = buildUserAgent(cfg)
  var client = newHttpClient(userAgent = agent)
  defer: client.close()

  for site in cfg.sites:
    let dumpUrl = "https://dumps.wikimedia.org/" & site & "/latest/" & site & "-latest-externallinks.sql.gz"
    let prefix = if cfg.domain == "ALL": "adn" else: cfg.domain
    let tempGzFile = cfg.outDir / (site & "_temp.sql.gz")
    let baseName = cfg.outDir / (prefix & "." & site & ".download")
    
    var fJson, fTsv, fRaw: File
    if cfg.genJson: fJson = open(baseName & ".jsonl", fmWrite)
    if cfg.genTsv: fTsv = open(baseName & ".tsv", fmWrite)
    if cfg.genRaw: fRaw = open(baseName & ".raw", fmWrite)

    defer:
      if cfg.genJson: fJson.close()
      if cfg.genTsv: fTsv.close()
      if cfg.genRaw: fRaw.close()

    try:
      if cfg.showProgress:
        stderr.writeLine("[Download] Fetching " & dumpUrl & " to " & tempGzFile)
      
      # Spool the compressed file to disk (For enwiki, this is ~1.5GB of disk space)
      client.downloadFile(dumpUrl, tempGzFile)
      
      if cfg.showProgress:
        stderr.writeLine("[Download] Complete. Opening stream buffer and parsing...")

      # Open the GZ stream. This reads directly from disk, unzipping only what it needs.
      var strm = newGzFileStream(tempGzFile)
      if strm == nil:
        quit("Fatal Error: Could not open gzip stream. Is the file corrupted?")
      
      var line: string
      # Read line by line. WMF dumps put each 1MB INSERT INTO block on its own line.
      while strm.readLine(line):
        if line.startsWith("INSERT INTO"):
          processSqlChunk(line, cfg, site, fTsv, fJson, fRaw)
          
      strm.close()

    except CatchableError:
      let errMsg = getCurrentExceptionMsg()
      stderr.writeLine("[Download] Error processing " & site & ": " & errMsg)
    
    finally:
      if fileExists(tempGzFile):
        removeFile(tempGzFile)
        if cfg.showProgress:
          stderr.writeLine("[Download] Cleaned up temporary spool file.")

proc runDumpStreamEngine(cfg: WikiUrlConfig) =
  if cfg.showProgress:
    stderr.writeLine("[Engine] Starting Dump Stream Engine (Pure Pipeline)...")

  for site in cfg.sites:
    let dumpUrl = "https://dumps.wikimedia.org/" & site & "/latest/" & site & "-latest-externallinks.sql.gz"
    let prefix = if cfg.domain == "ALL": "adn" else: cfg.domain
    let baseName = cfg.outDir / (prefix & "." & site & ".stream")
    
    var fJson, fTsv, fRaw: File
    if cfg.genJson: fJson = open(baseName & ".jsonl", fmWrite)
    if cfg.genTsv: fTsv = open(baseName & ".tsv", fmWrite)
    if cfg.genRaw: fRaw = open(baseName & ".raw", fmWrite)

    defer:
      if cfg.genJson: fJson.close()
      if cfg.genTsv: fTsv.close()
      if cfg.genRaw: fRaw.close()

    if cfg.showProgress:
      stderr.writeLine("[Stream] Opening live pipeline to: " & dumpUrl)

    # Use curl to fetch silently (-sL follows redirects), pipe to gzip for decompression.
    # We use Nim's osproc to capture the stdout stream in real-time.
    let cmd = "curl -sL " & dumpUrl & " | gzip -d"
    
    var p: Process
    try:
      # poEvalCommand tells Nim to use the system shell (sh/bash) to evaluate the pipe "|"
      p = startProcess(cmd, options = {poEvalCommand})
      var strm = p.outputStream()
      var line: string
      
      # Stream line-by-line directly from the shell pipeline into our Nim parser
      while strm.readLine(line):
        if line.startsWith("INSERT INTO"):
          processSqlChunk(line, cfg, site, fTsv, fJson, fRaw)
          
    except CatchableError:
      let errMsg = getCurrentExceptionMsg()
      stderr.writeLine("[Stream] Fatal Pipeline Error: " & errMsg)
    
    finally:
      if p != nil:
        p.close()

    if cfg.showProgress:
      stderr.writeLine("[Stream] Finished processing " & site)


# -------------------------------------------------------------------------
# SQL Engine
# -------------------------------------------------------------------------

proc getReplicaCreds(cfg: WikiUrlConfig): tuple[user, pass: string] =
  if not fileExists(cfg.replicaCnf):
    quit("Fatal Error: Could not find replica credentials at " & cfg.replicaCnf)
  
  var dict = loadConfig(cfg.replicaCnf)
  let user = dict.getSectionValue("client", "user")
  let pass = dict.getSectionValue("client", "password")
  
  if user == "" or pass == "":
    quit("Fatal Error: Could not parse user/password from " & cfg.replicaCnf)
    
  return (user, pass)

proc runSqlEngine(cfg: WikiUrlConfig) =
  if cfg.showProgress:
    stderr.writeLine("[Engine] Starting Toolforge SQL Engine (via SSH Tunnel)...")

  let creds = getReplicaCreds(cfg)
  let targetRevDomain = reverseDomainArg(cfg.domain)
  var currentLocalPort = 4711 # Increment per loop to avoid TIME_WAIT collisions

  for site in cfg.sites:
    if cfg.showProgress:
      stderr.writeLine("[SQL] Establishing SSH tunnel to Toolforge for " & site & "...")

    let dbHost = site & ".analytics.db.svc.wikimedia.cloud"
    let dbName = site & "_p"
    let localPort = $currentLocalPort
    currentLocalPort += 1

    when defined(windows):
      # Native Windows OpenSSH lacks ControlMaster multiplexing support
      # We launch it as a tracked background process instead
      var sshProc: Process
      try:
        sshProc = startProcess("ssh", args = ["-N", "-L", localPort & ":" & dbHost & ":3306", "login.toolforge.org"], options = {poUsePath})
        if cfg.showProgress:
          stderr.writeLine("[SQL] Waiting 3 seconds for Windows SSH tunnel to establish...")
        sleep(3000)
      except CatchableError:
        stderr.writeLine("[SQL] Failed to launch SSH process. Is OpenSSH installed?")
        continue
        
      defer:
        if cfg.showProgress: stderr.writeLine("[SQL] Closing SSH tunnel...")
        if sshProc != nil:
          sshProc.terminate()
          sshProc.close()
    else:
      # POSIX (Linux/macOS/WSL) uses reliable SSH multiplexing sockets
      let tunnelSock = getTempDir() / ("wikiurl_tunnel_" & site & ".sock")
      let sshStartCmd = "ssh -N -f -M -S " & tunnelSock & " -L " & localPort & ":" & dbHost & ":3306 login.toolforge.org"
      
      if execCmd(sshStartCmd) != 0:
        stderr.writeLine("[SQL] Failed to establish SSH tunnel. Ensure your SSH config/keys are set up for login.toolforge.org")
        continue

      defer:
        if cfg.showProgress: stderr.writeLine("[SQL] Closing SSH tunnel...")
        discard execCmd("ssh -S " & tunnelSock & " -O exit login.toolforge.org > /dev/null 2>&1")

    # Connect natively through the tunnel to the replica
    var db: DbConn
    try:
      # Nim's db_mysql allows host:port format in the connection string
      db = open("127.0.0.1:" & localPort, creds.user, creds.pass, dbName)
    except CatchableError:
      stderr.writeLine("[SQL] Failed to connect to local tunnel port " & localPort)
      continue
      
    defer: db.close()

    let prefix = if cfg.domain == "ALL": "adn" else: cfg.domain
    let baseName = cfg.outDir / (prefix & "." & site & ".sql")
    
    var fJson, fTsv: File
    
    if cfg.genJson: fJson = open(baseName & ".jsonl", fmWrite)
    if cfg.genTsv: fTsv = open(baseName & ".tsv", fmWrite)

    defer:
      if cfg.genJson: fJson.close()
      if cfg.genTsv: fTsv.close()

    if cfg.genRaw or cfg.genArticles:
      stderr.writeLine("[SQL] Note: --genRaw and --genArticles are ignored in SQL mode.")

    if cfg.showProgress:
      stderr.writeLine("[SQL] Executing indexed query for " & cfg.domain & "...")

    try:
      var nsFilter = ""
      if cfg.nsList.len > 0:
        nsFilter = " AND p.page_namespace IN (" & cfg.nsList.join(",") & ")"

      if cfg.domain == "ALL":
        # The URLS-ALL equivalent: Massive firehose dump with JOIN
        let queryStr = "SELECT p.page_namespace, p.page_title, el.el_to_domain_index, el.el_to_path FROM externallinks el JOIN page p ON p.page_id = el.el_from WHERE (el.el_to_domain_index LIKE ? OR el.el_to_domain_index LIKE ?)" & nsFilter
        for row in db.fastRows(sql(queryStr), "http://" & targetRevDomain & "%", "https://" & targetRevDomain & "%"):
          let ns = row[0]
          let title = row[1].replace("_", " ")
          let rawRevDomain = row[2]
          let properDomain = unreverseDomain(rawRevDomain)
          let cleanRevDomain = stripSchemeFromIndex(rawRevDomain)
          let fullUrl = properDomain & row[3]
          
          if cfg.regex != "":
            if fullUrl.find(cfg.compiledRegex) == -1:
              continue
              
          if cfg.genTsv:
            fTsv.writeLine(title & "\t" & ns & "\t" & cleanRevDomain & "\t" & fullUrl)
            
          if cfg.genJson:
            let line = %*{
              "namespace": parseInt(ns),
              "title": title,
              "domain": cleanRevDomain,
              "url": fullUrl
            }
            fJson.writeLine($line)
            
      else:
        # The specific domain search with JOIN
        let queryStr = "SELECT p.page_namespace, p.page_title, el.el_to_domain_index, el.el_to_path FROM externallinks el JOIN page p ON p.page_id = el.el_from WHERE (el.el_to_domain_index LIKE 'http://" & targetRevDomain & "%' OR el.el_to_domain_index LIKE 'https://" & targetRevDomain & "%')" & nsFilter
        for row in db.fastRows(sql(queryStr)):
          let ns = row[0]
          let title = row[1].replace("_", " ")
          let rawRevDomain = row[2]
          let properDomain = unreverseDomain(rawRevDomain)
          let cleanRevDomain = stripSchemeFromIndex(rawRevDomain)
          let fullUrl = properDomain & row[3]
          
          if cfg.regex != "":
            if fullUrl.find(cfg.compiledRegex) == -1:
              continue
              
          if cfg.genTsv:
            fTsv.writeLine(title & "\t" & ns & "\t" & cleanRevDomain & "\t" & fullUrl)
            
          if cfg.genJson:
            let line = %*{
              "namespace": parseInt(ns),
              "title": title,
              "domain": cleanRevDomain,
              "url": fullUrl
            }
            fJson.writeLine($line)

    except CatchableError:
      let errMsg = getCurrentExceptionMsg()
      stderr.writeLine("[SQL] Query failed: " & errMsg)

    if cfg.showProgress:
      stderr.writeLine("[SQL] Finished processing " & site)

# -------------------------------------------------------------------------
# Main Execution Routing
# -------------------------------------------------------------------------

proc executeWikiUrl(domain: string = "", site: string = "", methodOpt: string = "", 
                    ns: string = "", regex: string = "", config: string = "", 
                    workdir: string = "", allwikis: bool = false, progress: bool = false, 
                    verbose: bool = false, genTsv: bool = false, genJson: bool = false, 
                    genArticles: bool = false, genRaw: bool = false) =
                    
  var cfg = WikiUrlConfig(
    runMethod: methodAuto,
    outDir: getCurrentDir(),
    genRaw: false,
    genArticles: false,
    genTsv: false,
    genJson: false,
    showProgress: false,
    verbose: false,
    generateAllWikis: false
  )

  # Load Tier 1
  let dotFile = getHomeDir() / ".wikiurlrc"
  loadConfigFile(dotFile, cfg)

  # Load Tier 2
  if config != "":
    loadConfigFile(config, cfg)

  # Load Tier 3 (CLI Args overrides)
  if domain != "": cfg.domain = domain
  if workdir != "": cfg.outDir = workdir
  
  if regex != "": 
    cfg.regex = regex
    try:
      cfg.compiledRegex = re(regex)
    except CatchableError:
      quit("Error: Invalid regular expression provided: " & regex)

  if ns != "": 
    cfg.ns = ns
    for n in ns.split(','):
      let cleanN = n.strip()
      if cleanN != "": cfg.nsList.add(cleanN)

  if site != "":
    cfg.sites = @[]
    if site == "ALL":
      cfg.sites.add("ALL")
    elif fileExists(site):
      for line in lines(site):
        let cleanLine = line.strip()
        if cleanLine != "": cfg.sites.add(cleanLine)
    else:
      for s in site.split(','):
        let cleanS = s.strip()
        if cleanS != "": cfg.sites.add(cleanS)

  if methodOpt != "":
    case methodOpt.toLowerAscii()
    of "api": cfg.runMethod = methodApi
    of "stream": cfg.runMethod = methodDumpStream
    of "download": cfg.runMethod = methodDumpDownload
    of "sql": cfg.runMethod = methodSql
    else: quit("Error: Unknown method '" & methodOpt & "'")

  if allwikis: cfg.generateAllWikis = true
  if progress: cfg.showProgress = true
  if verbose: cfg.verbose = true
  if genRaw: cfg.genRaw = true
  if genArticles: cfg.genArticles = true
  if genTsv: cfg.genTsv = true
  if genJson: cfg.genJson = true

  # Resolve -s ALL late evaluation
  if cfg.sites.len == 1 and cfg.sites[0] == "ALL":
    let allWikisFile = cfg.outDir / "allwikis.txt"
    if fileExists(allWikisFile):
      cfg.sites = @[]
      for line in lines(allWikisFile):
        let cleanLine = line.strip()
        if cleanLine != "": cfg.sites.add(cleanLine)
    elif not cfg.generateAllWikis:
      quit("Error: -s ALL requires 'allwikis.txt' in the working directory. Run './wikiurl -a' first to generate it.")

  # If they just wanted to generate the list, exit cleanly without validation
  if cfg.generateAllWikis:
    generateAllWikisTxt(cfg)
    quit(0)

  validateConfig(cfg)

  # Auto-detect method if not explicitly set
  if cfg.runMethod == methodAuto:
    if cfg.domain == "ALL" or cfg.sites.len > 100:
      cfg.runMethod = methodDumpStream
    else:
      cfg.runMethod = methodApi

  if cfg.nsList.len > 0 and (cfg.runMethod == methodDumpStream or cfg.runMethod == methodDumpDownload):
    stderr.writeLine("[WARNING] The '-n' (namespace) filter is ignored when using the 'stream' or 'download' methods. Offline dumps lack namespace data.")

  if cfg.verbose:
    stderr.writeLine("[Verbose] Using Output Directory: " & cfg.outDir)
    stderr.writeLine("[Verbose] Execution Method: " & $cfg.runMethod)

  # Route to the appropriate engine
  case cfg.runMethod
  of methodApi: runApiEngine(cfg)
  of methodDumpStream: runDumpStreamEngine(cfg)
  of methodDumpDownload: runDumpDownloadEngine(cfg)
  of methodSql: runSqlEngine(cfg)
  of methodAuto: discard


when isMainModule:
  if paramCount() == 0:
    # If run with zero arguments, seamlessly trigger the cligen help menu
    discard execCmd(getAppFilename() & " --help")
    quit(0)

  # Customize cligen's help table to only show Flags and Descriptions
  clCfg.hTabCols = @[clOptKeys, clDescrip]

  dispatch(executeWikiUrl, 
    cmdName = "wikiurl", # <--- This forces the Usage line to say 'wikiurl'
    doc = "wikiurl - list page names and URLs that contain a domain\n\nExamples:\n  ./wikiurl -d cnn.com -s simplewiki -m stream --progress --genTsv\n  ./wikiurl -d ALL -s mysites.txt --progress --genTsv",
    help = {
      "domain": "Domain to search for eg. cnn.com (Use 'ALL' for all domains)",
      "site": "Site codes [comma separated] OR path to a text file list. Use 'ALL' to read from allwikis.txt (see -a)",
      "methodOpt": "Extraction method: api, download, stream, sql",
      "ns": "Namespace(s) to target [comma separated] eg. '0,6' (API/SQL methods only)",
      "regex": "Only report URLs that match the given regex",
      "config": "Path to a custom job config file to override ~/.wikiurlrc",
      "workdir": "Working directory for output. Default is CWD.",
      "allwikis": "Generate a fresh allwikis.txt file in the working directory (see -s ALL)",
      "progress": "Print status messages to stderr",
      "verbose": "Print detailed HTTP/network debug information",
      "genTsv": "Generate a .tsv file",
      "genJson": "Generate a .jsonl file",
      "genArticles": "Generate a .articles file (list of page titles, API/SQL methods only)",
      "genRaw": "Keep raw SQL/API output file"
    },
    short = {"domain": 'd', "site": 's', "methodOpt": 'm', "ns": 'n', "regex": 'r', "config": 'c', "workdir": 'w', "allwikis": 'a'}
  )
