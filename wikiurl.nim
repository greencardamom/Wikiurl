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

import std/[os, parseopt, parsecfg, strutils, httpclient, json, uri, algorithm, streams, osproc], zip/gzipfiles, db_connector/db_mysql

# For -m:sql 
# sudo apt-get install libmysqlclient-dev

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
    runMethod: OutputMethod
    showProgress: bool
    verbose: bool
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

proc printHelp() =
  echo "\n  wikiurl - list page names and URLs that contain a domain\n"
  echo "    -d <domain>   (required) Domain to search for eg. cnn.com (Use 'ALL' for all domains)"
  echo "    -s <site>     (required) Site codes [comma separated] OR path to a text file list (e.g. allwikis.txt)"
  echo "    -m <method>   (optional) Extraction method to use. Auto-detects if omitted. Options:"
  echo "                             api      - Live Web API (slow, good for small scrapes)"
  echo "                             download - Offline Dump with disk spooling (safe, low RAM)"
  echo "                             stream   - Offline Dump via Unix pipeline (fast, zero disk)"
  echo "                             sql      - Toolforge MariaDB replica (fastest, requires tunnel or grid)"
  echo "    -n <ns>       (optional) Namespace(s) to target [comma separated] eg. '0,6'. Default is all."
  echo "                             Note: Ignored by stream/download methods (offline dumps lack namespaces)"
  echo "    -r <regex>    (optional) Only report URLs that match the given regex"
  echo "    -c <config>   (optional) Path to a custom job config file to override ~/.wikiurlrc"
  echo "    -w <dir>      (optional) Working directory for output. Default is CWD."
  echo "    --progress    (optional) Print status messages to stderr"
  echo "    --verbose     (optional) Print detailed HTTP/network debug information"
  echo "    --genTsv:true      (optional) Generate a .tsv file"
  echo "    --genJson:true     (optional) Generate a .jsonl file"
  echo "    --genArticles:true (optional) Generate a .articles file (List of page titles, API/SQL only)"
  echo "    --genRaw:true      (optional) Keep raw SQL/API output file"
  echo ""
  echo "    Examples:"
  echo "      Run pure pipeline stream on your local machine:"
  echo "         ./wikiurl -d:cnn.com -s:simplewiki -m:stream --progress --genTsv:true"
  echo "      Run indexed query targeting specific namespaces and matching a regex:"
  echo "         ./wikiurl -d:cnn.com -s:simplewiki -m:sql -n:0,6 -r:\"^https://\" --progress --genTsv:true"
  echo "      Dump all links from sites listed in a file using auto-detected method:"
  echo "         ./wikiurl -d:ALL -s:mysites.txt --progress --genTsv:true"
  echo ""

# -------------------------------------------------------------------------
# CLI Parsing & Initialization
# -------------------------------------------------------------------------

proc initConfig(): WikiUrlConfig =
  # Intercept empty commands immediately
  if paramCount() == 0:
    printHelp()
    quit(0)

  # 1. Initialize with bare minimum defaults
  result = WikiUrlConfig(
    runMethod: methodAuto,
    outDir: getCurrentDir(),
    genRaw: false,
    genArticles: false,
    genTsv: false,
    genJson: false,
    showProgress: false,
    verbose: false
  )

  # 2. Load Tier 1: Global Dotfile (~/.wikiurlrc)
  let dotFile = getHomeDir() / ".wikiurlrc"
  loadConfigFile(dotFile, result)

  # Prepare to capture CLI args
  var customConfFile = ""
  var cliParams = initOptParser()

  # First pass to find if a custom job.conf was passed via -c
  for kind, key, val in cliParams.getopt():
    if kind == cmdLongOption or kind == cmdShortOption:
      if key == "c" or key == "config":
        customConfFile = val
        break
  
  # 3. Load Tier 2: Job Config (if provided)
  if customConfFile != "":
    loadConfigFile(customConfFile, result)

  # 4. Load Tier 3: CLI Flags (Overrides everything)
  cliParams = initOptParser() # Reset parser for full run
  for kind, key, val in cliParams.getopt():
    case kind
    of cmdLongOption, cmdShortOption:
      case key
      of "d", "domain": result.domain = val
      of "s", "site":
        result.sites = @[]
        if fileExists(val):
          for line in lines(val):
            let cleanLine = line.strip()
            if cleanLine != "": result.sites.add(cleanLine)
        else:
          for s in val.split(','):
            let cleanS = s.strip()
            if cleanS != "": result.sites.add(cleanS)
      of "m", "method":
        case val.toLowerAscii()
        of "api": result.runMethod = methodApi
        of "stream": result.runMethod = methodDumpStream
        of "download": result.runMethod = methodDumpDownload
        of "sql": result.runMethod = methodSql
        else: quit("Error: Unknown method '" & val & "'")
      of "w", "workdir": result.outDir = val
      of "progress": result.showProgress = true
      of "verbose": result.verbose = true
      of "c", "config": discard # Already handled
      of "genRaw": result.genRaw = parseBool(val)
      of "genArticles": result.genArticles = parseBool(val)
      of "genTsv": result.genTsv = parseBool(val)
      of "genJson": result.genJson = parseBool(val)
      else:
        quit("Error: Unknown option '--" & key & "'")
    of cmdArgument:
      discard
    of cmdEnd:
      break

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

proc buildUserAgent(cfg: WikiUrlConfig): string =
  # Safely read the email from the secrets file
  var email = "unknown@example.com"
  if fileExists(cfg.emailFile):
    email = readFile(cfg.emailFile).strip()
  else:
    quit("Error: Could not read email file at " & cfg.emailFile)
    
  # Format: wikiurl/1.0 (https://github.com/greencardamom/wikiurl; User:GreenC; mailto:email@example.com)
  return "wikiurl/1.0 (https://github.com/greencardamom/wikiurl; " & cfg.userId & "; mailto:" & email & ")"

proc reverseDomain(urlStr: string): string =
  # Converts "https://www.archive.today/foo" to "today.archive.www."
  try:
    let parsed = parseUri(urlStr)
    var parts = parsed.hostname.split('.')
    parts.reverse()
    return parts.join(".") & "."
  except:
    return ""

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
    let apiUrl = "https://" & fqdn & "/w/api.php?action=query&list=exturlusage&euquery=" & cfg.domain & "&euprop=title|url|ids&format=json&eulimit=max"

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
          
          # Custom TSV Stream (Tab separated) includes NS and Title
          if cfg.genTsv:
            fTsv.writeLine($ns & "\t" & title & "\t" & revDomain & "\t" & url)

          # Articles Stream (We can add namespace filtering from the config here later)
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
  # We are looking for strings like: (1234,0,'com.cnn.www.','http://www.cnn.com/')
  # Because schemas change, we isolate the tuples by their parentheses

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

    # Split the tuple. Expected: id, namespace, reversed_domain, path
    var parts = tupleStr.split(',')
    if parts.len >= 4:
      try:
        let pageId = parts[0].strip()
        let revDomain = parts[2].strip().replace("'", "")
        let url = parts[3].strip().replace("'", "")

        if cfg.domain == "ALL" or targetRevDomain in revDomain:
          if cfg.genRaw:
            fRaw.writeLine(tupleStr)

          # Offline dumps don't have titles, so we only print ID, Domain, and URL
          if cfg.genTsv:
            fTsv.writeLine(pageId & "\t" & revDomain & "\t" & url)

          if cfg.genJson:
            let line = %*{
              "page_id": parseInt(pageId),
              "domain": revDomain,
              "url": url
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

  for site in cfg.sites:
    if cfg.showProgress:
      stderr.writeLine("[SQL] Establishing SSH tunnel to Toolforge for " & site & "...")

    let dbHost = site & ".analytics.db.svc.wikimedia.cloud"
    let dbName = site & "_p"
    
    # Mirroring the findlinks.awk socket logic
    let tunnelSock = getTempDir() / ("wikiurl_tunnel_" & site & ".sock")
    let localPort = "4711"

    # 1. Start SSH Tunnel (-f puts it in background, -M creates control socket)
    let sshStartCmd = "ssh -N -f -M -S " & tunnelSock & " -L " & localPort & ":" & dbHost & ":3306 login.toolforge.org"
    
    if execCmd(sshStartCmd) != 0:
      stderr.writeLine("[SQL] Failed to establish SSH tunnel. Ensure your SSH config/keys are set up for login.toolforge.org")
      continue

    # 2. Guarantee the tunnel is killed when we leave this block, even if the database crashes
    defer:
      if cfg.showProgress: stderr.writeLine("[SQL] Closing SSH tunnel...")
      discard execCmd("ssh -S " & tunnelSock & " -O exit login.toolforge.org > /dev/null 2>&1")

    # 3. Connect natively through the tunnel to the replica
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
      if cfg.domain == "ALL":
        # The URLS-ALL equivalent: Massive firehose dump with JOIN
        let queryStr = "SELECT p.page_namespace, p.page_title, el.el_to_domain_index, el.el_to_path FROM externallinks el JOIN page p ON p.page_id = el.el_from WHERE el.el_to_domain_index LIKE ? OR el.el_to_domain_index LIKE ?"
        for row in db.fastRows(sql(queryStr), "http://" & targetRevDomain & "%", "https://" & targetRevDomain & "%"):
          let ns = row[0]
          let title = row[1]
          let properDomain = unreverseDomain(row[2])
          let fullUrl = properDomain & row[3]
          
          if cfg.genTsv:
            fTsv.writeLine(ns & "\t" & title & "\t" & properDomain & "\t" & fullUrl)
            
          if cfg.genJson:
            let line = %*{
              "namespace": parseInt(ns),
              "title": title,
              "domain": properDomain,
              "url": fullUrl
            }
            fJson.writeLine($line)
            
      else:
        # The specific domain search with JOIN
        let queryStr = "SELECT p.page_namespace, p.page_title, el.el_to_domain_index, el.el_to_path FROM externallinks el JOIN page p ON p.page_id = el.el_from WHERE el.el_to_domain_index LIKE 'http://" & targetRevDomain & "%' OR el.el_to_domain_index LIKE 'https://" & targetRevDomain & "%'"
        for row in db.fastRows(sql(queryStr)):
          let ns = row[0]
          let title = row[1]
          let properDomain = unreverseDomain(row[2])
          let fullUrl = properDomain & row[3]
          
          if cfg.genTsv:
            fTsv.writeLine(ns & "\t" & title & "\t" & properDomain & "\t" & fullUrl)
            
          if cfg.genJson:
            let line = %*{
              "namespace": parseInt(ns),
              "title": title,
              "domain": properDomain,
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

proc main() =
  var config = initConfig()
  validateConfig(config)

  # Auto-detect method if not explicitly set
  if config.runMethod == methodAuto:
    if "ALL" in config.sites:
      config.runMethod = methodDumpStream
    else:
      config.runMethod = methodApi

  if config.verbose:
    stderr.writeLine("[Verbose] Using Output Directory: " & config.outDir)
    stderr.writeLine("[Verbose] Execution Method: " & $config.runMethod)

  # Route to the appropriate engine
  case config.runMethod
  of methodApi:
    runApiEngine(config)
  of methodDumpStream:
    runDumpStreamEngine(config)
  of methodDumpDownload:
    runDumpDownloadEngine(config)
  of methodSql:
    runSqlEngine(config)
  of methodAuto:
    discard # Handled above

when isMainModule:
  main()
