# wikiurl

`wikiurl` is a high-performance, multi-engine command-line tool for extracting external links from Wikimedia projects. It allows you to search for specific domains across various wikis and output the results in multiple formats (TSV, JSONL, raw SQL, or article title list).

## Example 

Extract all CNN URLs from the Simple English Wikipedia (`simplewiki`) using the live API engine and output in TSV format:

```bash
./wikiurl -d:cnn.com -s:simplewiki -m:api --genTsv:true
```

**Output (`cnn.com.simplewiki.tsv`):**
```tsv
0       The Blues Brothers      com.cnn.www.                    [http://www.cnn.com/2005/SHOWBIZ/Movies/08/30/film.bluesbrothers.ap/index.html](http://www.cnn.com/2005/SHOWBIZ/Movies/08/30/film.bluesbrothers.ap/index.html)
0       America's Army          com.cnn.www.                    [http://www.cnn.com/US/9909/30/army.recruitment/#1](http://www.cnn.com/US/9909/30/army.recruitment/#1)
0       Kryptos                 com.cnn.www.                    [http://www.cnn.com/2005/US/06/19/cracking.the.code/index.html](http://www.cnn.com/2005/US/06/19/cracking.the.code/index.html)
0       Sartell, Minnesota      com.cnn.sportsillustrated.      [http://sportsillustrated.cnn.com/football/nfl/players/3751/](http://sportsillustrated.cnn.com/football/nfl/players/3751/)
```

## Features

* **Multi-Engine Extraction:** Choose the best method for the job:
  * **API:** Live web API querying (ideal for small scrapes).
  * **Stream:** Unix pipeline decompression of offline dumps (fast, near-zero disk footprint).
  * **Download:** Disk-spooling of offline dumps (safe, low RAM).
  * **SQL:** Toolforge MariaDB replica querying via SSH tunnel (fastest, requires Toolforge access).
* **Multiple Output Formats:** Generate `.tsv`, `.jsonl`, `.articles` (lists of titles), or raw `.raw` dumps.
* **Resilient:** Automatically handles Wikimedia API rate limits and server lag spikes.
* **Flexible Filtering:** Target specific namespaces or apply regex filtering.

## Installation

### Pre-compiled Binaries (Recommended)

If you do not want to install Nim and compile the tool from source, you can download pre-compiled binaries for Linux, macOS, and Windows.

1. Navigate to the **[Releases](https://github.com/greencardamom/wikiurl/releases)** page on this repository.
2. Under the latest release tag (e.g., `v1.0.0`), expand the **Assets** section at the bottom.
3. Download the binary that matches your operating system:
   * `wikiurl-linux-amd64` (Linux)
   * `wikiurl-macos-amd64` (macOS)
   * `wikiurl-windows-amd64.exe` (Windows)

**Security Note:** To ensure transparency and trust, all pre-compiled binaries are built and attached to releases 
automatically by GitHub Actions directly from the public source code. No binaries are compiled on or uploaded from personal 
machines. You can verify the build environment and inspect the workflow logs at any time by clicking the **Actions** tab at 
the top of this repository. Because this tool is fully open source, you are highly encouraged to independently review how it 
works. If you would like a second opinion on its safety or a plain-English breakdown of its logic, feel free to copy the 
source code into an AI assistant (like Claude, ChatGPT, or Gemini) and ask for a review.

### Prerequisites

1. **Nim:** To compile from source you will need the Nim compiler installed. 
2. **MySQL Client Library:** Required for the `-m:sql` engine. On Debian/Ubuntu systems:
   ```bash
   sudo apt-get install libmysqlclient-dev
   ```
*Note: If running Nim version 2.0 or greater also run `nimble install db_connector`*

### Building from Source

Clone the repository and compile with the release flag for maximum performance:

```bash
git clone [https://github.com/greencardamom/wikiurl.git](https://github.com/greencardamom/wikiurl.git)
cd wikiurl
nim c -d:release wikiurl.nim
```

## Configuration

To comply with Wikimedia's User-Agent policies, `wikiurl` requires a configuration file to identify your bot/script. 

Create a file at `~/.wikiurlrc` with the following minimum configuration:

```ini
[Identity]
userid = "User:YourWikiUsername"
email_file = "/path/to/a/text/file/containing/your/email.txt"

[Defaults]
# Optional: Set a default working directory
# output_dir = "/tmp/wikiurl_output"
```

*Note: The `email_file` should be a plain text file containing only your email address. This prevents hardcoding your email directly into configuration files that might be accidentally committed.*

### Toolforge SQL Configuration (Optional)
If you intend to use the `-m:sql` method, add your replica credentials to the config. These are available in your Toolforge account. You will also need passwordless-ssh setup on Toolforge.

```ini
[Authentication]
replica_cnf = "/path/to/your/replica.my.cnf"
```

## Usage

```text
wikiurl - list page names and URLs that contain a domain

  -d <domain>   (required) Domain to search for eg. cnn.com (Use 'ALL' for all domains)
  -s <site>     (required) Site codes [comma separated] OR path to a text file list (e.g. allwikis.txt)
  -m <method>   (optional) Extraction method to use. Auto-detects if omitted. Options:
                           api      - Live Web API (slow, good for small scrapes)
                           download - Offline Dump with disk spooling (safe, low RAM)
                           stream   - Offline Dump via Unix pipeline (fast, zero disk)
                           sql      - Toolforge MariaDB replica (fastest, requires tunnel or grid)
  -n <ns>       (optional) Namespace(s) to target [comma separated] eg. '0,6'. Default is all.
                           Note: Ignored by stream/download methods (offline dumps lack namespaces)
  -r <regex>    (optional) Only report URLs that match the given regex
  -c <config>   (optional) Path to a custom job config file to override ~/.wikiurlrc
  -w <dir>      (optional) Working directory for output. Default is CWD.
  --progress    (optional) Print status messages to stderr
  --verbose     (optional) Print detailed HTTP/network debug information
  --genTsv:true      (optional) Generate a .tsv file
  --genJson:true     (optional) Generate a .jsonl file
  --genArticles:true (optional) Generate a .articles file (List of page titles, API/SQL only)
  --genRaw:true      (optional) Keep raw SQL/API output file
```

## Extraction Engines Comparison

| Engine | Speed | Freshness | Access | Page & Namespace View | Pros | Cons | Dependencies |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **API** (`api`) | Medium | Hours old | Open | Yes | Easy to use anywhere; perfect for small-to-medium scrapes; no disk space required. | Slow for massive wikis (e.g., enwiki); subject to server `maxlag` and rate limits. | |
| **Stream** (`stream`) | High | Up to 30 days | Open | No | Extremely fast; near-zero disk footprint; completely bypasses API rate limits. | Data can be weeks out of date; offline dumps lack page titles and namespaces. | curl and gzip |
| **Download** (`download`) | Medium | Up to 30 days | Open | No | Resilient against network drops; great if you need to run multiple passes over the same dump. | Requires heavy disk space (GBs for large wikis); data is delayed; no titles/namespaces. | curl and gzip |
| **SQL** (`sql`) | High | Hours old | Authentication | Yes | The absolute fastest method; provides full metadata (titles/namespaces) with real-time replica data. | Requires approved Wikimedia Toolforge access and SSH key configuration. | ssh |

**Note for Windows Users**: While the API method (`-m:api`) works natively on Windows, the Stream, Download, and SQL engines rely on UNIX pipelines (`gzip`) and SSH multiplexing sockets (ControlMaster). It is highly recommended to run `wikiurl` via WSL (Windows Subsystem for Linux) to utilize all features.

### Examples

**Run a pure pipeline stream on your local machine:**
```bash
./wikiurl -d:cnn.com -s:simplewiki -m:stream --progress --genTsv:true
```

**Run an indexed query targeting specific namespaces and matching a regex (Toolforge):**
```bash
./wikiurl -d:cnn.com -s:simplewiki -m:sql -n:0,6 -r:"^https://" --progress --genTsv:true
```

**Dump all links from sites listed in a file using the auto-detected method:**
```bash
./wikiurl -d:ALL -s:mysites.txt --progress --genTsv:true
```
