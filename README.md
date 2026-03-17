# wikiurl

`wikiurl` is a high-performance, multi-engine command-line tool for extracting URLs from Wikimedia projects. It allows you to search on specific domains across various wikis and output the results in multiple formats (TSV, JSONL, raw SQL, or article title list). At maximum, you could download all URLS across all 800+ wikis.

The tool is particularly useful for tool, bot and report makers. It has advantages over [Quarry](https://toolhub.wikimedia.org/tools/wikimedia-quarry) by offering multiple extraction engines while avoiding SQL programming, web UI row limits, and query timeouts. It can be easily automated in shell scripts, seamlessly iterates across hundreds of wikis in a single command. A binary executable is available for **Linux**, **Mac** and **Windows**.

## Example 

Extract all CNN URLs from the Simple English Wikipedia (`simplewiki`) using the live API engine and output in TSV format:

```bash
./wikiurl -d cnn.com -s simplewiki -m api --genTsv
```

**Output (`cnn.com.simplewiki.tsv`):**
```tsv
The Blues Brothers    123  0   com.cnn.www.                    http://www.cnn.com/2005/SHOWBIZ/Movies/08/30/film.bluesbrothers.ap/index.html
America's Army        124  0   com.cnn.www.                    http://www.cnn.com/US/9909/30/army.recruitment/#1
Kryptos               564  0   com.cnn.www.                    http://www.cnn.com/2005/US/06/19/cracking.the.code/index.html
Sartell, Minnesota    987  0   com.cnn.sportsillustrated.      http://sportsillustrated.cnn.com/football/nfl/players/3751/
```

*Columns (tab-separated): Article Title | Page ID | Namespace | Sortable Domain | URL*

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

We offer two versions for each operating system:
* **Standalone (Default):** Runs instantly out of the box. Supports the API, Stream, and Download engines.
* **SQL Version (`-sql`):** Required *only* if you intend to use the `-m sql` engine on Toolforge. **Note:** This version requires you to have MySQL/MariaDB client libraries installed on your system.

1. Navigate to the **[Releases](https://github.com/greencardamom/wikiurl/releases)** page on this repository.
2. Under the latest release tag, expand the **Assets** section at the bottom.
3. Download the binary that matches your operating system and needs:
   * **Linux:** `wikiurl-linux-amd64` (or `-sql`)
   * **macOS (M-Series/ARM):** `wikiurl-macos-arm64` (or `-sql`)
   * **Windows:** `wikiurl-windows-amd64.exe` (or `-sql.exe`)

**Security Note:** To ensure transparency and trust, all pre-compiled binaries are built automatically with a GitHub feature called Workflow. *No binaries are uploaded from personal machines.* You can verify GitHub's compile log by clicking the **Actions** tab at the top of this repository.

### Prerequisites

1. **Nim:** To compile from source you will need the [Nim compiler](https://nim-lang.org/install.html) installed.
2. **Nim Libraries:** Install the required external packages using Nim's package manager:
   ```bash
   nimble install cligen zip
   ```
   *Note: If you are running Nim version 2.0 or greater, you must also install the database connector: `nimble install db_connector`*

3. **MySQL Client Library:** Required only if you intend to compile and use the `-m sql` engine. 
   * **Linux (Debian/Ubuntu):**
     ```bash
     sudo apt-get install libmysqlclient-dev
     ```
   * **macOS (via Homebrew):**
     ```bash
     brew install mysql-client
     ```
   * **Windows:** Nim requires `libmysql.dll` or `libmariadb.dll` in your system `PATH` (or placed next to the `wikiurl.exe` file). You can obtain this by downloading the [MariaDB C Connector](https://mariadb.com/downloads/connectors/connectors-data-access/c-connector/) or via MSYS2 (`pacman -S mingw-w64-x86_64-libmariadbclient`).

### Building from Source

Clone the repository and compile with the release flag for maximum performance:

```bash
git clone [https://github.com/greencardamom/wikiurl.git](https://github.com/greencardamom/wikiurl.git)
cd wikiurl
nim c -d:ssl -d:release --outdir:. src/wikiurl.nim
```

## Configuration

To comply with Wikimedia's User-Agent policies, `wikiurl` requires a configuration file to identify your bot/script. 

Create a file in your home directory (`~/.wikiurlrc` on Linux/macOS, or `C:\Users\<YourUsername>\.wikiurlrc` on Windows) with the following minimum configuration:

```ini
[Identity]
userid = "User:WikiUser"
email_file = "/path/to/a/text/file/containing/your/email.txt"

[Defaults]
# Optional: Set a default working directory
# output_dir = "/tmp/wikiurl_output"
```

*Note: The `email_file` should be a plain text file containing only your email address. This prevents hardcoding your email directly into configuration files. All values are surrounded by double-quote.*

**Windows Path Note:** Because the values are double-quoted, you must either use forward slashes or double backslashes for your file paths so they are parsed correctly. 
* Good: `"C:/Users/Name/email.txt"` or `"C:\\Users\\Name\\email.txt"`
* Bad: `"C:\Users\Name\email.txt"`

### Toolforge SQL Configuration (Optional)
If you intend to use the `-m sql` method, add your replica credentials to the config. The file `replica.my.cnf` is available in your Toolforge shell account. You will also need passwordless-ssh setup on Toolforge.

```ini
[Authentication]
replica_cnf = "/path/to/your/replica.my.cnf"
```

## Usage

```text
Usage:
  wikiurl [optional-params] 
wikiurl - list page names and URLs that contain a domain

Examples:
  ./wikiurl -d cnn.com -s simplewiki -m stream --progress --genTsv
  ./wikiurl -d ALL -s mysites.txt --progress --genTsv
Options:
  -h, --help         print this cligen-erated help
  -d=, --domain=     Domain to search for eg. cnn.com (Use 'ALL' for all domains)
  -s=, --site=       Site codes [comma separated] OR path to a text file list. Use 'ALL' to read from
                     allwikis.txt (see -a)
  -m=, --methodOpt=  Extraction method: api, download, stream, sql
  -n=, --ns=         Namespace(s) to target [comma separated] eg. '0,6' (API/SQL methods only)
  -r=, --regex=      Only report URLs that match the given regex
  -c=, --config=     Path to a custom job config file to override ~/.wikiurlrc
  -w=, --workdir=    Working directory for output. Default is CWD.
  -a, --allwikis     Generate a fresh allwikis.txt file in the working directory (see -s ALL)
  -p, --progress     Print status messages to stderr
  -v, --verbose      Print detailed HTTP/network debug information
  -g, --genTsv       Generate a .tsv file
  --genJson          Generate a .jsonl file
  --genArticles      Generate a .articles file (list of page titles, API/SQL methods only)
  --genRaw           Keep raw SQL/API output file
```

## Extraction Engines Comparison

| Engine | Speed | Freshness | Access | Page & Namespace View | Pros | Cons | Dependencies |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **API** (`api`) | Medium | Hours old | Open | Yes | Easy to use anywhere; perfect for small-to-medium scrapes; no disk space required. | Slow for massive wikis (e.g., enwiki); subject to server `maxlag` and rate limits. | |
| **Stream** (`stream`) | High | Up to 30 days | Open | No | Extremely fast; near-zero disk footprint; completely bypasses API rate limits. | Data can be weeks out of date; offline dumps lack page titles and namespaces. | curl and gzip |
| **Download** (`download`) | Medium | Up to 30 days | Open | No | Resilient against network drops; great if you need to run multiple passes over the same dump. | Requires heavy disk space (GBs for large wikis); data is delayed; no titles/namespaces. | curl and gzip |
| **SQL** (`sql`) | High | Hours old | Authentication | Yes | The absolute fastest method; provides full metadata (titles/namespaces) with real-time replica data. | Requires approved Wikimedia Toolforge access and SSH key configuration. | ssh |

**Note for Windows Users**: The API (`-m api`), Download (`-m download`), and SQL (`-m sql`) engines work natively on Windows (the SQL engine utilizes standard Windows 10/11 OpenSSH). However, the Stream engine (`-m stream`) relies on the UNIX `gzip` utility via shell pipelines. To use the Stream method on Windows, you must either have `gzip` installed in your system PATH (e.g., via Git Bash) or run `wikiurl` via WSL (Windows Subsystem for Linux).

### Examples

**Run a pure pipeline stream on your local machine:**
```bash
./wikiurl -d cnn.com -s simplewiki -m stream --progress --genTsv
```

**Run an indexed query targeting specific namespaces and matching a regex (Toolforge):**
```bash
./wikiurl -d cnn.com -s simplewiki -m sql -n 0,6 -r "^https://" --progress --genTsv
```

**Dump all links from sites listed in a file using the auto-detected method:**
```bash
./wikiurl -d ALL -s mysites.txt --progress --genTsv
```

## Testing

`wikiurl` includes a comprehensive test suite to validate the integrity of all four extraction engines and their output formats (TSV, JSONL, RAW, Articles). It runs parallel extractions across different domains and outputs line counts to ensure data consistency across the engines.

To execute the test suite:

```bash
cd testsuite
./testsuite.sh
```
*(Note: Depending on your network connection and the size of the wikis, the `download` and `stream` engines may take a minute or two to complete).*

## macOS Security Warning

Because this is an open-source tool not signed with a paid Apple Developer certificate, macOS will flag the binary when you first download it and prevent it from running. 

To clear the Apple quarantine flag and allow the tool to run, open your terminal and run this command on the downloaded file:

`xattr -d com.apple.quarantine wikiurl-macos-amd64`

If it still does not work try this:

`codesign -s - --force ./wikiurl-macos-amd64`
