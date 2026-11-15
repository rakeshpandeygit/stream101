# PySpark Structured Streaming Practice

A small learning repository for exploring Apache Spark Structured Streaming with JSON files and a TCP socket. The examples progress from a simple file stream to aggregations and event-time windows, with a batch version included for comparison.

> These scripts are hands-on learning examples. The socket source and console sink are for local experimentation, not production ingestion or durable output.

## What you’ll learn

- Read newline-delimited JSON files as a stream.
- Filter streaming rows and write results to the console.
- Compare a batch aggregation with a streaming aggregation.
- Explode nested order line items and aggregate by customer.
- Read JSON text from a TCP socket, parse it into a typed structure, and aggregate by event-time window.
- Use a checkpoint directory to preserve streaming query progress and state.

## Repository map

| File | Purpose |
|---|---|
| [stream101.py](stream101.py) | Reads order JSON files from `data/streamJson`, filters for `order_status = 'COMPLETE'`, and prints new rows to the console. |
| [stream102-batch.py](stream102-batch.py) | Batch comparison: reads the JSON files once, explodes line items, aggregates by customer, and writes CSV output to `data/stream_output`. |
| [stream102-stream-contin-agg.py](stream102-stream-contin-agg.py) | Streams JSON files, explodes line items, and continuously updates customer-level aggregates in the console. |
| [stream103-stream-timebound-agg.py](stream103-stream-timebound-agg.py) | Reads JSON messages from a TCP socket on port `9986`, parses the JSON, and aggregates invoice amounts into 15-minute event-time windows. |
| [lib/utils.py](lib/utils.py) | Creates the local Spark session and configures the Windows local filesystem/checkpoint support used by this setup. |
| `data/streamJson/` | Sample newline-delimited order JSON files for the file-stream examples. |
| `data/order.json` | Sample nested order document used by the separate parsing and batch exercises. |
| `data/customer.csv` | Sample customer data used by the deduplication exercise. |

The repository also contains small Python exercises (`parser.py`, `pysparkInPython.py`, `pysparkDedup.py`, `countChar.py`, and `oops.py`) that are not streaming query entry points.

## Requirements

- Python compatible with the PySpark version pinned in `requirements.txt`.
- Java installed and available to Spark through `JAVA_HOME` / `PATH`.
- The Windows local filesystem helper JAR expected by `lib/utils.py`:
  `%LOCALAPPDATA%\Programs\hadoop-bare-naked-local-fs\hadoop-bare-naked-local-fs-0.1.0.jar`.

The current `lib/utils.py` is configured for this Windows setup and expects that JAR at the path above. If you use another operating system or do not have that JAR, adjust the local Spark-session configuration in `lib/utils.py` for your environment.

## Set up the Python environment

From the repository root in PowerShell:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

If PowerShell blocks virtual-environment activation, allow scripts for the current terminal session and activate again:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

## Run the file-stream examples

Start a script from the repository root so relative paths such as `data/streamJson` resolve correctly:

```powershell
python .\stream101.py
```

Spark watches `data/streamJson` for JSON files and prints completed orders. To see new input arrive while the query is running, copy a new newline-delimited JSON file into that directory. Each line in an input file must contain one JSON record.

For the customer aggregation:

```powershell
python .\stream102-stream-contin-agg.py
```

For the batch comparison:

```powershell
python .\stream102-batch.py
```

The batch script overwrites its output under `data/stream_output`. The streaming aggregation uses the console sink, so its displayed results are for inspection and are not saved as a durable dataset.

## Run the socket event-time example

The socket example reads from **port 9986** (as set in `stream103-stream-timebound-agg.py`). Start the producer first, then start Spark.

### Option A: Ncat on Windows

Install Ncat (included with Nmap) and open a new PowerShell terminal. Start a listener:

```powershell
ncat -lk 9986
```

Leave that terminal open. In a second PowerShell terminal, activate the virtual environment, go to the repository root, and run:

```powershell
python .\stream103-stream-timebound-agg.py
```

Return to the Ncat terminal and enter one JSON object per line, pressing Enter after each event:

```json
{"order_id":57012,"order_date":"2026-11-14 19:05:00","order_customer_id":2765,"order_status":"PROCESSING","amount":200.0}
```

Use a timestamp within the recent 15-minute interval to see the event appear in the current window. The sample script does not set a watermark; window state is therefore not automatically removed based on event-time progress.

Stop Spark with `Ctrl+C`, then stop Ncat with `Ctrl+C`.

### Option B: Test whether the port is reachable

With the Spark query running and the listener available, you can check the port in another PowerShell window:

```powershell
Test-NetConnection localhost -Port 9986
```

A successful test shows `TcpTestSucceeded : True`. The terminal command `nc -lk 9988` from some Unix/macOS demonstrations will not work in PowerShell unless Netcat is installed, and this repository's socket reader is configured for port `9986`, not `9988`.

## Checkpoints and reruns

Structured Streaming stores progress and state in its checkpoint directory. The current examples use `data/checkpoint` (the local helper in `lib/utils.py` configures Spark's file-based checkpoint manager for Windows).

- Stop the running query before changing or removing its checkpoint.
- For a fresh learning run, remove the relevant checkpoint directory after stopping the query.
- Do not point different queries at the same checkpoint directory in a production design. Give each query its own stable checkpoint path.

## Concepts in the examples

### File stream

Spark's file source discovers files placed in the input directory and processes them as streaming input. A streaming DataFrame is lazy: transformations describe the work, and `writeStream.start()` starts the query.

### Batch versus streaming aggregation

`stream102-batch.py` computes an aggregate over the files present at the time it runs. `stream102-stream-contin-agg.py` keeps a streaming aggregation updated as more files arrive. The streaming example uses approximate distinct order counts.

### Socket JSON and event time

The socket source provides each incoming line in a `value` column. `from_json` parses that JSON string using the declared schema. The example groups parsed records by a 15-minute window on `order_date` and emits updated results to the console.

This TCP socket is a simple learning input. It does not provide the durable storage, replay, delivery guarantees, or operational controls expected from a production event broker.

## Troubleshooting

| Symptom | Check |
|---|---|
| `nc is not recognized` in PowerShell | Netcat is not installed. Use Ncat (Option A) or install a suitable socket utility. |
| Spark reports connection refused | Start the socket listener before the Spark query; confirm both use port `9986`. |
| No socket rows appear | Send one complete JSON object per line, press Enter, and ensure the timestamp matches the schema and a recent window. |
| Local filesystem or checkpoint error on Windows | Confirm the helper JAR exists at the path expected by `lib/utils.py`; run the script from the repository root. |
| File stream appears idle | Add a new JSON file to `data/streamJson` while the query is running and check the console output. |
| Query fails after a code or schema change | Stop the query and use a fresh checkpoint directory for the learning run. |

## Learning sequence

1. Run `stream101.py` and observe filtering and the console sink.
2. Run `stream102-batch.py` to understand the nested line-item transformation and aggregation.
3. Run `stream102-stream-contin-agg.py` and add new JSON files while it is active.
4. Run `stream103-stream-timebound-agg.py` with Ncat to follow a JSON message through parsing into an event-time window.
5. Compare the batch and streaming behaviors, output modes, checkpoints, and input mechanisms.

## License

No license is currently specified for this repository.
