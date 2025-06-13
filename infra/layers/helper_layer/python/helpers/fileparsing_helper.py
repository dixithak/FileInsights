
import pyarrow.parquet as pq
from io import BytesIO
import zipfile
import gzip


__all__ = ["read_file_header", "detect_delimiter", "parse_header_line"]

def detect_delimiter(line):
    for delim in [',', '\t', ';', '|']:
        if delim in line:
            return delim
    return ','

def parse_header_line(raw_line):
    try:
        line = raw_line.decode('utf-8').strip()
    except UnicodeDecodeError:
        return ["Decode error"]
    delimiter = detect_delimiter(line)
    return [col.strip().strip('"') for col in line.split(delimiter)]

def read_file_header(file_data, key):
    try:
        # Handle Parquet files
        if key.endswith('.parquet') or key.endswith('.pq'):
            table = pq.read_table(BytesIO(file_data))
            return table.schema.names

        # Handle ZIP files
        elif key.endswith('.zip'):
            with zipfile.ZipFile(BytesIO(file_data)) as zipf:
                names = [f for f in zipf.namelist() if f.endswith('.csv') or f.endswith('.txt')]
                if not names:
                    return ["No CSV or TXT file in ZIP"]
                with zipf.open(names[0]) as csvfile:
                    return parse_header_line(csvfile.readline())

        # Handle GZ, CSV, TXT
        elif key.endswith('.gz') or key.endswith('.csv') or key.endswith('.txt') or key.endswith('.psv'):
            file_like = gzip.GzipFile(fileobj=BytesIO(file_data)) if key.endswith('.gz') else BytesIO(file_data)
            with file_like as f:
                return parse_header_line(f.readline())

        else:
            return ["Unsupported file format"]

    except Exception as e:
        print(f"Header parsing error for {key}: {e}")
        return [f"Header parsing failed: {str(e)}"]

