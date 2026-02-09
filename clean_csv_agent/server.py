import csv
import io
import os
import shutil
import tempfile
from contextlib import asynccontextmanager

import duckdb
import uvicorn
from fastapi import FastAPI, UploadFile, Query
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from google.adk.cli.fast_api import get_fast_api_app


UPLOAD_DIR = os.path.join(tempfile.gettempdir(), "datagrunt_uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)


def _count_lines_fast(filepath: str) -> int:
    """Count newlines using buffered binary reads — orders of magnitude
    faster than Python's line iterator for large files."""
    count = 0
    with open(filepath, "rb") as f:
        while True:
            buf = f.read(1024 * 1024)  # 1 MB chunks
            if not buf:
                break
            count += buf.count(b"\n")
    return max(count - 1, 0)  # subtract header


async def upload_csv(file: UploadFile):
    dest = os.path.join(UPLOAD_DIR, file.filename)
    with open(dest, "wb") as f:
        shutil.copyfileobj(file.file, f)

    row_count = _count_lines_fast(dest)

    return {"file_path": dest, "filename": file.filename, "row_count": row_count}


async def download_csv(
    file_path: str = Query(...),
    preview: int = Query(None, ge=1),
    download: bool = Query(False),
):
    abs_path = os.path.abspath(file_path)
    if not os.path.isfile(abs_path):
        return {"error": f"File not found: {file_path}"}

    filename = os.path.basename(abs_path)

    if preview:
        with open(abs_path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f)
            buf = io.StringIO()
            writer = csv.writer(buf)
            for i, row in enumerate(reader):
                if i > preview:  # header + N data rows
                    break
                writer.writerow(row)
        buf.seek(0)
        return StreamingResponse(
            buf,
            media_type="text/csv",
            headers={"Content-Disposition": f'inline; filename="{filename}"'},
        )

    if download:
        return FileResponse(
            abs_path,
            media_type="text/csv",
            filename=filename,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return FileResponse(abs_path, media_type="text/csv", filename=filename)


async def preview_data(
    table: str = Query("data"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Query DuckDB directly for paginated data preview — no CSV re-parsing."""
    try:
        result = duckdb.sql(
            f'SELECT * FROM "{table}" LIMIT {limit} OFFSET {offset}'
        )
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        total = duckdb.sql(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={"error": str(e)},
        )

    return {
        "columns": columns,
        "rows": [dict(zip(columns, row)) for row in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.add_api_route("/upload", upload_csv, methods=["POST"])
    app.add_api_route("/download", download_csv, methods=["GET"])
    app.add_api_route("/preview", preview_data, methods=["GET"])
    yield


app = get_fast_api_app(
    agents_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    web=True,
    lifespan=lifespan,
)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
