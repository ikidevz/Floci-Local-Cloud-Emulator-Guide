import io
import zipfile
from pathlib import Path


def zip_dir(src_dir: str) -> bytes:
    """Zip a directory with files at the archive root (handler.py must not be nested)."""
    buf = io.BytesIO()
    root = Path(src_dir)
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(root.rglob("*")):
            if p.is_file() and "__pycache__" not in p.parts:
                zf.write(p, p.relative_to(root))
    return buf.getvalue()
