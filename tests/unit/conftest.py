"""pytest configuration — makes spark/jobs importable in unit tests."""
import sys
from pathlib import Path

# Add spark/jobs to sys.path so `import bronze_ingest` works regardless
# of where pytest is invoked from (repo root or inside the container).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "spark" / "jobs"))
