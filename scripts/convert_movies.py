"""
รันเดี่ยวๆ จากเครื่อง (ไม่ต้องมี Airflow):  python scripts/convert_movies.py
อ่าน include/data/movies/*.json -> เขียน include/output/movies_insert.sql
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))          # ให้ import movies_lib ได้

from movies_lib import documents_to_sql, load_documents  # noqa: E402

docs = load_documents(ROOT / "include" / "data" / "movies")
sql = documents_to_sql(docs)

out = ROOT / "include" / "output" / "movies_insert.sql"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(sql, encoding="utf-8")
print(f"{len(docs)} documents -> {out.relative_to(ROOT)}")
