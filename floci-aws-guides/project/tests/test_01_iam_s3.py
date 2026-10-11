from src.common.aws import name
from src.iam_ops import roles
from src.s3_ops import buckets, objects


def test_iam_roles_roundtrip():
    arns = roles.create_all()
    assert set(arns) == set(roles.ROLES)
    assert roles.create_all() == arns  # idempotent
    for suffix in ("lambda-role", "glue-role"):
        assert arns[suffix].endswith(name(suffix))


def test_s3_buckets_and_objects(tmp_path):
    b = buckets.create_pipeline_buckets()
    raw = b["raw"]
    assert raw in buckets.list_buckets()

    objects.put_json(raw, "raw/a.json", {"x": 1})
    assert objects.get_json(raw, "raw/a.json") == {"x": 1}
    f = tmp_path / "t.csv"
    f.write_text("a,b\n1,2\n")
    objects.upload_file(raw, "raw/t.csv", str(f))
    assert sorted(objects.list_keys(raw, "raw/")) == ["raw/a.json", "raw/t.csv"]
    objects.copy(raw, "raw/t.csv", b["curated"], "copy/t.csv")
    assert objects.get_text(b["curated"], "copy/t.csv") == "a,b\n1,2\n"
    assert objects.presigned_get(raw, "raw/t.csv").startswith("http")
    out = objects.download(raw, "raw/t.csv", str(tmp_path / "dl" / "t.csv"))
    assert out.read_text() == "a,b\n1,2\n"
