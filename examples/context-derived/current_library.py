"""Current generated scope integrity, independent of the historical fixture."""
import hashlib,json
from pathlib import Path
def current_library_inputs():
    case=Path(__file__).resolve().parent
    root=case/"rust-lib-core"
    meta=root/"current-generator-fixture.json"
    m=json.loads(meta.read_text())
    sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
    assert m["evaluationErrors"]==0 and sha(case/"model.xml")==m["modelSha256"]
    actual=[]
    for p in root.rglob("*"):
        assert not p.is_symlink()
        if p.is_file():actual.append(str(p.relative_to(root)))
    assert sorted(actual)==sorted([*m["files"],"current-generator-fixture.json"])
    for name,digest in m["files"].items():
        assert not Path(name).is_absolute() and ".." not in Path(name).parts
        assert sha(root/name)==digest,name
    return dict(files=m["files"],modelSha256=m["modelSha256"],metadataSha256=sha(meta))
