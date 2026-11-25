"""Build an adjacent QA-only copy of the pinned source pilot. Original files are read-only."""
from hashlib import sha256
from pathlib import Path
import sys

def build(source, destination):
    original=source.read_bytes()
    if sha256(original).hexdigest()!='a8eeecb35ce957edd8cd9c40a61dc9df8dbf48e9f3c018091536315f169bb215':
        raise ValueError('unexpected_candidate_pilot')
    text=original.decode()
    changes=[("    return {'ok':correct, 'groups':len(result['groups']) if correct else 0}",
              "    if correct and mode == 'first':\n        from retained_evidence import bounded_read, pack, MAX_IMAGE\n        blob = pack(result, bounded_read(folder/'map.png', MAX_IMAGE), session.run_id)\n        (folder/'market-only.bin').write_bytes(blob)\n    return {'ok':correct, 'groups':len(result['groups']) if correct else 0}"),
             ('    start = time.monotonic()','    retained = None\n    start = time.monotonic()'),
             ("                    result = {'ok':True,'groups':data['groups']}",
              "                    result = {'ok':True,'groups':data['groups']}\n                    if mode == 'first':\n                        from retained_evidence import bounded_read, MAX_IMAGE, MAX_JSON\n                        retained = bounded_read(folder/'market-only.bin', MAX_IMAGE+MAX_JSON+520)"),
             ('    result.update(elapsed_ms=',"    if mode == 'first' and result['ok']:\n        from retained_evidence import retain\n        result['retained_market_sha256'] = retain(jid,retained)\n    result.update(elapsed_ms=")]
    for old,new in changes:
        if text.count(old)!=1:raise ValueError('candidate_anchor_mismatch')
        text=text.replace(old,new)
    compile(text,str(destination),'exec'); destination.write_text(text)
if __name__=='__main__':build(Path(sys.argv[1]),Path(sys.argv[2]))
