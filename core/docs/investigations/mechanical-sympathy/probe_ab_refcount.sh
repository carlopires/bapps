#!/bin/bash
# Interleaved A/B: atomic refcount vs non-atomic, alternating, same session.
cd /home/dahankzter/projects/glommio
RAW=glommio/src/task/raw.rs
BAK=/tmp/claude-1000/-home-dahankzter-projects-glommio/2d199654-44ab-4cc7-966f-5dd4df8ce37f/scratchpad/raw_ab.bak
cp $RAW $BAK
SWITCH=/tmp/claude-1000/-home-dahankzter-projects-glommio/2d199654-44ab-4cc7-966f-5dd4df8ce37f/scratchpad/probe/switch

apply_nonatomic() {
  python3 - <<'PY'
p="glommio/src/task/raw.rs"
s=open(p).read()
s=s.replace("        let refs = header.references.fetch_add(1, Ordering::Relaxed);",
            "        let refs = header.references.load(Ordering::Relaxed);\n        header.references.store(refs + 1, Ordering::Relaxed);")
s=s.replace("        let refs = header.references.fetch_sub(1, Ordering::Relaxed);",
            "        let refs = header.references.load(Ordering::Relaxed);\n        header.references.store(refs - 1, Ordering::Relaxed);")
open(p,"w").write(s)
PY
}

for round in 1 2; do
  cp $BAK $RAW
  (cd $SWITCH && cargo build --release --quiet 2>/dev/null)
  echo -n "round $round  ATOMIC     "; (cd $SWITCH && cargo run --release --quiet 2>/dev/null | tail -2 | tr "\n" " ")
  apply_nonatomic
  (cd $SWITCH && cargo build --release --quiet 2>/dev/null)
  echo -n "round $round  NON-ATOMIC "; (cd $SWITCH && cargo run --release --quiet 2>/dev/null | tail -2 | tr "\n" " ")
done
cp $BAK $RAW
