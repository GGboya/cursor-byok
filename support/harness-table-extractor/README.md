# agent-harness table extractor

Cursor's downloaded agent-harness module (`~/Library/Application Support/Cursor/agent-harness/versions/<hash>/agent-harness.cjs`)
declares protobuf types in protobuf-es v2 *compact table* strings
(`static $(){return["Msg|1 field 9?|2 other #0", <refVar>, ...]}`), which the
Go protocol extractor cannot parse. This script extracts a chosen type closure
from those tables into a `.proto` file:

```bash
python3 extract_runinference.py   # writes /tmp/rif/runinference_v1.proto
```

Notes learned while building this (2026-11-04):
- `#N` indexes the variables *after* the self table string in the same array
  (`[selfTable, ref0, ref1, ...]` → `#0` = `ref0`).
- Variable names are per-module-scope in the minified bundle; resolve refs by
  the nearest definition *before* the referencing class.
- Well-known types (Struct/Value) use the classic `typeName=` declaration, not
  compact tables; classes without `$()` in their own span must be identified
  via `.typeName`.
- Enums are built by a minified helper shaped like `VAR=Re(l,nd,"Name",[[no,"VAL"],...])`.
- Enum value names must be prefixed (e.g. `INFERENCE_MESSAGE_ROLE_USER`) to
  satisfy proto3 package-wide enum-value uniqueness.
