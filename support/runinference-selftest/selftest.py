#!/usr/bin/env python3
"""Drives a full RunInference tool round against the live gateway on :3100
with curl (no Cursor needed): run_request -> invoke_model (user) ->
tool_result -> final answer. Usage:
  python3 selftest.py [model_hash]
"""
import struct, subprocess, sys, time, json

MODEL = sys.argv[1] if len(sys.argv) > 1 else "910b0350730ef193"
RID = f"selftest-{int(time.time())}"

def env(p): return b"\x00" + struct.pack(">I", len(p)) + p
def varint(n):
    out = b""
    while True:
        b = n & 0x7F; n >>= 7
        out += bytes([b | (0x80 if n else 0)])
        if not n: return out
def field(no, wt, d): return varint((no << 3) | wt) + d
def s(no, v):
    b = v.encode() if isinstance(v, str) else v
    return field(no, 2, varint(len(b)) + b)
def m(no, b): return field(no, 2, varint(len(b)) + b)
def i_(no, v): return field(no, 0, varint(v))

def post(path, frame):
    return subprocess.run(
        ["curl", "-s", "-m", "10", "-X", "POST",
         f"localhost:3100{path}", "-H", "Content-Type: application/proto",
         "--data-binary", f"@{frame}", "-o", "/dev/null", "-w", "%{http_code}"],
        capture_output=True, text=True).stdout

def subscribe(out, seconds=70):
    return subprocess.Popen(
        ["curl", "-s", "-N", "-m", str(seconds), "-X", "POST",
         "localhost:3100/agent.v1.AgentService/RunSSE",
         "-H", "Content-Type: application/proto",
         "--data-binary", "@/tmp/st_runsse.bin"],
        stdout=open(out, "wb"))

# 1. subscribe + run
open("/tmp/st_runsse.bin", "wb").write(env(s(1, RID)))
proc = subscribe("/tmp/st_stream1.out")
time.sleep(1)
run_request = m(1, s(1, RID) + m(3, s(1, MODEL)))
open("/tmp/st_run.bin", "wb").write(
    env(s(1, run_request.hex()) + m(2, s(1, RID)) + i_(3, 0)))
print("run:", post("/aiserver.v1.BidiService/BidiAppend", "/tmp/st_run.bin"))

# 2. user invoke that should trigger a tool call
core = field(1, 0, varint(1)) + s(2, "用 Shell 工具执行 ls 列出当前目录，然后一句话总结这个项目")
stream_req = m(1, core) + s(5, MODEL) + m(7, s(1, MODEL))
invoke = s(1, f"{RID}-a") + m(2, stream_req)
open("/tmp/st_inv.bin", "wb").write(
    env(s(1, m(2, invoke).hex()) + m(2, s(1, RID)) + i_(3, 1)))
print("invoke:", post("/aiserver.v1.BidiService/BidiAppend", "/tmp/st_inv.bin"))

# 3. wait for a tool call id in the stream
call_id = None
for _ in range(50):
    data = open("/tmp/st_stream1.out", "rb").read()
    k = data.find(b"call_")
    if k > 0:
        j = data.find(b"\x12", k)
        call_id = data[k:j].decode(errors="replace")
        break
    time.sleep(1)
print("tool_call_id:", call_id)
proc.terminate()

# 4. tool result (delta message only; engine must synthesize assistant tool_use)
RESULT = "CLAUDE.md\nREADME.md\napps\nprotocols\nserver\nsupport"
rv = field(3, 2, varint(len(RESULT)) + RESULT.encode())
parts = s(1, call_id or "call_x") + s(2, "Shell") + m(3, rv)
core = field(1, 0, varint(3)) + m(6, m(1, parts))
stream_req = m(1, core) + s(5, MODEL) + m(7, s(1, MODEL))
invoke = s(1, f"{RID}-b") + m(2, stream_req)
open("/tmp/st_result.bin", "wb").write(
    env(s(1, m(2, invoke).hex()) + m(2, s(1, RID)) + i_(3, 2)))
proc = subscribe("/tmp/st_stream2.out")
time.sleep(1)
print("tool_result:", post("/aiserver.v1.BidiService/BidiAppend", "/tmp/st_result.bin"))

# 5. final answer should contain text deltas
time.sleep(25)
data = open("/tmp/st_stream2.out", "rb").read()
print(f"final stream: {len(data)} bytes")
print("has error frame:", b'Anthropic 400' in data or b'provider error' in data)
print("tail:", data[-200:])
