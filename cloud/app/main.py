import json, os, time
from collections import deque
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

AGENT_TOKEN    = os.getenv("AGENT_TOKEN", "change-this-secret-token")
DASHBOARD_PASS = os.getenv("DASHBOARD_PASS", "lospros677")

_latest = {}
_history = deque(maxlen=500)
_ws_clients = []
_pending_commands = deque(maxlen=50)
_last_push = 0.0

app = FastAPI(title="LosPros677 Cloud", docs_url=None)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

static_path = os.path.join(os.path.dirname(__file__), "..", "static")
if os.path.isdir(static_path):
    app.mount("/static", StaticFiles(directory=static_path), name="static")

bearer = HTTPBearer(auto_error=False)

def verify_agent(creds: Optional[HTTPAuthorizationCredentials] = Depends(bearer)):
    if not creds or creds.credentials != AGENT_TOKEN:
        raise HTTPException(status_code=401, detail="Token invalido")

def verify_dashboard(creds: Optional[HTTPAuthorizationCredentials] = Depends(bearer)):
    if not creds or creds.credentials != DASHBOARD_PASS:
        raise HTTPException(status_code=401, detail="Acceso denegado")

class ThrottleCmd(BaseModel):
    ip: str
    limit_mbps: Optional[float] = None

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    html_path = os.path.join(static_path, "index.html")
    if os.path.exists(html_path):
        return FileResponse(html_path)
    return HTMLResponse("<h2>Dashboard no encontrado</h2>")

@app.post("/api/push")
async def receive_push(payload: dict, _=Depends(verify_agent)):
    global _latest, _last_push
    _latest = payload
    _last_push = time.time()
    snap = {**payload, "received_at": datetime.utcnow().isoformat()+"Z"}
    _history.append(snap)
    dead = []
    for ws in _ws_clients:
        try: await ws.send_text(json.dumps(snap, default=str))
        except: dead.append(ws)
    for ws in dead: _ws_clients.remove(ws)
    return {"ok": True, "ws_clients": len(_ws_clients)}

@app.get("/api/status")
async def get_status(_=Depends(verify_dashboard)):
    age = round(time.time()-_last_push, 1) if _last_push else None
    return {"agent_online": age is not None and age<15, "agent_last_push_s": age, "snapshot": _latest}

@app.get("/api/history")
async def get_history(n: int = 60, _=Depends(verify_dashboard)):
    return list(_history)[-n:]

@app.post("/api/control/throttle")
async def throttle(cmd: ThrottleCmd, _=Depends(verify_dashboard)):
    _pending_commands.append({"type":"throttle","ip":cmd.ip,"limit_mbps":cmd.limit_mbps,"ts":datetime.utcnow().isoformat()+"Z"})
    return {"ok": True}

@app.get("/api/commands")
async def get_commands(_=Depends(verify_agent)):
    cmds = list(_pending_commands)
    _pending_commands.clear()
    return {"commands": cmds}

@app.get("/health")
async def health():
    age = round(time.time()-_last_push,1) if _last_push else None
    return {"status":"ok","agent_online": age is not None and age<15}

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    _ws_clients.append(ws)
    if _latest:
        try: await ws.send_text(json.dumps(_latest, default=str))
        except: pass
    try:
        while True: await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if ws in _ws_clients: _ws_clients.remove(ws)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT",8000)))
