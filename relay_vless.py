# relay_vless.py - نسخه نهایی پایدار

import asyncio
import socket
import logging
from datetime import datetime

logger = logging.getLogger("ARG-Gateway")
RELAY_BUF = 64 * 1024

# ========== تابع Relay از WebSocket به TCP ==========
async def relay_ws_to_tcp(websocket, sock, uuid):
    try:
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_bytes(), timeout=30.0)
                if not data:
                    break
                sock.sendall(data)
            except asyncio.TimeoutError:
                continue
            except:
                break
    except:
        pass
    finally:
        try:
            sock.close()
        except:
            pass

# ========== تابع Relay از TCP به WebSocket ==========
async def relay_tcp_to_ws(websocket, sock, uuid):
    loop = asyncio.get_event_loop()
    try:
        while True:
            try:
                data = await asyncio.wait_for(loop.sock_recv(sock, RELAY_BUF), timeout=30.0)
                if not data:
                    break
                await websocket.send_bytes(data)
            except asyncio.TimeoutError:
                continue
            except:
                break
    except:
        pass
    finally:
        try:
            sock.close()
        except:
            pass

# ========== تابع اصلی WebSocket Tunnel ==========
async def websocket_tunnel(websocket, uuid):
    from main import connections, LINKS, LINKS_LOCK, log_activity, is_link_allowed
    
    client_addr = websocket.client.host if websocket.client else "unknown"
    logger.info(f"🔗 New connection: {uuid} from {client_addr}")
    
    # بررسی اعتبار
    try:
        async with LINKS_LOCK:
            link = LINKS.get(uuid)
            if not link:
                await websocket.close(code=1008, reason="User not found")
                return
            if not is_link_allowed(link):
                await websocket.close(code=1008, reason="User inactive")
                return
    except:
        await websocket.close(code=1011)
        return
    
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect(("127.0.0.1", 443))
        sock.settimeout(None)
        
        connections[uuid] = {
            "ip": client_addr,
            "uuid": uuid,
            "connected_at": datetime.now().isoformat(),
            "transport": "vless-ws"
        }
        
        await asyncio.gather(
            relay_ws_to_tcp(websocket, sock, uuid),
            relay_tcp_to_ws(websocket, sock, uuid)
        )
    except Exception as e:
        logger.error(f"WS error: {e}")
    finally:
        connections.pop(uuid, None)
        if sock:
            try:
                sock.close()
            except:
                pass
        try:
            await websocket.close()
        except:
            pass
