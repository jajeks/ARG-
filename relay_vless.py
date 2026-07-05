# relay_vless.py - نسخه نهایی پایدار (تنظیم شده با Xray 127.0.0.1:443)

import asyncio
import socket
import logging
from datetime import datetime

logger = logging.getLogger("ARG-Gateway")

RELAY_BUF = 64 * 1024  # 64KB

# ========== تنظیمات سرور Xray ==========
TARGET_HOST = "127.0.0.1"   # ← آدرس Xray (همون سرور)
TARGET_PORT = 443           # ← پورت Xray (پورت پیش‌فرض)

# ========== تابع Relay از WebSocket به TCP ==========
async def relay_ws_to_tcp(websocket, sock: socket.socket, uuid: str):
    """Relay از WebSocket به TCP"""
    try:
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_bytes(), timeout=30.0)
                if not data:
                    break
                try:
                    sock.sendall(data)
                    # به‌روزرسانی مصرف
                    try:
                        from main import LINKS, LINKS_LOCK
                        async with LINKS_LOCK:
                            if uuid in LINKS:
                                LINKS[uuid]["used_bytes"] = LINKS[uuid].get("used_bytes", 0) + len(data)
                    except:
                        pass
                except (socket.error, BrokenPipeError, ConnectionResetError):
                    break
            except asyncio.TimeoutError:
                continue
            except Exception:
                break
    except Exception:
        pass
    finally:
        try:
            sock.close()
        except:
            pass

# ========== تابع Relay از TCP به WebSocket ==========
async def relay_tcp_to_ws(websocket, sock: socket.socket, uuid: str):
    """Relay از TCP به WebSocket"""
    loop = asyncio.get_event_loop()
    try:
        while True:
            try:
                data = await asyncio.wait_for(
                    loop.sock_recv(sock, RELAY_BUF),
                    timeout=30.0
                )
                if not data:
                    break
                try:
                    await websocket.send_bytes(data)
                    # به‌روزرسانی مصرف
                    try:
                        from main import LINKS, LINKS_LOCK
                        async with LINKS_LOCK:
                            if uuid in LINKS:
                                LINKS[uuid]["used_bytes"] = LINKS[uuid].get("used_bytes", 0) + len(data)
                    except:
                        pass
                except Exception:
                    break
            except asyncio.TimeoutError:
                continue
            except (socket.error, ConnectionResetError):
                break
    except Exception:
        pass
    finally:
        try:
            sock.close()
        except:
            pass

# ========== تابع بررسی اعتبار کاربر ==========
async def check_and_use(uuid: str) -> bool:
    """بررسی اعتبار کاربر و افزایش مصرف"""
    try:
        from main import LINKS, LINKS_LOCK, is_link_allowed
        async with LINKS_LOCK:
            link = LINKS.get(uuid)
            if not link:
                return False
            if not is_link_allowed(link):
                return False
            link["used_bytes"] = link.get("used_bytes", 0) + 1024
            return True
    except Exception:
        return False

# ========== تابع اصلی WebSocket Tunnel ==========
async def websocket_tunnel(websocket, uuid: str):
    """WebSocket tunnel اصلی - نسخه پایدار"""
    from main import connections, LINKS, LINKS_LOCK, log_activity, is_link_allowed
    
    client_addr = websocket.client.host if websocket.client else "unknown"
    logger.info(f"🔗 New WS connection: {uuid} from {client_addr}")
    
    # ===== مرحله 1: بررسی اعتبار کاربر =====
    try:
        async with LINKS_LOCK:
            link = LINKS.get(uuid)
            if not link:
                await websocket.close(code=1008, reason="User not found")
                logger.warning(f"❌ User not found: {uuid}")
                return
            
            if not is_link_allowed(link):
                await websocket.close(code=1008, reason="User inactive or expired")
                logger.warning(f"❌ User inactive/expired: {uuid}")
                return
            
            logger.info(f"✅ User validated: {uuid} - {link.get('label', 'Unknown')}")
            
    except Exception as e:
        logger.error(f"❌ Auth error: {e}")
        try:
            await websocket.close(code=1011, reason="Internal error")
        except:
            pass
        return
    
    # ===== مرحله 2: اتصال به سرور مقصد =====
    sock = None
    
    try:
        # ایجاد سوکت
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(15)
        
        # اتصال به سرور Xray
        sock.connect((TARGET_HOST, TARGET_PORT))
        sock.settimeout(None)
        
        logger.info(f"🔗 Connected to Xray: {TARGET_HOST}:{TARGET_PORT}")
        
        # ذخیره اتصال
        connections[uuid] = {
            "ip": client_addr,
            "uuid": uuid,
            "connected_at": datetime.now().isoformat(),
            "transport": "vless-ws",
            "bytes": 0,
        }
        
        log_activity("ws", f"User {uuid} connected from {client_addr}", "ok")
        
        # ===== مرحله 3: شروع Relay =====
        await asyncio.gather(
            relay_ws_to_tcp(websocket, sock, uuid),
            relay_tcp_to_ws(websocket, sock, uuid),
        )
        
    except asyncio.CancelledError:
        logger.info(f"⏹️ Task cancelled: {uuid}")
        
    except socket.timeout:
        logger.error(f"⏱️ Connection timeout to Xray: {uuid}")
        try:
            await websocket.close(code=1011, reason="Connection timeout")
        except:
            pass
        
    except ConnectionRefusedError:
        logger.error(f"🚫 Xray not running on {TARGET_HOST}:{TARGET_PORT}")
        try:
            await websocket.close(code=1011, reason="Xray unreachable")
        except:
            pass
        
    except Exception as e:
        logger.error(f"❌ WS error: {e}")
        try:
            await websocket.close(code=1011, reason=f"Error: {str(e)[:50]}")
        except:
            pass
        
    finally:
        # ===== مرحله 4: پاکسازی =====
        connections.pop(uuid, None)
        
        if sock:
            try:
                sock.close()
                logger.debug(f"🔌 Socket closed: {uuid}")
            except:
                pass
        
        try:
            await websocket.close(code=1000)
        except:
            pass
        
        log_activity("ws", f"User {uuid} disconnected from {client_addr}", "info")
        logger.info(f"👋 WS disconnected: {uuid}")
