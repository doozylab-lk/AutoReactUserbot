#!/usr/bin/env python3
"""
Userbot-only combined app.py
- Inline buttons and callbacks are handled by the user account.
- Voice playback uses an assistant account (ASSISTANT_SESSION) + pytgcalls when available,
  or uses the userbot account's PyTgCalls if ASSISTANT_SESSION is not provided.
- YouTube playback (extract) uses yt-dlp (yt_dlp).
Replace existing app.py with this file.
"""
import os
import re
import time
import asyncio
import logging
import random
import inspect
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse, parse_qs

from dotenv import load_dotenv
load_dotenv()

from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from pyrogram.errors import FloodWait, ReactionInvalid, PeerIdInvalid, RPCError

# optional pytgcalls
try:
    from pytgcalls import PyTgCalls
    from pytgcalls.types import MediaStream
except Exception:
    PyTgCalls = None
    MediaStream = None

# yt-dlp
try:
    import yt_dlp as youtube_dl
except Exception:
    youtube_dl = None

# thumbnail & helpers
import aiohttp
import aiofiles
from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont, ImageOps

# optional DB
try:
    from pymongo import MongoClient
except Exception:
    MongoClient = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dlk_userbot")

# ===================== ENV / CONFIG =====================
API_ID = int(os.environ.get("API_ID", "0") or 0)
API_HASH = os.environ.get("API_HASH", "") or ""
SESSION_STRING = os.environ.get("SESSION_STRING")
ASSISTANT_SESSION = os.environ.get("ASSISTANT_SESSION")  # assistant user session string (optional)
MONGO_URI = os.environ.get("MONGO_URI", "")
MONGO_DBNAME = os.environ.get("MONGO_DBNAME", "dlk_radio")
OWNER_ID_ENV = os.environ.get("OWNER_ID")
OWNER_ID: Optional[int] = int(OWNER_ID_ENV) if OWNER_ID_ENV else None

# New: toggle inline player controls via environment (kept for compatibility)
INLINE_CONTROLS = os.environ.get("INLINE_CONTROLS", "1") != "0"

if not API_ID or not API_HASH:
    logger.critical("API_ID and API_HASH must be set in environment")
    raise SystemExit(1)

# ===================== CLIENTS =====================
user_app = Client(
    name="userbot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    in_memory=True,
)

assistant = None
call_py = None
CALL_CLIENT = None  # client instance used by PyTgCalls (assistant or user_app)

if ASSISTANT_SESSION:
    assistant = Client("assistant", api_id=API_ID, api_hash=API_HASH, session_string=ASSISTANT_SESSION, in_memory=True)
    CALL_CLIENT = assistant
    if PyTgCalls:
        call_py = PyTgCalls(assistant)
    else:
        logger.warning("pytgcalls not available - voice playback disabled.")
else:
    # Use userbot's client for voice if pytgcalls is present and assistant isn't configured.
    CALL_CLIENT = user_app
    if PyTgCalls:
        call_py = PyTgCalls(user_app)
    else:
        logger.info("pytgcalls not available - voice playback disabled (no assistant).")

if not ASSISTANT_SESSION:
    logger.info("ASSISTANT_SESSION not provided — attempting to use user account for VC if pytgcalls available.")

# ===================== DB SETUP =====================
mongo_client = None
db = None
settings_coll = None
playing_coll = None
if MONGO_URI:
    if MongoClient is None:
        logger.warning("pymongo not installed; continuing without DB persistence.")
    else:
        try:
            mongo_client = MongoClient(MONGO_URI)
            db = mongo_client.get_database(MONGO_DBNAME)
            settings_coll = db.get_collection("react_settings")
            playing_coll = db.get_collection("playing")
            logger.info("Connected to MongoDB.")
        except Exception as e:
            logger.warning(f"Failed to connect to MongoDB: {e}")

# ===================== IN-MEMORY CACHES =====================
VALID_EMOJIS = [
    "👍", "👎", "❤️", "🔥", "🥰", "👏", "😁", "🤔", "🤯", "😱",
    "🤬", "😢", "🎉", "🤩", "🤮", "💩", "🙏", "👌", "🤡",
    "🥱", "🥴", "😍", "🐳", "❤️‍🔥", "🌭", "💯", "🤣", "⚡", "🍌",
    "🏆", "💔", "🤨", "😐", "🍓", "🍾", "💋", "🖕", "😈", "😴"
]
react_cache: Dict[tuple, bool] = {}
radio_cache: Dict[tuple, str] = {}

# Example stations
RADIO_STATION = {
    "SirasaFM": "http://live.trusl.com:1170/;",
    "HelaNadaFM": "https://stream-176.zeno.fm/9ndoyrsujwpvv",
    "RedFM": "https://shaincast.caster.fm:47830/listen.mp3",
    "HiruFM": "https://radio.lotustechnologieslk.net:2020/stream/hirufmgarden?1707015384",
}

# Thumbnail cache dirs
THUMB_CACHE_DIR = "cache"
os.makedirs(THUMB_CACHE_DIR, exist_ok=True)
DOWNLOADS_DIR = "downloads"
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

# Radio runtime state
radio_tasks: Dict[int, asyncio.Task] = {}
radio_paused = set()
radio_state: Dict[int, Dict[str, Any]] = {}
radio_queue: Dict[int, List[Dict[str, Any]]] = {}
track_watchers: Dict[int, asyncio.Task] = {}

# ===================== UTIL: DB helpers =====================
def _key(owner_id: int, chat_id: int) -> dict:
    return {"owner_id": owner_id, "chat_id": chat_id}

def get_react_setting(owner_id: int, chat_id: int) -> bool:
    if settings_coll is None:
        return True
    doc = settings_coll.find_one(_key(owner_id, chat_id), {"react": 1})
    if doc and "react" in doc:
        return bool(doc["react"])
    return True

def set_react_setting(owner_id: int, chat_id: int, enabled: bool) -> None:
    if settings_coll:
        settings_coll.update_one(_key(owner_id, chat_id), {"$set": {"react": bool(enabled)}}, upsert=True)
    react_cache[(owner_id, chat_id)] = bool(enabled)

def get_radio(owner_id: int, chat_id: int) -> Optional[str]:
    if settings_coll is None:
        return None
    doc = settings_coll.find_one(_key(owner_id, chat_id), {"radio_url": 1})
    if doc:
        return doc.get("radio_url")
    return None

def set_radio(owner_id: int, chat_id: int, url: Optional[str]) -> None:
    if settings_coll is None:
        return
    k = _key(owner_id, chat_id)
    if url:
        settings_coll.update_one(k, {"$set": {"radio_url": url}}, upsert=True)
        radio_cache[(owner_id, chat_id)] = url
    else:
        settings_coll.update_one(k, {"$unset": {"radio_url": ""}})
        radio_cache.pop((owner_id, chat_id), None)

def load_caches_for_owner(owner_id: int):
    if settings_coll is None:
        return
    for doc in settings_coll.find({"owner_id": owner_id}):
        key = (doc["owner_id"], doc["chat_id"])
        react_cache[key] = bool(doc.get("react", True))
        if "radio_url" in doc:
            radio_cache[key] = doc["radio_url"]

# ===================== STARTUP helper =====================
async def ensure_owner_id():
    global OWNER_ID
    if OWNER_ID is None:
        me = await user_app.get_me()
        OWNER_ID = me.id
    load_caches_for_owner(OWNER_ID)
    logger.info(f"Owner user id: {OWNER_ID}")

# ===================== THUMB / IMAGE HELPERS (trimmed) =====================
def clear_title(text: str) -> str:
    parts = (text or "").split(" ")
    title = ""
    for i in parts:
        if len(title) + len(i) < 60:
            title += " " + i
    return title.strip()

async def _download_file(url: str, dest: str) -> Optional[str]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                f = await aiofiles.open(dest, mode="wb")
                await f.write(await resp.read())
                await f.close()
                return dest
    except Exception as e:
        logger.debug(f"_download_file failed: {e}")
        try:
            if os.path.exists(dest):
                os.remove(dest)
        except Exception:
            pass
        return None

def _create_circular_artwork(image: Image.Image, diameter: int = 520, border: int = 8) -> Image.Image:
    try:
        square = ImageOps.fit(image, (diameter, diameter), centering=(0.5, 0.5))
    except Exception:
        square = image.resize((diameter, diameter), Image.LANCZOS)
    mask = Image.new('L', (diameter, diameter), 0)
    draw = ImageDraw.Draw(mask)
    draw.ellipse((0, 0, diameter, diameter), fill=255)
    circ = Image.new('RGBA', (diameter, diameter), (0, 0, 0, 0))
    circ.paste(square.convert('RGBA'), (0, 0), mask=mask)
    out_size = diameter + border * 2
    out = Image.new('RGBA', (out_size, out_size), (0, 0, 0, 0))
    shadow = Image.new('RGBA', (out_size, out_size), (0, 0, 0, 0))
    shadow_mask = Image.new('L', (out_size, out_size), 0)
    draw_sm = ImageDraw.Draw(shadow_mask)
    draw_sm.ellipse((border//2, border//2, out_size - border//2, out_size - border//2), fill=200)
    shadow.putalpha(shadow_mask)
    shadow = shadow.filter(ImageFilter.GaussianBlur(radius=6))
    out = Image.alpha_composite(out, shadow)
    border_layer = Image.new('RGBA', (out_size, out_size), (255, 255, 255, 0))
    draw_bl = ImageDraw.Draw(border_layer)
    draw_bl.ellipse((border, border, out_size - border, out_size - border), fill=(255, 255, 255, 255))
    inner_margin = border + 4
    draw_bl.ellipse((inner_margin, inner_margin, out_size - inner_margin, out_size - inner_margin), fill=(0, 0, 0, 0))
    out = Image.alpha_composite(out, border_layer)
    paste_pos = (border, border)
    out.paste(circ, paste_pos, circ)
    return out

async def _process_image_and_overlay(src_path: str, out_key: str, title: str) -> Optional[str]:
    try:
        image = Image.open(src_path).convert("RGBA")
        try:
            background = ImageOps.fit(image, (1280, 720), centering=(0.5, 0.5)).convert("RGBA")
        except Exception:
            background = image.resize((1280, 720), Image.LANCZOS).convert("RGBA")
        background = background.filter(ImageFilter.BoxBlur(6))
        enhancer = ImageEnhance.Brightness(background)
        background = enhancer.enhance(0.85)
        art = _create_circular_artwork(image, diameter=520, border=10)
        art_x = 60
        art_y = (720 - art.size[1]) // 2
        background.paste(art, (art_x, art_y), art)
        draw = ImageDraw.Draw(background)
        try:
            title_font = ImageFont.truetype("arial.ttf", 48)
            small_font = ImageFont.truetype("arial.ttf", 18)
        except Exception:
            title_font = ImageFont.load_default()
            small_font = ImageFont.load_default()
        draw.text((20, 20), "DLK DEVELOPER", fill="white", font=small_font)
        title_x = art_x + art.size[0] + 30
        title_y = art_y + 30
        shadow_color = (0, 0, 0, 200)
        for dx, dy in ((1, 1), (2, 2)):
            draw.text((title_x + dx, title_y + dy), clear_title(title), fill=shadow_color, font=title_font)
        draw.text((title_x, title_y), clear_title(title), fill="white", font=title_font)
        out_path = os.path.join(THUMB_CACHE_DIR, f"{out_key}.png")
        background.save(out_path)
        return out_path
    except Exception as e:
        logger.debug(f"_process_image failed: {e}")
        return None

async def get_thumb_from_url_or_webpage(thumbnail_url: Optional[str], webpage: Optional[str], title: str) -> Optional[str]:
    if thumbnail_url:
        if os.path.isfile(thumbnail_url):
            key = re.sub(r"[^0-9A-Za-z_-]", "_", os.path.basename(thumbnail_url))[:40]
            return await _process_image_and_overlay(thumbnail_url, key, title)
        if thumbnail_url.startswith("http"):
            key = re.sub(r"[^0-9A-Za-z_-]", "_", thumbnail_url)[:40]
            tmp = os.path.join(THUMB_CACHE_DIR, f"tmp_{key}")
            downloaded = await _download_file(thumbnail_url, tmp)
            if downloaded:
                processed = await _process_image_and_overlay(downloaded, key, title)
                try:
                    os.remove(downloaded)
                except Exception:
                    pass
                return processed
    # fallback not implemented fully (keeps simple)
    return None

# ===================== YT / stream extraction =====================
def looks_like_url(text: str) -> bool:
    try:
        p = urlparse(text)
        return bool(p.scheme and p.netloc)
    except Exception:
        return False

def get_youtube_id(url: str) -> Optional[str]:
    try:
        p = urlparse(url)
        if "youtube" in p.netloc or "youtu.be" in p.netloc:
            if p.netloc.endswith("youtu.be"):
                return p.path.lstrip("/")
            qs = parse_qs(p.query)
            if "v" in qs:
                return qs["v"][0]
            match = re.search(r"/embed/([^/?&]+)", p.path)
            if match:
                return match.group(1)
    except Exception:
        pass
    return None

def extract_audio_url(query: str) -> Optional[Dict[str, Any]]:
    if youtube_dl is None:
        logger.warning("yt_dlp not installed. /play requires yt-dlp.")
        return None
    target = query if looks_like_url(query) else f"ytsearch1:{query}"
    ydl_opts = {
        "format": "bestaudio/best",
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
    }
    try:
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(target, download=False)
            if not info:
                return None
            if "entries" in info and isinstance(info["entries"], list) and info["entries"]:
                info = info["entries"][0]
            stream_url = info.get("url")
            if not stream_url and "formats" in info:
                formats = info.get("formats", [])
                best = None
                for f in sorted(formats, key=lambda x: (x.get("abr") or 0), reverse=True):
                    if f.get("acodec") and f.get("url"):
                        best = f.get("url")
                        break
                stream_url = best or stream_url
            if not stream_url:
                logger.warning("yt_dlp did not return playable stream URL.")
                return None
            return {
                "title": info.get("title") or "Unknown",
                "webpage_url": info.get("webpage_url") or info.get("id") or target,
                "stream_url": stream_url,
                "thumbnail": info.get("thumbnail"),
                "duration": int(info.get("duration")) if info.get("duration") else None,
            }
    except Exception as e:
        logger.warning(f"yt_dlp extraction failed for {query}: {e}")
        return None

# ===================== PLAY FLOW =====================
async def _safe_call_py_method(method_name: str, *args, **kwargs):
    try:
        if not call_py:
            return None
        if not hasattr(call_py, method_name):
            return None
        attr = getattr(call_py, method_name)
        if not callable(attr):
            return None
        result = attr(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result
    except Exception as e:
        logger.debug(f"_safe_call_py_method {method_name} failed: {e}")
        return None

def player_controls_markup(chat_id: int):
    # Inline player controls removed per request - return None so no inline buttons are posted.
    return None

async def update_radio_timer(chat_id: int, msg_id: int, title: str, start_time: float):
    while True:
        try:
            elapsed = int(time.time() - start_time)
            m, s = divmod(elapsed, 60)
            h, m = divmod(m, 60)
            timer = f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"
            caption = f"🎧 Now Playing: {title}\n⏳ Duration: {timer}"
            try:
                await user_app.edit_message_caption(chat_id=chat_id, message_id=msg_id, caption=caption, reply_markup=player_controls_markup(chat_id))
            except Exception:
                try:
                    await user_app.edit_message_text(chat_id, msg_id, caption, reply_markup=player_controls_markup(chat_id))
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"Timer update failed for {chat_id}/{msg_id}: {e}")
            break
        await asyncio.sleep(8)

def store_play_state(chat_id: int, title: str, url: str, msg_id: int, start_time: Optional[float], elapsed: float = 0.0, paused: bool = False):
    state = {"chat_id": chat_id, "station": title, "url": url, "msg_id": msg_id, "start_time": start_time, "elapsed": elapsed, "paused": paused, "ts": time.time()}
    radio_state[chat_id] = state
    try:
        if playing_coll:
            playing_coll.update_one({"chat_id": chat_id}, {"$set": state}, upsert=True)
    except Exception:
        pass

async def leave_voice_chat(chat_id: int):
    try:
        if chat_id in radio_tasks:
            radio_tasks[chat_id].cancel()
            radio_tasks.pop(chat_id, None)
        if chat_id in track_watchers:
            try:
                track_watchers[chat_id].cancel()
            except Exception:
                pass
            track_watchers.pop(chat_id, None)
        if chat_id in radio_paused:
            radio_paused.discard(chat_id)
        radio_state.pop(chat_id, None)
        if call_py:
            try:
                await _safe_call_py_method("leave_call", chat_id)
                await _safe_call_py_method("stop", chat_id)
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Failed to leave VC/cancel task for {chat_id}: {e}")

async def prepare_entry_from_reply(reply_msg: Message) -> Optional[Dict[str, Any]]:
    try:
        media_field = None
        if reply_msg.voice:
            media_field = reply_msg.voice
        elif reply_msg.audio:
            media_field = reply_msg.audio
        elif reply_msg.document:
            media_field = reply_msg.document
        if media_field is None:
            return None
        ext = os.path.splitext(getattr(media_field, "file_name", "") or "")[1] or ""
        if not ext:
            mime = getattr(media_field, "mime_type", "") or ""
            if "ogg" in mime or "opus" in mime:
                ext = ".ogg"
            elif "mpeg" in mime or "mp3" in mime:
                ext = ".mp3"
            elif "wav" in mime:
                ext = ".wav"
            else:
                ext = ".raw"
        base_name = f"audio_{int(time.time())}_{random.randint(1000,9999)}"
        download_path = os.path.join(DOWNLOADS_DIR, base_name + ext)
        local_path = await user_app.download_media(reply_msg, file_name=download_path)
        title = getattr(media_field, "title", None) or getattr(media_field, "file_name", None) or reply_msg.caption or "Telegram Audio"
        duration = getattr(media_field, "duration", None) or None
        thumb_path = None
        if reply_msg.photo:
            tmp_img = os.path.join(THUMB_CACHE_DIR, f"photo_{base_name}.jpg")
            thumb_path_local = await user_app.download_media(reply_msg.photo, file_name=tmp_img)
            thumb_path = await _process_image_and_overlay(thumb_path_local, base_name, title)
            try:
                os.remove(thumb_path_local)
            except Exception:
                pass
        entry = {
            "title": title,
            "stream_url": local_path,
            "webpage": None,
            "thumbnail": thumb_path,
            "duration": duration,
            "is_local": True,
        }
        return entry
    except Exception as e:
        logger.debug(f"prepare_entry_from_reply failed: {e}")
        return None

async def play_entry(chat_id: int, entry: dict, reply_message: Optional[Message] = None):
    try:
        if chat_id in radio_tasks:
            radio_tasks[chat_id].cancel()
            radio_tasks.pop(chat_id, None)
        stream_source = entry["stream_url"]
        # play via call_py (either assistant or user account)
        if not call_py:
            # fallback: just post the link
            try:
                await user_app.send_message(chat_id, f"▶️ Now playing: {entry.get('title')}\n{stream_source}")
            except Exception:
                pass
            return True

        # If the PyTgCalls instance is using assistant, ensure assistant is present in the chat.
        if CALL_CLIENT is not None and CALL_CLIENT != user_app:
            # CALL_CLIENT is assistant: make sure assistant is in chat
            try:
                assistant_user = await CALL_CLIENT.get_me()
                assistant_id = assistant_user.id
            except Exception:
                assistant_id = None
            assistant_present = False
            if assistant_id:
                try:
                    await CALL_CLIENT.get_chat_member(chat_id, assistant_id)
                    assistant_present = True
                except Exception:
                    assistant_present = False
            if not assistant_present:
                # try inviting assistant
                try:
                    invite = await user_app.create_chat_invite_link(chat_id, member_limit=1, name="dlk_assistant_invite")
                    invite_link = invite.invite_link
                    try:
                        await CALL_CLIENT.join_chat(invite_link)
                        assistant_present = True
                    except Exception:
                        # cannot auto-join assistant
                        await user_app.send_message(chat_id, "Assistant not in group. Add it via invite link and retry.")
                        await user_app.send_message(chat_id, invite_link)
                        return False
                except Exception:
                    await user_app.send_message(chat_id, "Assistant not in this group. Please add the assistant account and try again.")
                    return False

        # Call play on the active PyTgCalls instance
        await _safe_call_py_method("play", chat_id, MediaStream(stream_source))

        thumb_path = None
        thumb_val = entry.get("thumbnail")
        title = entry.get("title") or "Unknown"
        if thumb_val and isinstance(thumb_val, str) and os.path.isfile(thumb_val):
            thumb_path = thumb_val
        elif thumb_val and isinstance(thumb_val, str) and thumb_val.startswith("http"):
            thumb_path = await get_thumb_from_url_or_webpage(thumb_val, entry.get("webpage"), title)
        else:
            thumb_path = None

        if thumb_path and os.path.isfile(thumb_path):
            try:
                msg = await user_app.send_photo(chat_id, photo=thumb_path, caption=f"🎧 Now Playing: {title}", reply_markup=player_controls_markup(chat_id))
            except Exception:
                msg = await user_app.send_photo(chat_id, photo="https://files.catbox.moe/3o9qj5.jpg", caption=f"🎧 Now Playing: {title}", reply_markup=player_controls_markup(chat_id))
        else:
            msg = await user_app.send_photo(chat_id, photo="https://files.catbox.moe/3o9qj5.jpg", caption=f"🎧 Now Playing: {title}", reply_markup=player_controls_markup(chat_id))

        start_time = time.time()
        store_play_state(chat_id, title, entry.get("stream_url"), msg.id, start_time, elapsed=0.0, paused=False)
        radio_tasks[chat_id] = asyncio.create_task(update_radio_timer(chat_id, msg.id, title, start_time))
        radio_paused.discard(chat_id)
        duration = entry.get("duration")
        if duration:
            if chat_id in track_watchers:
                try:
                    track_watchers[chat_id].cancel()
                except Exception:
                    pass
            track_watchers[chat_id] = asyncio.create_task(track_watcher(chat_id, duration, msg.id))
        return True
    except Exception as e:
        logger.exception("Play entry failed")
        try:
            await leave_voice_chat(chat_id)
        except Exception:
            pass
        return False

async def track_watcher(chat_id: int, duration: int, msg_id: int):
    try:
        await asyncio.sleep(max(1, duration) + 2)
        q = radio_queue.get(chat_id, [])
        if q:
            next_entry = q.pop(0)
            radio_queue[chat_id] = q
            await play_entry(chat_id, next_entry)
        else:
            try:
                await leave_voice_chat(chat_id)
            except Exception:
                pass
            try:
                await user_app.edit_message_caption(chat_id=chat_id, message_id=msg_id, caption="▶️ Playback finished.", reply_markup=None)
            except Exception:
                pass
    except asyncio.CancelledError:
        return
    except Exception as e:
        logger.debug(f"track_watcher error for {chat_id}: {e}")

# ===================== UI: radio menu + controls =====================
def radio_buttons(page: int = 0, per_page: int = 6):
    # Inline radio buttons removed — return None to avoid showing them.
    return None

# ===================== PRIVILEGE CHECK =====================
async def dlk_privilege_validator(subject: Any) -> bool:
    try:
        if isinstance(subject, CallbackQuery):
            user = subject.from_user
            chat = subject.message.chat
            sender_chat = getattr(subject.message, "sender_chat", None)
        else:
            user = subject.from_user
            chat = subject.chat
            sender_chat = getattr(subject, "sender_chat", None)
        if user and OWNER_ID and user.id == OWNER_ID:
            return True
        if chat.type == "private":
            return False
        if user:
            try:
                member = await user_app.get_chat_member(chat.id, user.id)
                status = getattr(member, "status", "").lower()
                if status in ("administrator", "creator"):
                    return True
            except Exception:
                pass
        if sender_chat:
            try:
                member = await user_app.get_chat_member(chat.id, sender_chat.id)
                status = getattr(member, "status", "").lower()
                if status in ("administrator", "creator"):
                    return True
            except Exception:
                pass
        return False
    except Exception as e:
        logger.warning(f"Privilege check failed: {e}")
        return False

# ===================== COMMANDS & CALLBACKS =====================

# help
@user_app.on_message(filters.command("help", prefixes=["!", "/"]) & filters.me)
async def user_help(client: Client, message: Message):
    await ensure_owner_id()
    await message.reply_text(
        "Userbot Help\n\n"
        "!react - Post control buttons to toggle Auto-React for this chat\n"
        "!setradio <url> - Save a radio URL for this chat\n"
        "!radio - List stations or use: !radio <station-name> to play\n"
        "!play <query or URL> - Play YouTube or reply to audio to play local\n"
        "!help - Show this message\n"
    )

# react controller (posts inline controller)
@user_app.on_message(filters.command("react", prefixes=["!", "/"]) & (filters.group | filters.channel) & filters.me)
async def user_post_react_buttons(client: Client, message: Message):
    await ensure_owner_id()
    chat_id = message.chat.id
    owner_id = OWNER_ID
    on_payload = f"react_on_{owner_id}_{chat_id}"
    off_payload = f"react_off_{owner_id}_{chat_id}"
    setradio_payload = f"setradio_{owner_id}_{chat_id}"
    show_payload = f"show_radio_{owner_id}_{chat_id}"
    close_payload = f"close_{owner_id}_{chat_id}"
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🟢 ON", callback_data=on_payload),
                InlineKeyboardButton("🔴 OFF", callback_data=off_payload),
            ],
            [
                InlineKeyboardButton("🔊 Set Radio", callback_data=setradio_payload),
                InlineKeyboardButton("🎧 Show Radio", callback_data=show_payload),
            ],
            [InlineKeyboardButton("Close", callback_data=close_payload)],
        ]
    )
    current = get_react_setting(owner_id, chat_id)
    sent = await message.reply_text(
        f"Auto React Controller\n\nChat: `{message.chat.title or message.chat.id}`\nStatus: `{'ON' if current else 'OFF'}`\n\nOwner-only buttons — handled by your user account.",
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )
    logger.info(f"Posted react controller in chat {chat_id} (msg {sent.message_id})")

# New handler: allow sending "!react on" or "!react off" as direct commands from owner
@user_app.on_message(filters.regex(r"^(?:!|/)react\s+(on|off)$") & filters.me)
async def user_react_onoff_cmd(client: Client, message: Message):
    await ensure_owner_id()
    m = re.match(r"^(?:!|/)react\s+(on|off)$", message.text.strip(), flags=re.I)
    if not m:
        return
    val = m.group(1).lower()
    owner = OWNER_ID
    chat_id = message.chat.id
    if val == "on":
        set_react_setting(owner, chat_id, True)
        await message.reply_text("🟢 Auto React ENABLED for this chat.")
    else:
        set_react_setting(owner, chat_id, False)
        await message.reply_text("🔴 Auto React DISABLED for this chat.")

@user_app.on_callback_query(filters.regex(r"^react_(on|off)_[\d-]+_[\d-]+$"))
async def user_handle_react_toggle(client: Client, cb: CallbackQuery):
    await ensure_owner_id()
    try:
        parts = cb.data.split("_", 3)
        if len(parts) != 4:
            raise ValueError("bad payload")
        _, state, owner_str, chat_str = parts
        owner = int(owner_str); chat_id = int(chat_str)
    except Exception:
        await cb.answer("Invalid payload", show_alert=True)
        return
    if cb.from_user.id != owner:
        await cb.answer("You are not allowed to change this.", show_alert=True)
        return
    enabled = state == "on"
    set_react_setting(owner, chat_id, enabled)
    text = "🟢 Auto React ENABLED!" if enabled else "🔴 Auto React DISABLED!"
    try:
        await cb.message.edit_text(
            f"Auto React Controller\n\nChat: `{cb.message.chat.title or cb.message.chat.id}`\nStatus: `{'ON' if enabled else 'OFF'}`\n\nOwner-only buttons — handled by your user account.",
            reply_markup=cb.message.reply_markup,
        )
    except Exception:
        pass
    try:
        await user_app.send_message(chat_id, f"{text}\n(Changed by @{cb.from_user.username or cb.from_user.first_name})")
        await cb.answer(text + " Notified the chat.")
    except Exception:
        await cb.answer(text + " (Could not post in the chat).")

@user_app.on_callback_query(filters.regex(r"^setradio_[\d-]+_[\d-]+$"))
async def user_handle_setradio_cb(client: Client, cb: CallbackQuery):
    await ensure_owner_id()
    try:
        _, tail = cb.data.split("_", 1)
        owner_str, chat_str = tail.split("_", 1)
        owner = int(owner_str); chat_id = int(chat_str)
    except Exception:
        await cb.answer("Invalid payload", show_alert=True)
        return
    if cb.from_user.id != owner:
        await cb.answer("This button is for the owner only.", show_alert=True)
        return
    try:
        await user_app.send_message(owner, f"Send the radio stream URL (http/https) for chat {chat_id} or send 'cancel'. Waiting 120s.")
    except Exception:
        await cb.answer("Cannot send private message. Check your privacy settings.", show_alert=True)
        return
    await cb.answer("Check your private messages to send the radio URL.", show_alert=False)
    try:
        resp = await user_app.listen(owner, filters=filters.user(owner) & filters.text, timeout=120)
    except asyncio.TimeoutError:
        try:
            await user_app.send_message(owner, "Timed out waiting for URL. Cancelled.")
        except Exception:
            pass
        return
    if not resp or not getattr(resp, "text", None):
        try:
            await user_app.send_message(owner, "No URL received. Cancelled.")
        except Exception:
            pass
        return
    text = resp.text.strip()
    if text.lower() == "cancel":
        await user_app.send_message(owner, "Cancelled.")
        return
    if not (text.startswith("http://") or text.startswith("https://")):
        await user_app.send_message(owner, "That doesn't look like a valid URL. Cancelled.")
        return
    set_radio(owner, chat_id, text)
    await user_app.send_message(owner, "Saved radio URL for this chat.")
    try:
        await user_app.send_message(chat_id, f"🔊 Radio set by @{(resp.from_user.username or resp.from_user.first_name)}. Use !radio to show it.")
    except Exception:
        pass

@user_app.on_callback_query(filters.regex(r"^show_radio_[\d-]+_[\d-]+$"))
async def user_handle_show_radio(client: Client, cb: CallbackQuery):
    await ensure_owner_id()
    try:
        _, owner_str, chat_str = cb.data.split("_", 2)
        owner = int(owner_str); chat_id = int(chat_str)
    except Exception:
        await cb.answer("Invalid payload", show_alert=True)
        return
    if cb.from_user.id != owner:
        await cb.answer("This button is for the owner only.", show_alert=True)
        return
    url = get_radio(owner, chat_id)
    if not url:
        await cb.answer("No radio URL saved for this chat.", show_alert=True)
        return
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Open Stream", url=url)]])
    try:
        await cb.message.reply_text(f"Radio for this chat:\n{url}", reply_markup=keyboard)
        await cb.answer()
    except Exception:
        await cb.answer("Failed to show radio.", show_alert=True)

@user_app.on_callback_query(filters.regex(r"^close_[\d-]+_[\d-]+$"))
async def user_handle_close(client: Client, cb: CallbackQuery):
    await ensure_owner_id()
    try:
        _, owner_str, _ = cb.data.split("_", 2)
        owner = int(owner_str)
    except Exception:
        await cb.answer("Invalid payload", show_alert=True)
        return
    if cb.from_user.id != owner:
        await cb.answer("You are not allowed to close this.", show_alert=True)
        return
    try:
        await cb.message.delete()
        await cb.answer()
    except Exception:
        await cb.answer("Could not delete message.", show_alert=True)

# auto-react
@user_app.on_message((filters.private | filters.group | filters.channel) & filters.incoming & ~filters.reply)
async def auto_react(client: Client, message: Message):
    if getattr(message, "edit_date", None):
        return
    await ensure_owner_id()
    chat_id = message.chat.id
    owner = OWNER_ID
    enabled = react_cache.get((owner, chat_id))
    if enabled is None:
        enabled = get_react_setting(owner, chat_id)
        react_cache[(owner, chat_id)] = enabled
    if not enabled:
        return
    emoji = random.choice(VALID_EMOJIS)
    try:
        await message.react(emoji=emoji)
        logger.info(f"Reacted {emoji} in chat {chat_id} (msg {message.id})")
    except ReactionInvalid:
        pass
    except FloodWait as e:
        logger.warning(f"FloodWait: sleeping {e.value}s")
        await asyncio.sleep(e.value)
    except PeerIdInvalid:
        logger.warning(f"PeerIdInvalid skipped: {chat_id}")
    except Exception:
        logger.exception("React failed")

# setradio command
@user_app.on_message(filters.command("setradio", prefixes=["!", "/"]) & filters.me)
async def user_set_radio(client: Client, message: Message):
    await ensure_owner_id()
    chat_id = message.chat.id
    owner = OWNER_ID
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply_text("Usage: !setradio <url> or !setradio none to unset")
        return
    url = parts[1].strip()
    if url.lower() in ("none", "off", "unset"):
        set_radio(owner, chat_id, None)
        await message.reply_text("Radio URL removed for this chat.")
        return
    if not (url.startswith("http://") or url.startswith("https://")):
        await message.reply_text("Please provide a valid http/https URL.")
        return
    set_radio(owner, chat_id, url)
    await message.reply_text("Saved radio URL. Use !radio to show it.")

# radio command: list stations or play by name: "!radio HiruFM"
@user_app.on_message(filters.command("radio", prefixes=["!", "/"]) & (filters.group | filters.channel | filters.me))
async def cmd_radio_menu(_, message: Message):
    await ensure_owner_id()
    chat_id = message.chat.id
    owner = OWNER_ID
    # If user supplied a station name or URL: play it directly
    parts = message.text.split(maxsplit=1)
    if len(parts) > 1:
        target = parts[1].strip()
        # if user provided a known station name (case-insensitive)
        found = None
        for name in RADIO_STATION:
            if name.lower() == target.lower():
                found = (name, RADIO_STATION[name])
                break
        # if provided a URL directly
        url = None
        title = None
        if found:
            title, url = found
        elif target.startswith("http://") or target.startswith("https://"):
            url = target
            title = target
        else:
            await message.reply_text("Station not found. Use `!radio` to list stations.")
            return

        # Prepare entry and play
        entry = {"title": title or "Radio", "stream_url": url, "webpage": None, "thumbnail": None, "duration": None, "is_local": False}
        if not call_py:
            await message.reply_text(f"▶️ {title}\n{url}")
            return
        # If CALL_CLIENT is assistant, ensure assistant is in chat, otherwise user_app will be used
        if CALL_CLIENT is not None and CALL_CLIENT != user_app:
            try:
                assistant_user = await CALL_CLIENT.get_me()
                assistant_id = assistant_user.id
            except Exception:
                assistant_id = None
            assistant_present = False
            if assistant_id:
                try:
                    await CALL_CLIENT.get_chat_member(chat_id, assistant_id)
                    assistant_present = True
                except Exception:
                    assistant_present = False
            if not assistant_present:
                try:
                    invite = await user_app.create_chat_invite_link(chat_id, member_limit=1, name="dlk_assistant_invite")
                    invite_link = invite.invite_link
                    try:
                        await CALL_CLIENT.join_chat(invite_link)
                        assistant_present = True
                    except Exception:
                        await message.reply_text("Assistant not in the group. Add it with the invite link and retry.")
                        await message.reply_text(invite_link)
                        return
                except Exception:
                    await message.reply_text("Assistant is not in this group. Please add the assistant account and try again.")
                    return
        ok = await play_entry(chat_id, entry, reply_message=message)
        if ok:
            await message.reply_text(f"▶️ Now playing: {entry['title']}")
        else:
            await message.reply_text("❌ Failed to play the requested station.")
        return

    # No arg: list available stations and usage
    lines = ["📻 Radio Stations (use `!radio <name>` to play):"]
    for name in RADIO_STATION:
        lines.append(f"- {name}")
    lines.append("\nOr set a per-chat radio URL with `!setradio <url>`.")
    await message.reply_text("\n".join(lines))

# stations list command (shows station names & URLs)
@user_app.on_message(filters.command("stations", prefixes=["!", "/"]) & filters.me)
async def cmd_stations(client: Client, message: Message):
    lines = ["Available stations:"]
    for name, url in RADIO_STATION.items():
        lines.append(f"- {name}: {url}")
    await message.reply_text("\n".join(lines))

# play command: plays YouTube via call_py (assistant or user account) or local reply audio
@user_app.on_message(filters.command("play", prefixes=["!", "/"]) & (filters.group | filters.channel))
async def cmd_play(_, message: Message):
    chat_id = message.chat.id

    entry = None
    info_msg = None
    if message.reply_to_message:
        entry = await prepare_entry_from_reply(message.reply_to_message)
        if entry:
            info_msg = await message.reply_text("Preparing your audio reply...")
    if not entry:
        query = None
        if len(message.command) > 1:
            query = message.text.split(None, 1)[1]
        elif message.reply_to_message and message.reply_to_message.text:
            query = message.reply_to_message.text
        if not query:
            return await message.reply_text("Usage: /play <YouTube url or search terms> OR reply to an audio/voice file and use /play")
        info_msg = await message.reply_text("🔎 Searching and preparing stream...")
        info = extract_audio_url(query)
        if info is None or not info.get("stream_url"):
            try:
                await info_msg.edit_text("❌ Could not extract audio stream. Ensure yt-dlp is installed.")
            except Exception:
                pass
            return
        entry = {
            "title": info.get("title"),
            "stream_url": info.get("stream_url"),
            "webpage": info.get("webpage_url"),
            "thumbnail": info.get("thumbnail"),
            "duration": info.get("duration"),
            "is_local": False,
        }
    if chat_id not in radio_queue:
        radio_queue[chat_id] = []
    current_state = radio_state.get(chat_id)
    if current_state and not current_state.get("paused"):
        radio_queue[chat_id].append(entry)
        try:
            if info_msg:
                await info_msg.edit_text(f"➕ Added to queue: {entry['title']}")
        except Exception:
            pass
        return

    ok = await play_entry(chat_id, entry, reply_message=message)
    if ok:
        try:
            if info_msg:
                await info_msg.edit_text(f"▶️ Now playing: {entry['title']}")
        except Exception:
            pass
    else:
        try:
            if info_msg:
                await info_msg.edit_text("❌ Failed to play the requested track.")
        except Exception:
            pass

# skip/stop/queue commands
@user_app.on_message(filters.command(["skip", "s"], prefixes=["!", "/"]) & (filters.group | filters.channel))
async def cmd_skip(_, message: Message):
    chat_id = message.chat.id
    if not await dlk_privilege_validator(message):
        return await message.reply_text("Only admins can skip tracks.")
    q = radio_queue.get(chat_id, [])
    if not q:
        await leave_voice_chat(chat_id)
        await message.reply_text("⛔ Skipped. No more tracks in queue.")
        return
    next_entry = q.pop(0)
    radio_queue[chat_id] = q
    if chat_id in track_watchers:
        try:
            track_watchers[chat_id].cancel()
        except Exception:
            pass
        track_watchers.pop(chat_id, None)
    ok = await play_entry(chat_id, next_entry)
    if ok:
        await message.reply_text(f"⏭️ Now playing: {next_entry['title']}")
    else:
        await message.reply_text(f"Failed to play next track: {next_entry.get('title')}")

@user_app.on_message(filters.command(["stop", "end"], prefixes=["!", "/"]) & (filters.group | filters.channel))
async def general_stop_handler(_, message: Message):
    chat_id = message.chat.id
    if not await dlk_privilege_validator(message):
        return await message.reply_text("Only admins can stop the playback!")
    await leave_voice_chat(chat_id)
    await message.reply_text("Stopped & cleaned up.")

# If any old inline callbacks arrive for radio playback, inform user they are disabled.
@user_app.on_callback_query(filters.regex("^radio_play_"))
async def play_radio_station(_, query: CallbackQuery):
    try:
        await query.answer("Inline radio buttons disabled. Use /radio <station-name> or !radio <station-name> to play.", show_alert=True)
    except Exception:
        pass

@user_app.on_callback_query(filters.regex(r"^radio_page_(\d+)$"))
async def cb_radio_page(_, query: CallbackQuery):
    try:
        await query.answer("Pagination disabled. Use /radio to list stations.", show_alert=True)
    except Exception:
        pass

@user_app.on_callback_query(filters.regex(r"^radio_close$"))
async def cb_radio_close(_, query: CallbackQuery):
    try:
        try:
            await query.message.delete()
        except Exception:
            try:
                await query.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
        await query.answer()
    except Exception as e:
        logger.debug(f"radio_close handler failed: {e}")
        try:
            await query.answer("Failed to close menu.", show_alert=True)
        except Exception:
            pass

# playback controls callbacks adjusted to still work if triggered (pause/resume/stop/skip)
@user_app.on_callback_query(filters.regex("^music_skip$"))
async def cb_music_skip(_, query: CallbackQuery):
    if not await dlk_privilege_validator(query):
        return await query.answer("Only admins can skip tracks.", show_alert=True)
    chat_id = query.message.chat.id
    q = radio_queue.get(chat_id, [])
    if not q:
        await leave_voice_chat(chat_id)
        try:
            await query.message.edit_caption(caption="⛔ Skipped. No more tracks in queue.", reply_markup=None)
        except Exception:
            pass
        await query.answer("Skipped. No queue.", show_alert=True)
        return
    next_entry = q.pop(0)
    radio_queue[chat_id] = q
    if chat_id in track_watchers:
        try:
            track_watchers[chat_id].cancel()
        except Exception:
            pass
        track_watchers.pop(chat_id, None)
    ok = await play_entry(chat_id, next_entry)
    if ok:
        await query.answer(f"⏭️ Now: {next_entry['title']}", show_alert=False)
    else:
        await query.answer("Failed to skip to next track.", show_alert=True)

@user_app.on_callback_query(filters.regex("^radio_pause$"))
async def radio_pause_cb(_, query: CallbackQuery):
    if not await dlk_privilege_validator(query):
        return await query.answer("Only admins can pause the radio!", show_alert=True)
    chat_id = query.message.chat.id
    state = radio_state.get(chat_id)
    if not state:
        return await query.answer("Nothing is playing.", show_alert=True)
    try:
        await _safe_call_py_method("pause_stream", chat_id)
        await _safe_call_py_method("pause", chat_id)
        start_time = state.get("start_time") or time.time()
        elapsed = time.time() - start_time if start_time else state.get("elapsed", 0.0)
        state["paused"] = True
        state["elapsed"] = elapsed
        state["start_time"] = None
        radio_paused.add(chat_id)
        store_play_state(chat_id, state.get("station"), state.get("url"), state.get("msg_id"), None, elapsed=elapsed, paused=True)
        try:
            await query.message.edit_reply_markup(reply_markup=player_controls_markup(chat_id))
        except Exception:
            pass
        await query.answer("Paused.", show_alert=False)
    except Exception as e:
        logger.debug(f"Pause failed: {e}")
        await query.answer("Failed to pause the stream.", show_alert=True)

@user_app.on_callback_query(filters.regex("^radio_resume$"))
async def radio_resume_cb(_, query: CallbackQuery):
    if not await dlk_privilege_validator(query):
        return await query.answer("Only admins can resume the bot!", show_alert=True)
    chat_id = query.message.chat.id
    state = radio_state.get(chat_id)
    if not state:
        return await query.answer("Nothing to resume.", show_alert=True)
    try:
        await _safe_call_py_method("resume_stream", chat_id)
        await _safe_call_py_method("resume", chat_id)
        elapsed = state.get("elapsed", 0.0) or 0.0
        start_time = time.time() - elapsed
        state["paused"] = False
        state["elapsed"] = 0.0
        state["start_time"] = start_time
        radio_paused.discard(chat_id)
        store_play_state(chat_id, state.get("station"), state.get("url"), state.get("msg_id"), start_time, elapsed=0.0, paused=False)
        if chat_id in radio_tasks:
            try:
                radio_tasks[chat_id].cancel()
            except Exception:
                pass
            radio_tasks.pop(chat_id, None)
        radio_tasks[chat_id] = asyncio.create_task(update_radio_timer(chat_id, state.get("msg_id"), state.get("station"), start_time))
        try:
            await query.message.edit_reply_markup(reply_markup=player_controls_markup(chat_id))
        except Exception:
            pass
        await query.answer("Resumed.", show_alert=False)
    except Exception as e:
        logger.debug(f"Resume failed: {e}")
        await query.answer("Failed to resume the stream.", show_alert=True)

@user_app.on_callback_query(filters.regex("^radio_stop$"))
async def cb_radio_stop(_, query: CallbackQuery):
    if not await dlk_privilege_validator(query):
        return await query.answer("Only admins can stop the radio!", show_alert=True)
    chat_id = query.message.chat.id
    try:
        await leave_voice_chat(chat_id)
        try:
            await query.message.delete()
        except Exception:
            pass
        await query.answer("Stopped!", show_alert=False)
    except Exception as e:
        logger.error(f"Stop failed via callback: {e}", exc_info=True)
        await query.answer("Failed to stop bot.", show_alert=True)

# ===================== START/STOP helpers =====================
async def start_all():
    await user_app.start()
    if not SESSION_STRING:
        try:
            session_str = await user_app.export_session_string()
            logger.info("New session string generated. Please save it for future runs.")
            logger.info(session_str)
        except Exception:
            pass
    if assistant:
        await assistant.start()
    if call_py:
        try:
            call_py.start()
        except Exception:
            logger.exception("Failed to start PyTgCalls")
    await ensure_owner_id()
    me = await user_app.get_me()
    logger.info(f"Userbot started as @{me.username or me.first_name} ({me.id})")
    if assistant:
        try:
            a = await assistant.get_me()
            logger.info(f"Assistant started as @{a.username} ({a.id})")
        except Exception:
            logger.info("Assistant started (username unknown).")
    if CALL_CLIENT is user_app:
        logger.info("Using userbot account for voice (PyTgCalls attached to user_app).")

async def stop_all():
    try:
        if call_py:
            call_py.stop()
    except Exception:
        pass
    try:
        if assistant:
            await assistant.stop()
    except Exception:
        pass
    try:
        await user_app.stop()
    except Exception:
        pass

def run():
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(start_all())
        from pyrogram import idle
        idle()
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down...")
    except Exception:
        logger.exception("Fatal error during run")
    finally:
        try:
            loop.run_until_complete(stop_all())
        except Exception:
            pass

if __name__ == "__main__":
    run()
