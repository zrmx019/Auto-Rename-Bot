import os
import re
import time
import shutil
import asyncio
import logging
from datetime import datetime
from PIL import Image
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import InputMediaDocument, Message
from hachoir.metadata import extractMetadata
from hachoir.parser import createParser
from plugins.antinsfw import check_anti_nsfw
from helper.utils import progress_for_pyrogram, humanbytes, convert
from helper.database import codeflixbots
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global dictionary to track ongoing operations
renaming_operations = {}

# Enhanced regex patterns for season and episode extraction
SEASON_EPISODE_PATTERNS = [
    re.compile(r'S(\d+)(?:E|EP)(\d+)'),  # S01E02, S01EP02
    re.compile(r'S(\d+)[\s-]*(?:E|EP)(\d+)'),  # S01 E02, S01-EP02
    re.compile(r'Season\s*(\d+)\s*Episode\s*(\d+)', re.IGNORECASE),  # Season 1 Episode 2
    re.compile(r'\[S(\d+)\]\[E(\d+)\]'),  # [S01][E02]
    re.compile(r'S(\d+)[^\d]*(\d+)'),  # S01 13
    re.compile(r'(?:E|EP|Episode)\s*(\d+)', re.IGNORECASE),  # EP02, Episode 2
    re.compile(r'\b(\d+)\b')  # Standalone number
]

# Quality detection patterns
QUALITY_PATTERNS = [
    (re.compile(r'\b(\d{3,4}[pi])\b', re.IGNORECASE),  # 1080p, 720p
    (re.compile(r'\b(4k|2160p)\b', re.IGNORECASE),  # 4k
    (re.compile(r'\b(2k|1440p)\b', re.IGNORECASE),  # 2k
    (re.compile(r'\b(HDRip|HDTV)\b', re.IGNORECASE),  # HDRip, HDTV
    (re.compile(r'\b(4kX264|4kx265)\b', re.IGNORECASE),  # 4kX264, 4kx265
    (re.compile(r'\[(\d{3,4}[pi])\]', re.IGNORECASE))  # [1080p]
]

# Audio language patterns
AUDIO_PATTERNS = [
    (re.compile(r'\b(Multi|Dual)[-\s]?Audio\b', re.IGNORECASE),  # Multi-Audio, DualAudio
    (re.compile(r'\b(Dual)[-\s]?(Audio|Track)\b', re.IGNORECASE),  # Dual-Audio, DualTrack
    (re.compile(r'\b(Sub(bed)?)\b', re.IGNORECASE),  # Sub, Subbed
    (re.compile(r'\b(Dub(bed)?)\b', re.IGNORECASE),  # Dub, Dubbed
    (re.compile(r'\[(Sub|Dub)\]'),  # [Sub], [Dub]
    (re.compile(r'\((Sub|Dub)\)'),  # (Sub), (Dub)
    (re.compile(r'\b(Eng(lish)?\s*/\s*(Jap|Kor|Chi))\b', re.IGNORECASE),  # English/Japanese
    (re.compile(r'\b(TrueHD|DTS[- ]?HD|Atmos)\b'),  # TrueHD, DTS-HD
    (re.compile(r'\[(Unknown)\]'))  # [Unknown]
]

def extract_season_episode(filename):
    """Extract season and episode numbers from filename"""
    for pattern in SEASON_EPISODE_PATTERNS:
        match = pattern.search(filename)
        if match:
            groups = match.groups()
            # Handle different pattern cases
            if len(groups) >= 2:
                season, episode = groups[0], groups[1]
            elif len(groups) == 1:
                # For patterns that only match episode number
                season, episode = None, groups[0]
            else:
                season, episode = None, None
                
            logger.info(f"Extracted season: {season}, episode: {episode} from {filename}")
            return season, episode
            
    logger.warning(f"No season/episode pattern matched for {filename}")
    return None, None

def extract_quality(filename):
    """Extract quality information from filename"""
    for pattern in QUALITY_PATTERNS:
        match = pattern[0].search(filename)
        if match:
            if len(pattern) > 1:
                quality = pattern[1](match) if callable(pattern[1]) else pattern[1]
            else:
                quality = match.group(1)
            logger.info(f"Extracted quality: {quality} from {filename}")
            return quality
    logger.warning(f"No quality pattern matched for {filename}")
    return "Unknown"

def extract_audio_info(filename):
    """Extract audio/language information from filename"""
    for pattern in AUDIO_PATTERNS:
        match = pattern[0].search(filename)
        if match:
            if len(pattern) > 1:
                audio_info = pattern[1](match) if callable(pattern[1]) else pattern[1]
            else:
                audio_info = match.group(1)
            logger.info(f"Extracted audio info: {audio_info} from {filename}")
            return audio_info
    logger.info(f"No audio pattern matched for {filename}")
    return None

async def cleanup_files(*paths):
    """Safely remove files if they exist"""
    for path in paths:
        try:
            if path and os.path.exists(path):
                if os.path.isfile(path):
                    os.remove(path)
                elif os.path.isdir(path):
                    shutil.rmtree(path)
        except Exception as e:
            logger.error(f"Error removing {path}: {e}")

async def process_thumbnail(thumb_path):
    """Process and resize thumbnail image"""
    if not thumb_path or not os.path.exists(thumb_path):
        return None
    
    try:
        with Image.open(thumb_path) as img:
            img = img.convert("RGB").resize((320, 320))
            processed_path = f"{thumb_path}_processed.jpg"
            img.save(processed_path, "JPEG")
        return processed_path
    except Exception as e:
        logger.error(f"Thumbnail processing failed: {e}")
        await cleanup_files(thumb_path)
        return None

async def add_metadata(input_path, output_path, user_id):
    """Add metadata to media file using ffmpeg"""
    ffmpeg = shutil.which('ffmpeg')
    if not ffmpeg:
        raise RuntimeError("FFmpeg not found in PATH")
    
    metadata = {
        'title': await codeflixbots.get_title(user_id) or "",
        'artist': await codeflixbots.get_artist(user_id) or "",
        'author': await codeflixbots.get_author(user_id) or "",
        'video_title': await codeflixbots.get_video(user_id) or "",
        'audio_title': await codeflixbots.get_audio(user_id) or "",
        'subtitle': await codeflixbots.get_subtitle(user_id) or ""
    }
    
    cmd = [
        ffmpeg,
        '-i', input_path,
        '-metadata', f'title={metadata["title"]}',
        '-metadata', f'artist={metadata["artist"]}',
        '-metadata', f'author={metadata["author"]}',
        '-metadata:s:v', f'title={metadata["video_title"]}',
        '-metadata:s:a', f'title={metadata["audio_title"]}',
        '-metadata:s:s', f'title={metadata["subtitle"]}',
        '-map', '0',
        '-c', 'copy',
        '-loglevel', 'error',
        output_path
    ]
    
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await asyncio.wait_for(process.communicate(), timeout=300)
        
        if process.returncode != 0:
            raise RuntimeError(f"FFmpeg error: {stderr.decode()}")
    except asyncio.TimeoutError:
        process.kill()
        raise RuntimeError("FFmpeg processing timed out")

@Client.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def auto_rename_files(client, message):
    """Main handler for auto-renaming files"""
    user_id = message.from_user.id
    try:
        format_template = await codeflixbots.get_format_template(user_id)
    except Exception as e:
        logger.error(f"Database error: {e}")
        return await message.reply_text("Error accessing database. Please try again later.")
    
    if not format_template:
        return await message.reply_text("Please set a rename format using /autorename")

    # Get file information
    try:
        if message.document:
            file_id = message.document.file_id
            file_name = message.document.file_name
            file_size = message.document.file_size
            media_type = "document"
        elif message.video:
            file_id = message.video.file_id
            file_name = message.video.file_name or "video"
            file_size = message.video.file_size
            media_type = "video"
        elif message.audio:
            file_id = message.audio.file_id
            file_name = message.audio.file_name or "audio"
            file_size = message.audio.file_size
            media_type = "audio"
        else:
            return await message.reply_text("Unsupported file type")
    except Exception as e:
        logger.error(f"Error getting file info: {e}")
        return await message.reply_text("Error processing file information")

    # NSFW check
    try:
        if await check_anti_nsfw(file_name, message):
            return await message.reply_text("NSFW content detected")
    except Exception as e:
        logger.error(f"NSFW check failed: {e}")
        return await message.reply_text("Error during content check")

    # Prevent duplicate processing
    current_time = datetime.now()
    if file_id in renaming_operations:
        if (current_time - renaming_operations[file_id]).seconds < 10:
            return
    renaming_operations[file_id] = current_time

    download_path = None
    metadata_path = None
    thumb_path = None
    msg = None

    try:
        # Extract metadata from filename
        season, episode = extract_season_episode(file_name)
        quality = extract_quality(file_name)
        audio_info = extract_audio_info(file_name)
        
        # Replace placeholders in template
        replacements = {
            '{season}': season or 'XX',
            '{episode}': episode or 'XX',
            '{quality}': quality,
            '{audio}': audio_info or 'Unknown',
            'Season': season or 'XX',
            'Episode': episode or 'XX',
            'QUALITY': quality,
            'AUDIO': audio_info or 'Unknown'
        }
        
        # Handle all case variations of placeholders
        for placeholder, value in replacements.items():
            format_template = re.sub(
                re.escape(placeholder),
                value,
                format_template,
                flags=re.IGNORECASE
            )

        # Prepare file paths
        ext = os.path.splitext(file_name)[1] or ('.mp4' if media_type == 'video' else '.mp3')
        new_filename = f"{format_template}{ext}"
        download_path = os.path.join("downloads", new_filename)
        metadata_path = os.path.join("metadata", new_filename)
        
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        os.makedirs(os.path.dirname(metadata_path), exist_ok=True)

        # Download file
        msg = await message.reply_text("**Downloading...**")
        try:
            file_path = await client.download_media(
                message,
                file_name=download_path,
                progress=progress_for_pyrogram,
                progress_args=("Downloading...", msg, time.time())
            )
        except FloodWait as e:
            await asyncio.sleep(e.value)
            file_path = await client.download_media(
                message,
                file_name=download_path,
                progress=progress_for_pyrogram,
                progress_args=("Downloading...", msg, time.time())
            )
        except Exception as e:
            await msg.edit(f"Download failed: {e}")
            raise

        # Process metadata
        await msg.edit("**Processing metadata...**")
        try:
            await add_metadata(file_path, metadata_path, user_id)
            file_path = metadata_path
        except Exception as e:
            await msg.edit(f"Metadata processing failed: {e}")
            raise

        # Prepare for upload
        await msg.edit("**Preparing upload...**")
        try:
            caption = await codeflixbots.get_caption(message.chat.id) or f"**{new_filename}**"
            thumb = await codeflixbots.get_thumbnail(message.chat.id)
            thumb_path = None

            # Handle thumbnail
            if thumb:
                thumb_path = await client.download_media(thumb)
            elif media_type == "video" and message.video.thumbs:
                thumb_path = await client.download_media(message.video.thumbs[0].file_id)
            
            thumb_path = await process_thumbnail(thumb_path)

            # Upload file
            await msg.edit("**Uploading...**")
            upload_params = {
                'chat_id': message.chat.id,
                'caption': caption,
                'thumb': thumb_path,
                'progress': progress_for_pyrogram,
                'progress_args': ("Uploading...", msg, time.time())
            }

            try:
                if media_type == "document":
                    await client.send_document(document=file_path, **upload_params)
                elif media_type == "video":
                    await client.send_video(video=file_path, **upload_params)
                elif media_type == "audio":
                    await client.send_audio(audio=file_path, **upload_params)
            except FloodWait as e:
                await asyncio.sleep(e.value)
                if media_type == "document":
                    await client.send_document(document=file_path, **upload_params)
                elif media_type == "video":
                    await client.send_video(video=file_path, **upload_params)
                elif media_type == "audio":
                    await client.send_audio(audio=file_path, **upload_params)

            if msg:
                await msg.delete()
        except Exception as e:
            if msg:
                await msg.edit(f"Upload failed: {e}")
            raise

    except Exception as e:
        logger.error(f"Processing error: {e}", exc_info=True)
        if msg:
            await msg.edit(f"Error: {str(e)}")
        else:
            await message.reply_text(f"Error: {str(e)}")
    finally:
        # Clean up files
        await cleanup_files(download_path, metadata_path, thumb_path)
        if file_id in renaming_operations:
            renaming_operations.pop(file_id)
