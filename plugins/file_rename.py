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

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

renaming_operations = {}

# Compile regex patterns once at module level
EPISODE_PATTERNS = [
    (re.compile(r'S(\d+)(?:E|EP)(\d+)'), 2),  # S01E02 or S01EP02
    (re.compile(r'S(\d+)\s*(?:E|EP|-\s*EP)(\d+)'), 2),  # S01 E02 or S01 EP02
    (re.compile(r'(?:[([<{]?\s*(?:E|EP)\s*(\d+)\s*[)\]>}]?)'), 1),  # EP01 or [EP01]
    (re.compile(r'(?:\s*-\s*(\d+)\s*)'), 1),  # - 01 -
    (re.compile(r'S(\d+)[^\d]*(\d+)', re.IGNORECASE), 2),  # S2 09
    (re.compile(r'(\d+)'), 1)  # Standalone number
]

QUALITY_PATTERNS = [
    (re.compile(r'\b(?:.*?(\d{3,4}[^\dp]*p).*?|.*?(\d{3,4}p))\b', re.IGNORECASE), lambda m: m.group(1) or m.group(2)),
    (re.compile(r'[([<{]?\s*4k\s*[)\]>}]?', re.IGNORECASE), lambda _: "4k"),
    (re.compile(r'[([<{]?\s*2k\s*[)\]>}]?', re.IGNORECASE), lambda _: "2k"),
    (re.compile(r'[([<{]?\s*HdRip\s*[)\]>}]?|\bHdRip\b', re.IGNORECASE), lambda _: "HdRip"),
    (re.compile(r'[([<{]?\s*4kX264\s*[)]>}]?', re.IGNORECASE), lambda _: "4kX264"),
    (re.compile(r'[([<{]?\s*4kx265\s*[)]>}]?', re.IGNORECASE), lambda _: "4kx265"),
    (re.compile(r'(?:720|1080|2160)[pi]?', re.IGNORECASE), lambda m: m.group(0))  # Basic resolution detection
]

def extract_quality(filename):
    """Extract quality information from filename using predefined patterns."""
    for pattern, extractor in QUALITY_PATTERNS:
        match = pattern.search(filename)
        if match:
            quality = extractor(match)
            logger.info(f"Extracted quality: {quality} from {filename}")
            return quality
    logger.warning(f"No quality pattern matched for {filename}")
    return "Unknown"

def extract_episode_number(filename):
    """Extract episode number from filename using predefined patterns."""
    for pattern, group_index in EPISODE_PATTERNS:
        match = pattern.search(filename)
        if match:
            episode = match.group(group_index)
            logger.info(f"Extracted episode: {episode} from {filename}")
            return episode
    logger.warning(f"No episode pattern matched for {filename}")
    return None

async def cleanup_files(*paths):
    """Safely remove multiple files if they exist."""
    for path in paths:
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except Exception as e:
            logger.error(f"Error removing file {path}: {e}")

async def process_thumbnail(ph_path):
    """Process and resize thumbnail if it exists."""
    if not ph_path or not os.path.exists(ph_path):
        return None
    
    try:
        img = Image.open(ph_path).convert("RGB")
        img = img.resize((320, 320))
        img.save(ph_path, "JPEG")
        return ph_path
    except Exception as e:
        logger.error(f"Thumbnail processing error: {e}")
        await cleanup_files(ph_path)
        return None

async def add_metadata(input_path, output_path, user_id):
    """Add metadata to media file using ffmpeg."""
    ffmpeg_cmd = shutil.which('ffmpeg')
    if not ffmpeg_cmd:
        raise RuntimeError("FFmpeg not found in PATH")
    
    metadata_command = [
        ffmpeg_cmd,
        '-i', input_path,
        '-metadata', f'title={await codeflixbots.get_title(user_id)}',
        '-metadata', f'artist={await codeflixbots.get_artist(user_id)}',
        '-metadata', f'author={await codeflixbots.get_author(user_id)}',
        '-metadata:s:v', f'title={await codeflixbots.get_video(user_id)}',
        '-metadata:s:a', f'title={await codeflixbots.get_audio(user_id)}',
        '-metadata:s:s', f'title={await codeflixbots.get_subtitle(user_id)}',
        '-map', '0',
        '-c', 'copy',
        '-loglevel', 'error',
        output_path
    ]
    
    process = await asyncio.create_subprocess_exec(
        *metadata_command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()
    
    if process.returncode != 0:
        error_message = stderr.decode()
        raise RuntimeError(f"FFmpeg error: {error_message}")

@Client.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def auto_rename_files(client, message):
    user_id = message.from_user.id
    format_template = await codeflixbots.get_format_template(user_id)
    media_preference = await codeflixbots.get_media_preference(user_id)
    
    if not format_template:
        return await message.reply_text("Please set an auto rename format first using /autorename")

    # Determine file type and get basic info
    if message.document:
        file_id = message.document.file_id
        file_name = message.document.file_name
        media_type = media_preference or "document"
        file_size = message.document.file_size
    elif message.video:
        file_id = message.video.file_id
        file_name = f"{message.video.file_name}.mp4" if message.video.file_name else "video.mp4"
        media_type = media_preference or "video"
        file_size = message.video.file_size
    elif message.audio:
        file_id = message.audio.file_id
        file_name = f"{message.audio.file_name}.mp3" if message.audio.file_name else "audio.mp3"
        media_type = media_preference or "audio"
        file_size = message.audio.file_size
    else:
        return await message.reply_text("Unsupported file type")

    # Anti-NSFW check
    if await check_anti_nsfw(file_name, message):
        return await message.reply_text("NSFW content detected. File upload rejected.")

    # Check for duplicate processing
    if file_id in renaming_operations:
        elapsed_time = (datetime.now() - renaming_operations[file_id]).seconds
        if elapsed_time < 10:
            return
    renaming_operations[file_id] = datetime.now()

    try:
        # Process filename template
        episode_number = extract_episode_number(file_name)
        if episode_number:
            for placeholder in ["episode", "Episode", "EPISODE", "{episode}"]:
                format_template = format_template.replace(placeholder, str(episode_number), 1)

        # Process quality in template
        for placeholder in ["quality", "Quality", "QUALITY", "{quality}"]:
            if placeholder in format_template:
                quality = extract_quality(file_name)
                format_template = format_template.replace(placeholder, quality)

        # Prepare file paths
        _, file_extension = os.path.splitext(file_name)
        renamed_file_name = f"{format_template}{file_extension}"
        renamed_file_path = f"downloads/{renamed_file_name}"
        metadata_file_path = f"Metadata/{renamed_file_name}"
        
        os.makedirs(os.path.dirname(renamed_file_path), exist_ok=True)
        os.makedirs(os.path.dirname(metadata_file_path), exist_ok=True)

        # Download file
        download_msg = await message.reply_text("**__Downloading...__**")
        try:
            path = await client.download_media(
                message,
                file_name=renamed_file_path,
                progress=progress_for_pyrogram,
                progress_args=("Download Started...", download_msg, time.time()),
            )
        except Exception as e:
            await download_msg.edit(f"**Download Error:** {e}")
            raise

        await download_msg.edit("**__Processing file...__**")

        # Process metadata
        try:
            await add_metadata(path, metadata_file_path, user_id)
            path = metadata_file_path  # Use the metadata-processed file for upload
        except Exception as e:
            await download_msg.edit(f"**Metadata Error:** {e}")
            raise

        # Prepare for upload
        upload_msg = await download_msg.edit("**__Uploading...__**")
        c_caption = await codeflixbots.get_caption(message.chat.id)
        c_thumb = await codeflixbots.get_thumbnail(message.chat.id)

        caption = (
            c_caption.format(
                filename=renamed_file_name,
                filesize=humanbytes(file_size),
                duration=convert(0),
            ) if c_caption else f"**{renamed_file_name}**"
        )

        # Handle thumbnail
        ph_path = None
        if c_thumb:
            ph_path = await client.download_media(c_thumb)
        elif media_type == "video" and message.video.thumbs:
            ph_path = await client.download_media(message.video.thumbs[0].file_id)
        
        ph_path = await process_thumbnail(ph_path)

        # Upload file
        try:
            upload_params = {
                "chat_id": message.chat.id,
                "caption": caption,
                "thumb": ph_path,
                "progress": progress_for_pyrogram,
                "progress_args": ("Upload Started...", upload_msg, time.time())
            }

            if media_type == "document":
                await client.send_document(document=path, **upload_params)
            elif media_type == "video":
                await client.send_video(video=path, duration=0, **upload_params)
            elif media_type == "audio":
                await client.send_audio(audio=path, duration=0, **upload_params)

            await upload_msg.delete()

        except Exception as e:
            await upload_msg.edit(f"**Upload Error:** {e}")
            raise

    except Exception as e:
        logger.error(f"Error processing file: {e}")
        await message.reply_text(f"An error occurred: {str(e)}")

    finally:
        # Cleanup in all cases
        await cleanup_files(
            renamed_file_path,
            metadata_file_path,
            ph_path
        )
        if file_id in renaming_operations:
            del renaming_operations[file_id]
