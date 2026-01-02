#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Add logging to _download_hls_with_ffmpeg method"""

from pathlib import Path

file_path = Path(__file__).parent / "media_downloader.py"
lines = file_path.read_text(encoding='utf-8').splitlines(keepends=True)

# Find the method start
start_idx = None
for i, line in enumerate(lines):
    if 'def _download_hls_with_ffmpeg' in line:
        start_idx = i
        break

if not start_idx:
    print("ERROR: Method not found")
    exit(1)

print(f"Found method at line {start_idx+1}")

# Find where to insert logs
# After method signature (line after docstring)
docstring_end = None
for i in range(start_idx + 1, min(start_idx + 5, len(lines))):
    if '"""' in lines[i]:
        # Find closing docstring
        for j in range(i + 1, min(i + 3, len(lines))):
            if '"""' in lines[j]:
                docstring_end = j
                break
        break

if docstring_end is None:
    print("ERROR: Could not find docstring end")
    exit(1)

# Find ffmpeg = shutil.which line
ffmpeg_check_idx = None
for i in range(docstring_end + 1, min(docstring_end + 5, len(lines))):
    if 'ffmpeg = shutil.which' in lines[i]:
        ffmpeg_check_idx = i
        break

if ffmpeg_check_idx is None:
    print("ERROR: Could not find ffmpeg check line")
    exit(1)

# Find "if not ffmpeg:" line
if_not_ffmpeg_idx = None
for i in range(ffmpeg_check_idx + 1, min(ffmpeg_check_idx + 3, len(lines))):
    if 'if not ffmpeg:' in lines[i]:
        if_not_ffmpeg_idx = i
        break

if if_not_ffmpeg_idx is None:
    print("ERROR: Could not find 'if not ffmpeg:' line")
    exit(1)

# Find return None after warning
return_none_idx = None
for i in range(if_not_ffmpeg_idx + 1, min(if_not_ffmpeg_idx + 5, len(lines))):
    if 'return None' in lines[i] and 'if not ffmpeg' not in lines[i-1]:
        return_none_idx = i
        break

# Find subprocess.run line
subprocess_idx = None
for i in range(start_idx, min(start_idx + 50, len(lines))):
    if 'subprocess.run(cmd, check=True)' in lines[i] and 'result =' not in lines[i]:
        subprocess_idx = i
        break

# Build new lines
new_lines = lines[:docstring_end + 1]

# Add enter log after docstring
new_lines.append('        # region agent log\n')
new_lines.append('        _agent_log("H1", "media_downloader.py:_download_hls_with_ffmpeg", "enter", {"tweet_id": tweet_id, "m3u8_url": _safe_url_tag(m3u8_url), "referer": _safe_url_tag(referer)})\n')
new_lines.append('        # endregion\n')
new_lines.append('\n')

# Keep existing ffmpeg check, but add log after it
new_lines.extend(lines[ffmpeg_check_idx:ffmpeg_check_idx+1])
new_lines.append('        # region agent log\n')
new_lines.append('        _agent_log("H1", "media_downloader.py:_download_hls_with_ffmpeg", "ffmpeg_check", {"ffmpeg_path": ffmpeg or "NOT_FOUND", "tweet_id": tweet_id})\n')
new_lines.append('        # endregion\n')
new_lines.append('\n')

# Keep if not ffmpeg and warning, add log before return
new_lines.extend(lines[if_not_ffmpeg_idx:return_none_idx])
new_lines.append('            # region agent log\n')
new_lines.append('            _agent_log("H1", "media_downloader.py:_download_hls_with_ffmpeg", "ffmpeg_not_found", {"tweet_id": tweet_id, "m3u8_url": _safe_url_tag(m3u8_url)})\n')
new_lines.append('            # endregion\n')
new_lines.append(lines[return_none_idx])

# Add remaining lines until subprocess.run
if subprocess_idx:
    new_lines.extend(lines[return_none_idx + 1:subprocess_idx])
    
    # Replace subprocess.run with logged version
    new_lines.append('            # region agent log\n')
    new_lines.append('            _agent_log("H5", "media_downloader.py:_download_hls_with_ffmpeg", "subprocess_start", {"tweet_id": tweet_id, "m3u8_url": _safe_url_tag(m3u8_url)})\n')
    new_lines.append('            # endregion\n')
    new_lines.append('            \n')
    new_lines.append('            result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=300)\n')
    new_lines.append('            \n')
    new_lines.append('            # region agent log\n')
    new_lines.append('            _agent_log("H5", "media_downloader.py:_download_hls_with_ffmpeg", "subprocess_success", {"tweet_id": tweet_id, "returncode": result.returncode, "save_path": str(save_path), "file_exists": save_path.exists() if save_path.exists() else False})\n')
    new_lines.append('            # endregion\n')
    new_lines.append('            \n')
    new_lines.append('            if save_path.exists() and save_path.stat().st_size > 0:\n')
    new_lines.append('                return save_path\n')
    new_lines.append('            else:\n')
    new_lines.append('                # region agent log\n')
    new_lines.append('                _agent_log("H5", "media_downloader.py:_download_hls_with_ffmpeg", "file_not_created", {"tweet_id": tweet_id, "save_path": str(save_path)})\n')
    new_lines.append('                # endregion\n')
    new_lines.append('                logger.error(f"ffmpeg実行後、ファイルが作成されませんでした: {save_path}")\n')
    new_lines.append('                return None\n')
    
    # Find except blocks and add logging
    except_idx = None
    for i in range(subprocess_idx + 1, min(subprocess_idx + 20, len(lines))):
        if 'except Exception as e:' in lines[i]:
            except_idx = i
            break
    
    if except_idx:
        # Replace Exception handler with specific handlers
        new_lines.append('        except subprocess.TimeoutExpired as e:\n')
        new_lines.append('            # region agent log\n')
        new_lines.append('            _agent_log("H5", "media_downloader.py:_download_hls_with_ffmpeg", "subprocess_timeout", {"tweet_id": tweet_id, "error": str(e)})\n')
        new_lines.append('            # endregion\n')
        new_lines.append('            logger.error(f"ffmpegでのm3u8保存がタイムアウトしました: {e}")\n')
        
        # Find the cleanup code and reuse it
        cleanup_start = None
        for i in range(except_idx + 1, min(except_idx + 10, len(lines))):
            if 'if save_path.exists()' in lines[i]:
                cleanup_start = i
                break
        
        if cleanup_start:
            new_lines.extend(lines[cleanup_start:cleanup_start+5])
            new_lines.append('            return None\n')
            
        new_lines.append('        except subprocess.CalledProcessError as e:\n')
        new_lines.append('            # region agent log\n')
        new_lines.append('            _agent_log("H5", "media_downloader.py:_download_hls_with_ffmpeg", "subprocess_error", {"tweet_id": tweet_id, "returncode": e.returncode, "stderr": (e.stderr or "")[:500], "stdout": (e.stdout or "")[:500]})\n')
        new_lines.append('            # endregion\n')
        new_lines.append('            logger.error(f"ffmpegでのm3u8保存に失敗 (returncode={e.returncode}): {e.stderr or e.stdout or str(e)}")\n')
        if cleanup_start:
            new_lines.extend(lines[cleanup_start:cleanup_start+5])
            new_lines.append('            return None\n')
            
        new_lines.append('        except Exception as e:\n')
        new_lines.append('            # region agent log\n')
        new_lines.append('            _agent_log("H5", "media_downloader.py:_download_hls_with_ffmpeg", "exception", {"tweet_id": tweet_id, "error_type": type(e).__name__, "error": str(e)[:500]})\n')
        new_lines.append('            # endregion\n')
        new_lines.append('            logger.error(f"ffmpegでのm3u8保存に失敗: {e}")\n')
        if cleanup_start:
            new_lines.extend(lines[cleanup_start:cleanup_start+5])
            new_lines.append('            return None\n')
        
        # Add remaining lines after the method
        next_method_idx = None
        for i in range(except_idx, min(except_idx + 30, len(lines))):
            if i > except_idx and lines[i].strip().startswith('def ') and '    def ' in lines[i]:
                next_method_idx = i
                break
        
        if next_method_idx:
            new_lines.extend(lines[next_method_idx:])
        else:
            # Fallback: find next method by indentation
            for i in range(except_idx + 20, len(lines)):
                if lines[i].strip().startswith('def ') and not lines[i].strip().startswith('def _download_hls'):
                    new_lines.extend(lines[i:])
                    break
    else:
        # Fallback: just add the lines after subprocess_idx
        new_lines.extend(lines[subprocess_idx + 3:])
else:
    # Fallback: add all remaining lines
    new_lines.extend(lines[return_none_idx + 1:])

# Write back
file_path.write_text(''.join(new_lines), encoding='utf-8')
print("Updated _download_hls_with_ffmpeg method with logging")




