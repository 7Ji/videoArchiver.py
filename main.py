import threading
import subprocess
import json
import re
import pathlib
import datetime
import shutil
import psutil
import time
import pickle


dir_raw = pathlib.Path('raw')
dir_archive = pathlib.Path('archive')
dir_preview = pathlib.Path('preview')
dir_work = pathlib.Path('work')
codec_vlo = ('012v', '8bps', 'aasc', 'alias_pix', 'apng', 'avrp', 'avui', 'ayuv', 'bitpacked', 'bmp', 'bmv_video', 'brender_pix', 'cdtoons', 'cllc', 'cscd', 'dpx', 'dxa', 'dxtory', 'ffv1', 'ffvhuff', 'fits', 'flashsv', 'flic', 'fmvc', 'fraps', 'frwu', 'gif', 'huffyuv', 'hymt', 'lagarith', 'ljpeg', 'loco', 'm101', 'magicyuv', 'mscc', 'msp2', 'msrle', 'mszh', 'mwsc', 'pam', 'pbm', 'pcx', 'pfm', 'pgm', 'pgmyuv', 'pgx', 'png', 'ppm', 'psd', 'qdraw', 'qtrle', 'r10k', 'r210', 'rawvideo', 'rscc', 'screenpresso', 'sgi', 'sgirle', 'sheervideo', 'srgc', 'sunrast', 'svg', 'targa', 'targa_y216', 'tiff', 'tscc', 'utvideo', 'v210', 'v210x', 'v308', 'v408', 'v410', 'vble', 'vmnc', 'wcmv', 'wrapped_avframe', 'xbm', 'xpm', 'xwd', 'y41p', 'ylc', 'yuv4', 'zerocodec', 'zlib', 'zmbv')
codec_alo = ('alac', 'ape', 'atrac3al', 'atrac3pal', 'dst', 'flac', 'mlp', 'mp4als', 'pcm_bluray', 'pcm_dvd', 'pcm_f16le', 'pcm_f24le', 'pcm_f32be', 'pcm_f32le', 'pcm_f64be', 'pcm_f64le', 'pcm_lxf', 'pcm_s16be', 'pcm_s16be_planar', 'pcm_s16le', 'pcm_s16le_planar', 'pcm_s24be', 'pcm_s24daud', 'pcm_s24le', 'pcm_s24le_planar', 'pcm_s32be', 'pcm_s32le', 'pcm_s32le_planar', 'pcm_s64be', 'pcm_s64le', 'pcm_s8', 'pcm_s8_planar', 'pcm_sga', 'pcm_u16be', 'pcm_u16le', 'pcm_u24be', 'pcm_u24le', 'pcm_u32be', 'pcm_u32le', 'pcm_u8', 'ralf', 's302m', 'shorten', 'tak', 'truehd', 'tta', 'wmalossless')
reg_complete = {
    'video': re.compile(r'video:(\d+)kB'),
    'audio': re.compile(r'audio:(\d+)kB')
}
reg_running = re.compile(r'size= *(\d+)kB')
reg_time = re.compile(r' time=([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{2}) ')
time_zero = datetime.timedelta()
time_second = datetime.timedelta(seconds=1)
waitpool_264 = []
waitpool_av1 = []
lock_264 = threading.Lock()
lock_av1 = threading.Lock()
db = {}
work = []
dirs_work_sub = []


def ffmpeg_time_size_poller(p: subprocess.Popen, size_allow:int=None):
    if size_allow is not None:
        inefficient = False
    reader = p.stderr
    while p.poll() is None:
        chars = []
        while True:
            char = reader.read(1)
            if char in (b'\r', b''):
                break
            elif char != b'\n':
                chars.append(char)
        if chars:
            line = ''.join([char.decode('utf-8') for char in chars])
            if size_allow is None:
                m = reg_time.search(line)
                if m:
                    t = m[1]
            else:
                m = reg_running.search(line)
                if m and int(m[1]) >= size_allow:
                    inefficient = True
                    p.kill()
                    break
                pass
        else:
            p.kill()
            break
    m = reg_complete[stream_type].search(line)
    if m:
        s = int(m[1])
        if size_allow is not None and s >= size_allow:
            inefficient = True
    if size_allow is None:
        return \
            p.wait(), \
            datetime.timedelta(
                hours = int(t[:2]),
                minutes = int(t[3:5]),
                seconds = float(t[6:])
            ), \
            s
    else:
        return inefficient



def get_duration_and_size(media: pathlib.Path, stream_id: int, stream_type: str):
    try:
        p = subprocess.Popen(('ffmpeg', '-i', media, '-c', 'copy', '-map', f'0:{stream_id}', '-f', 'null', '-'), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        r, t, s = ffmpeg_time_size_poller(p)
    except KeyboardInterrupt:
        p.kill()
    return t, s

def scan_dir(d: pathlib.Path):
    global db
    for i in d.iterdir():
        if i.is_dir():
            scan_dir(i)
        elif i.is_file() and i not in db:
            r = subprocess.run(('ffprobe', '-show_format', '-show_streams', '-select_streams', 'V', '-of', 'json', i), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            if r.returncode:
                db[i] = None
            else:
                j = json.loads(r.stdout)
                j_format =  j['format']['format_name']
                if j_format.endswith('_pipe') or j_format in ('image2', 'tty'):
                    db[i] = None
                else:
                    if j['streams']:
                        streams = []
                        video = False
                        for id, s in enumerate(
                            json.loads(subprocess.run(('ffprobe', '-show_format', '-show_streams', '-of', 'json', i), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout)['streams']
                        ):
                            if s['codec_type'] == 'video':
                                if not video and s['codec_type'] == 'video':
                                    video = True
                                duration, size = get_duration_and_size(i, id, 'video')
                                streams.append({
                                    'type': 'video',
                                    'lossless': s['codec_name'] in codec_vlo,
                                    'duration': duration,
                                    'size': size
                                })
                            elif s['codec_type'] == 'audio':
                                duration, size = get_duration_and_size(i, id, 'audio')
                                streams.append({
                                    'type': 'audio',
                                    'lossless': s['codec_name'] in codec_alo,
                                    'duration': duration,
                                    'size': size
                                })
                            else:
                                streams.append(None)
                        if video:
                            db[i] = streams
                        else:
                            db[i] = None
                    else:
                        db[i] = None
        else:
            db[i] = None
def encoder(
    dir_work_sub: pathlib.Path,
    file_raw: pathlib.Path, 
    file_out: pathlib.Path, 
    file_done: pathlib.Path,
    encode_type: str, 
    stream_id: int, # Is used as stream counts when preview + audio
    stream_type: str,
    duration: datetime.timedelta, 
    size_raw: int, 
    lossless: bool
):  
    file_concat_pickle = pathlib.Path(
        dir_work_sub,
        f'{file_raw.stem}_{encode_type}_{stream_id}_{stream_type}_concat.pkl'
    )
    start = time_zero
    size_exist = 0
    concat_list = []
    check_efficiency = encode_type == 'archive'  and not lossless 
    try:
        if file_out.exists() and file_out.stat().st_size:
            file_check = file_out
            suffix = 0
            while file_check.exists() and file_check.stat().st_size:
                time_delta, size_delta = get_duration_and_size(file_check, 0, stream_type)
                # Special case: suffix = 0, the first iteration, here, file_check = file_out
                if suffix == 0 and abs(duration - time_delta) < time_second:
                    # First file, that is, file_out, its length is already OK, consider it finished
                    file_done.touch()
                    return
                start += time_delta
                size_exist += size_delta
                concat_list.append(file_check)
                file_check = pathlib.Path(
                    dir_work_sub,
                    f'{file_raw.stem}_{encode_type}_{stream_id}_{stream_type}_{suffix}.nut'
                )
                suffix += 1
            file_recovery = pathlib.Path(
                dir_work_sub,
                f'{file_raw.stem}_{encode_type}_{stream_id}_{stream_type}_recovery.nut'
            )
            # Recovery
            if stream_type == 'video':
                p = subprocess.run((
                    'ffprobe', '-show_frames', '-select_streams', 'v:0', '-of', 'json'
                ), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                if p.returncode == 0:
                    frames = json.loads(p.stdout)['frames']
                    frame_last = 0
                    for frame_id, frame in enumerate(reversed(frames)):
                        if frame['key_frame']:
                            frame_last = len(frames) - frame_id - 2
                            break
                    if frame_last:
                        p = subprocess.Popen((
                                'ffmpeg', '-i', file_out, '-c', 'copy', '-map', '0', '-y', '-vframes', str(frame_last), file_recovery
                            ), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            else:
                p = subprocess.Popen((
                    'ffmpeg', '-i', file_out, '-c', 'copy', '-map', '0', '-y', file_recovery
                ), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            if p is subprocess.Popen:
                r, t, s = ffmpeg_time_size_poller(p)
                if r == 0:
                    shutil.move(file_recovery, file_check)
                    start += t
                    size_exist += s
                    concat_list.append(file_check)
            file_out.unlink()
            with file_concat_pickle.open('wb') as f:
                pickle.dump({
                    'list': concat_list,
                    'start': start,
                    'size_exist': size_exist
                }, f)
        if not concat_list and file_concat_pickle.exists():
            # No recovery this time (hence, concat_list is empty), but concat_list from last time is found, we use that.
            with file_concat_pickle.open('rb') as f:
                dict_concat = pickle.load(f)
            concat_list = dict_concat['list']
            start = dict_concat['start']
            size_exist = dict_concat['size_exist']
            del dict_concat
        if concat_list:
            # We've already transcoded this
            if check_efficiency and size_raw and size_exist > size_raw * 0.9:
                file_copy = pathlib.Path(
                    dir_work_sub,
                    f'{file_raw.stem}_{encode_type}_{stream_id}_{stream_type}_copy.nut'
                )
                subprocess.run((
                    'ffmpeg', '-i', file_raw, '-c', 'copy', '-map', f'0:{stream_id}', '-y', file_copy
                ), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                shutil.move(file_copy, file_out)
                file_done.touch()
                return 
            if start >= duration or  duration - start < time_second:
                # Consider it finished
                file_list = pathlib.Path(
                    dir_work_sub,
                    f'{file_raw.stem}_{encode_type}_{stream_id}_{stream_type}.list'
                )
                file_concat = pathlib.Path(
                    dir_work_sub,
                    f'{file_raw.stem}_{encode_type}_{stream_id}_{stream_type}_concat.nut'
                )
                with file_list.open('w') as f:
                    for file in concat_list:
                        f.write(f'file {file}\n')
                while subprocess.run((
                    'ffmpeg', '-f', 'concat', '-safe', '0', '-i', file_list, '-c', 'copy', '-map', '0', '-y', file_concat
                ), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
                    time.sleep(60)
                shutil.move(file_concat, file_out)
                file_done.touch()
                return
        if stream_type == 'video':
            if stream_type == 'archive':
                global waitpool_264
            else:
                global waitpool_av1
        # Real encoding happenes below
        if check_efficiency:
            size_allow = size_raw * 0.9 - size_exist
        args_in = ('ffmpeg', '-ss', str(start), '-i', file_raw)
        args_map = ('-map', f'0:{stream_id}')
        args_out = ('-y', file_out)
        if stream_type == 'video':
            if encode_type == 'archive':
                args = (*args_in,  '-c:v', 'libx264', '-crf', '18', '-preset', 'veryslow', *args_map, *args_out)
            else:
                args = (*args_in,  '-c:v', 'libaom-av1', '-crf', '63', '-cpu-used', '2', *args_map, *args_out)
        else: # Audio
            if encode_type == 'archive':
                args = (*args_in, '-c:a', 'libfdk_aac', '-vbr', '5', *args_map, *args_out)
            else:
                if stream_id == 1:
                    args = (*args_in, '-c:a', 'libfdk_aac', '-vbr', '1', '-map', '0:a', *args_out)
                else:
                    args = (*args_in, '-c:a', 'libfdk_aac', '-vbr', '1', '-map', '0:a', '-filter_complex', f'amix=inputs={stream_id}:duration=longest', *args_out)
        while True:
            if stream_type == 'video':
                waiter = threading.Event()
                if encode_type == 'archive':
                    with lock_264:
                        waitpool_264.append(waiter)
                    waiter.wait()
                else:
                    with lock_av1:
                        waitpool_av1.append(waiter)
                    waiter.wait()
            p = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            if check_efficiency and ffmpeg_time_size_poller(p, size_allow):
                file_copy = pathlib.Path(
                    dir_work_sub,
                    f'{file_raw.stem}_{encode_type}_{stream_id}_{stream_type}_copy.nut'
                )
                subprocess.run((
                    'ffmpeg', '-i', file_raw, '-c', 'copy', '-map', f'0:{stream_id}', '-y', file_copy
                ), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                shutil.move(file_copy, file_out)
                file_done.touch()
                return 
            if p.wait() == 0:
                file_done.touch()
                break
    except KeyboardInterrupt:
        try:
            p.kill()
        except NameError:  # Special case: KeyboardInterrupt before p is defined
            pass
def muxer(
    dir_work_sub: pathlib.Path,
    file_raw: pathlib.Path,
    file_cache: pathlib.Path,
    file_out: pathlib.Path,
    streams: list,
    threads: list = None,
):
    if threads is not None and threads:
        for thread in threads:
            thread.join()
    inputs = ['-i', file_raw]
    input_id = 0
    mappers = []
    for stream_id, stream in enumerate(streams):
        if stream is None:
            mappers += ['-map', f'0:{stream_id}']
        else:
            inputs += ['-i', stream]
            input_id += 1
            mappers += ['-map', f'{input_id}']
    subprocess.run((
        'ffmpeg', *inputs, '-c', 'copy', *mappers, '-y', file_out
    ), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    

            
def scheduler():
    global waitpool_264
    global waitpool_av1
    while True:
        wake = False
        cpu_percent = psutil.cpu_percent()
        while cpu_percent < 50 and waitpool_264:
            with lock_264:
                waitpool_264.pop(0).set()
            wake = True
            time.sleep(5)
            cpu_percent = psutil.cpu_percent()
        while cpu_percent < 90 and waitpool_av1:
            with lock_av1:
                waitpool_av1.pop(0).set()
            wake = True
            time.sleep(5)
            cpu_percent = psutil.cpu_percent()
        if wake:
            time.sleep(60)
        else:
            time.sleep(600)

if __name__ == '__main__':
    t_scheduler = threading.Thread(target = scheduler)
    t_scheduler.start()

    while True:
        scan_dir(dir_raw)
        for i in db:
            if i not in work:
                dir_work_sub = pathlib.Path(
                    dir_work,
                    i.stem
                )
                if dir_work_sub in dirs_work_sub:
                    suffix = 0
                    while dir_work_sub in dirs_work_sub:
                        dir_work_sub = pathlib.Path(
                            dir_work,
                            i.stem + str(suffix)
                        )
                dir_work_sub.mkdir()
                dirs_work_sub.append(dir_work_sub)
                threads_archive = []
                threads_preview = []
                streams_archive = []
                streams_preview = []
                audios = []
                file_archive = pathlib.Path(
                    dir_archive,
                    i.name
                )
                file_preview = pathlib.Path(
                    dir_preview,
                    i.name
                )
                if file_archive.exists() and file_preview.exists():
                    pass
                else:
                    for stream_id, stream in enumerate(i):
                        stream_type = stream['type']
                        stream_duration = stream['duration']
                        stream_size = stream['size']
                        stream_lossless = stream['lossless']
                        if stream_type in ('video', 'audio'):
                            if stream_type == 'video':
                                # Archive
                                file_stream_archive = pathlib.Path(
                                    dir_work_sub,
                                    f'{i.stem}_archive_{stream_id}_{stream_type}.nut'
                                )
                                file_stream_done_archive = pathlib.Path(
                                    dir_work_sub,
                                    f'{i.stem}_archive_{stream_id}_{stream_type}.done'
                                )
                                file_stream_preview = pathlib.Path(
                                    dir_work_sub,
                                    f'{i.stem}_preview_{stream_id}_{stream_type}.nut'
                                )
                                file_stream_done_preview = pathlib.Path(
                                    dir_work_sub,
                                    f'{i.stem}_preview_{stream_id}_{stream_type}.done'
                                )
                                if not file_stream_archive.exists() and file_stream_done_archive.exists():
                                    file_stream_done_archive.unlink()
                                if not file_stream_preview.exists() and file_stream_done_preview.exists():
                                    file_stream_done_preview.unlink()
                                streams_archive.append(file_stream_archive)
                                streams_preview.append(file_stream_preview)
                                if not file_stream_done_archive.exists():
                                    threads_archive.append(
                                        threading.Thread(
                                            target=encoder,
                                            args=(
                                                dir_work_sub, 
                                                i,
                                                file_stream_archive, #file_out
                                                file_stream_done_archive,
                                                'archive',
                                                stream_id,
                                                'video',
                                                stream_duration,
                                                stream_size,
                                                stream_lossless
                                            )
                                        )
                                    )
                                    threads_archive[-1].start()
                                if not file_stream_done_preview.exists():
                                    threads_preview.append(
                                        threading.Thread(
                                            target=encoder,
                                            args=(
                                                dir_work_sub, 
                                                i,
                                                file_stream_preview, #file_out
                                                file_stream_done_preview,
                                                'preview',
                                                stream_id,
                                                'video',
                                                stream_duration,
                                                stream_size,
                                                stream_lossless
                                            )
                                        )
                                    )
                            else: # Audio
                                pass
                        else:
                            streams_archive.append(None)
                            streams_preview.append(None)
                muxer_video = threading.Thread(
                    target=muxer,
                    args=(
                        dir_work_sub,
                        i,
                        pathlib.Path(
                            dir_work_sub,

                        ),
                        streams_archive,
                        threads_archive
                    )
                )
                muxer_audio = ''

                work.append(i)
        time.sleep(3600)