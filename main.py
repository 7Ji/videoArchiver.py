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
file_db = dir_work / 'db.pkl'
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
lock_db = threading.Lock()
db = {}
work = []
dirs_work_sub = []

class progressBar:
    def __init__(self, title = 'Status'):
        self.title = title
        self.title_length = len(title)
        self.check_terminal()
        self.percent = 0
        self.display()

    def check_terminal(self):
        width = shutil.get_terminal_size()[0]
        try:
            if width == self.width:
                return 
        except AttributeError:
            pass
        self.width = width
        self.bar_length = width - self.title_length - 5
        if self.bar_length <= 0:
            raise ValueError('Terminal too small')

    def display(self):
        bar_complete = int(self.percent * self.bar_length)
        bar_incomplete = self.bar_length - bar_complete
        print(''.join([
            self.title,
            ' ',
            ''.join(['█' for i in range(bar_complete)]),
            ''.join(['-' for i in range(bar_incomplete)]),
            f'{int(self.percent * 100)}%'.rjust(4)
        ]),
            end='\r'
        )

    def set(self, percent):
        self.check_terminal()
        self.percent = max(min(percent, 1), 0)
        self.display()
        if self.percent == 1:
            print()

    def add(self, delta_percent):
        self.check_terminal()
        self.percent = max(min(self.percent + delta_percent, 1), 0)
        self.display()
        if self.percent == 1:
            print()

def ffmpeg_time_size_poller(p: subprocess.Popen, stream_type:str, size_allow:int=None, progress_bar:progressBar=None, target_time:datetime.timedelta=None):
    if size_allow is not None:
        inefficient = False
    check_time = size_allow is None or (size_allow is not None and progress_bar is not None)
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
            if check_time:
                m = reg_time.search(line)
                if m:
                    t = m[1]
                    if progress_bar is not None:
                        t = datetime.timedelta(
                            hours = int(t[:2]),
                            minutes = int(t[3:5]),
                            seconds = float(t[6:])
                        )
                        progress_bar.set(t/target_time)
            if size_allow is not None:
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
    if progress_bar is not None:
        progress_bar.set(1)
    if size_allow is None:
        if progress_bar is None:
            return \
                p.wait(), \
                datetime.timedelta(
                    hours = int(t[:2]),
                    minutes = int(t[3:5]),
                    seconds = float(t[6:])
                ), \
                s
        else:
            return p.wait(), t, s
    else:
        return inefficient



def get_duration_and_size(media: pathlib.Path, stream_id: int, stream_type: str):
    try:
        p = subprocess.Popen(('ffmpeg', '-i', media, '-c', 'copy', '-map', f'0:{stream_id}', '-f', 'null', '-'), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        r, t, s = ffmpeg_time_size_poller(p, stream_type)
    except KeyboardInterrupt:
        p.kill()
    return t, s

def scan_dir(d: pathlib.Path):
    global db
    for i in d.iterdir():
        if i.is_dir():
            scan_dir(i)
        elif i.is_file():
            if i not in db:
                r = subprocess.run(('ffprobe', '-show_format', '-show_streams', '-select_streams', 'V', '-of', 'json', i), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                if r.returncode:
                    with lock_db:
                        db[i] = None
                else:
                    j = json.loads(r.stdout)
                    j_format =  j['format']['format_name']
                    if j_format.endswith('_pipe') or j_format in ('image2', 'tty'):
                        with lock_db:
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
                                with lock_db:
                                    db[i] = streams
                            else:
                                with lock_db:
                                    db[i] = None
                        else:
                            with lock_db:
                                db[i] = None
        else:
            with lock_db:
                db[i] = None
    db_write()

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
    if encode_type == 'preview' and stream_type == 'audio':
        if stream_id == 1:
            debug_title = f'{file_raw.name}: Stream ? (audio) preview'
        else:
            debug_title = f'{file_raw.name}: Amix ({stream_id} audios) preview'
    else:
        debug_title = f'{file_raw.name}: Stream {stream_id} ({stream_type}) {encode_type}'
    print(f'{debug_title} work started')
    if encode_type == 'preview' and stream_type == 'audio':
        prefix = f'{file_raw.stem}_preview_audio'
    else:
        prefix = f'{file_raw.stem}_{encode_type}_{stream_id}_{stream_type}'
    file_concat_pickle = pathlib.Path(
        dir_work_sub,
        f'{prefix}_concat.pkl'
    )
    start = time_zero
    size_exist = 0
    concat_list = []
    check_efficiency = encode_type == 'archive'  and not lossless 
    if file_out.exists() and file_out.stat().st_size:
        print(f'{debug_title} output already exists, potentially broken before')
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
                f'{prefix}_{suffix}.nut'
            )
            suffix += 1
        file_recovery = pathlib.Path(
            dir_work_sub,
            f'{prefix}_recovery.nut'
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
                    print(f'{debug_title} {frame_last} frames seem usable, trying to recovering those')
                    p = subprocess.Popen((
                            'ffmpeg', '-i', file_out, '-c', 'copy', '-map', '0', '-y', '-vframes', str(frame_last), file_recovery
                        ), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        else:
            p = subprocess.Popen((
                'ffmpeg', '-i', file_out, '-c', 'copy', '-map', '0', '-y', file_recovery
            ), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        if p is subprocess.Popen:
            r, t, s = ffmpeg_time_size_poller(p, stream_type)
            if r == 0:
                shutil.move(file_recovery, file_check)
                start += t
                size_exist += s
                concat_list.append(file_check)
            print(f'{debug_title} {t} of failed transcode recovered')
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
            print(f'{debug_title} transcode inefficient, copying raw stream instead')
            file_copy = pathlib.Path(
                dir_work_sub,
                f'{prefix}_copy.nut'
            )
            subprocess.run((
                'ffmpeg', '-i', file_raw, '-c', 'copy', '-map', f'0:{stream_id}', '-y', file_copy
            ), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            shutil.move(file_copy, file_out)
            print(f'{debug_title} stream copy done')
            file_done.touch()
            return 
        if start >= duration or  duration - start < time_second:
            print(f'{debug_title} seems already finished, concating all failed parts')
            # Consider it finished
            file_list = pathlib.Path(
                dir_work_sub,
                f'{prefix}.list'
            )
            file_concat = pathlib.Path(
                dir_work_sub,
                f'{prefix}_concat.nut'
            )
            with file_list.open('w') as f:
                for file in concat_list:
                    f.write(f'file {file}\n')
            while subprocess.run((
                'ffmpeg', '-f', 'concat', '-safe', '0', '-i', file_list, '-c', 'copy', '-map', '0', '-y', file_concat
            ), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
                time.sleep(60)
            shutil.move(file_concat, file_out)
            print(f'{debug_title} concating done')
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
    args_in = ('ffmpeg', '-hwaccel', 'auto', '-ss', str(start), '-i', file_raw)
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
            print(f'{debug_title} waiting for CPU resources')
            waiter = threading.Event()
            if encode_type == 'archive':
                with lock_264:
                    waitpool_264.append(waiter)
                waiter.wait()
            else:
                with lock_av1:
                    waitpool_av1.append(waiter)
                waiter.wait()
        print(f'{debug_title} transcode started')
        if check_efficiency:
            p = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        else:
            p = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        progress_bar = progressBar(debug_title)
        #print(f'{debug_title} started')
        target_time=duration-start
        if check_efficiency and ffmpeg_time_size_poller(p, stream_type, size_allow, progress_bar, target_time):
            #print(f'{debug_title} inefficient')
            print(f'{debug_title} transcode inefficient, copying raw stream instead')
            file_copy = pathlib.Path(
                dir_work_sub,
                f'{prefix}_copy.nut'
            )
            subprocess.run((
                'ffmpeg', '-i', file_raw, '-c', 'copy', '-map', f'0:{stream_id}', '-y', file_copy
            ), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            shutil.move(file_copy, file_out)
            print(f'{debug_title} stream copy done')
            file_done.touch()
            return 
        else:
            ffmpeg_time_size_poller(p, stream_type, progress_bar=progress_bar, target_time=target_time)
        #print(f'{debug_title} waiting to end')
        if p.wait() == 0:
            if concat_list:
                print(f'{debug_title} transcode done, concating all parts')
                file_list = pathlib.Path(
                    dir_work_sub,
                    f'{prefix}.list'
                )
                file_concat = pathlib.Path(
                    dir_work_sub,
                    f'{prefix}_concat.nut'
                )
                with file_list.open('w') as f:
                    for file in concat_list:
                        f.write(f'file {file}\n')
                    f.write(f'file {file_out}\n')
                while subprocess.run((
                    'ffmpeg', '-f', 'concat', '-safe', '0', '-i', file_list, '-c', 'copy', '-map', '0', '-y', file_concat
                ), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
                    time.sleep(60)
                shutil.move(file_concat, file_out)
                print(f'{debug_title} concating done')
            else:
                print(f'{debug_title} transcode done')
            file_done.touch()
            #print(f'{debug_title} ended {file_done}')
            break
        print(f'{debug_title} transcode failed, returncode: {p.wait()}')
def muxer(
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
    print(f'Muxing {file_out}...')
    while subprocess.run((
        'ffmpeg', *inputs, '-c', 'copy', *mappers, '-map_metadata', '0', '-y', file_cache
    ), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
        print(f'Muxing of {file_out} failed, remuxing...')
        time.sleep(60)
    shutil.move(file_cache, file_out)
    print(f'Muxing of {file_out} done')

def cleaner(
    dir_work_sub: pathlib.Path,
    file_raw: pathlib.Path,
    muxers: list
):
    for muxer in muxers:
        muxer.join()
    print(f'Cleaning {file_raw}... {dir_work_sub} and all its contents will be deleted')
    shutil.rmtree(dir_work_sub)
    file_raw.unlink()
    with lock_db:
        del db[file_raw]
    db_write()
    print(f'{file_raw.name} done')

def db_write():
    with lock_db:
        with open(file_db, 'wb') as f:
            pickle.dump(db, f)
            
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
            time.sleep(5)
        else:
            time.sleep(30)

if __name__ == '__main__':
    for dir in ( dir_raw, dir_archive, dir_preview, dir_work ):
        if not dir.exists():
            dir.mkdir()
    if file_db.exists():
        with open(file_db, 'rb') as f:
            db = pickle.load(f)
    t_scheduler = threading.Thread(target = scheduler)
    try:
        while True:
            work_count = len(work)
            scan_dir(dir_raw)
            for i, j in db.items():
                if j is not None and i not in work:
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
                    if not dir_work_sub.exists():
                        dir_work_sub.mkdir()
                    dirs_work_sub.append(dir_work_sub)
                    threads_archive = []
                    threads_preview = []
                    threads_muxer = []
                    streams_archive = []
                    streams_preview = []
                    audios = []
                    audios_size = 0
                    audios_duration = time_zero
                    file_archive = pathlib.Path(
                        dir_archive,
                        i.name
                    )
                    file_preview = pathlib.Path(
                        dir_preview,
                        i.name
                    )
                    if file_archive.exists() and file_preview.exists():
                        with lock_db:
                            del db[i]
                        db_write()
                        if i.exists():
                            i.unlink()
                    else:
                        for stream_id, stream in enumerate(j):
                            stream_type = stream['type']
                            stream_duration = stream['duration']
                            stream_size = stream['size']
                            stream_lossless = stream['lossless']
                            if stream_type in ('video', 'audio'):
                                if not file_archive.exists():
                                    file_stream_archive = pathlib.Path(
                                        dir_work_sub,
                                        f'{i.stem}_archive_{stream_id}_{stream_type}.nut'
                                    )
                                    file_stream_done_archive = pathlib.Path(
                                        dir_work_sub,
                                        f'{i.stem}_archive_{stream_id}_{stream_type}.done'
                                    )
                                    streams_archive.append(file_stream_archive)
                                    if not file_stream_archive.exists() and file_stream_done_archive.exists():
                                        file_stream_done_archive.unlink()
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
                                                    stream_type,
                                                    stream_duration,
                                                    stream_size,
                                                    stream_lossless
                                                )
                                            )
                                        )
                                        threads_archive[-1].start()
                                if not file_preview.exists():
                                    if stream_type == 'video':
                                        file_stream_preview = pathlib.Path(
                                            dir_work_sub,
                                            f'{i.stem}_preview_{stream_id}_{stream_type}.nut'
                                        )
                                        file_stream_done_preview = pathlib.Path(
                                            dir_work_sub,
                                            f'{i.stem}_preview_{stream_id}_{stream_type}.done'
                                        )
                                        streams_preview.append(file_stream_preview)
                                        if not file_stream_preview.exists() and file_stream_done_preview.exists():
                                            file_stream_done_preview.unlink()
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
                                            threads_preview[-1].start()
                                    else:
                                        audios.append(stream_id)
                                        audios_size += stream_size
                                        audios_duration = max(stream_duration, audios_duration)
                            else:
                                if not file_archive.exists():
                                    streams_archive.append(None)
                                if not file_preview.exists():
                                    streams_preview.append(None)

                        muxers = []
                        if not file_archive.exists():
                            muxers.append(
                                threading.Thread(
                                    target=muxer,
                                    args=(
                                        i,
                                        pathlib.Path(
                                            dir_work_sub,
                                            f'{i.stem}_archive.mkv'
                                        ),
                                        file_archive,
                                        streams_archive,
                                        threads_archive
                                    )
                                )
                            )
                            muxers[-1].start()
                        if not file_preview.exists():
                            if audios:
                                file_stream_preview = pathlib.Path(
                                    dir_work_sub,
                                    f'{i.stem}_preview_audio.nut'
                                )
                                file_stream_done_preview = pathlib.Path(
                                    dir_work_sub,
                                    f'{i.stem}_preview_audio.done'
                                )
                                streams_preview.append(file_stream_preview)
                                if not file_stream_preview.exists() and file_stream_done_preview.exists():
                                    file_stream_done_preview.unlink()
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
                                                len(audios),
                                                'audio',
                                                audios_duration,
                                                audios_size,
                                                True
                                            )
                                        )
                                    )
                                    threads_preview[-1].start()
                            muxers.append(
                                threading.Thread(
                                    target=muxer,
                                    args=(
                                        i,
                                        pathlib.Path(
                                            dir_work_sub,
                                            f'{i.stem}_preview.mkv'
                                        ),
                                        file_preview,
                                        streams_preview,
                                        threads_preview
                                    )
                                )
                            )
                            muxers[-1].start()
                        thread_cleaner = threading.Thread(
                            target=cleaner,
                            args=(
                                dir_work_sub,
                                i,
                                muxers
                            )
                        )
                        thread_cleaner.start()
                        work.append(i)
            if not t_scheduler.is_alive():
                t_scheduler.start()
            if len(work) != work_count:
                time.sleep(10)
            else:
                time.sleep(60)
    except KeyboardInterrupt:
        with lock_db:
            db_write()