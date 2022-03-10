import enum
from genericpath import exists
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
import math

dir_raw = pathlib.Path('raw')
dir_archive = pathlib.Path('archive')
dir_preview = pathlib.Path('preview')
dir_screenshot = pathlib.Path('screenshot')
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
time_ten_seconds = datetime.timedelta(seconds=10)
waitpool_264 = []
waitpool_av1 = []
waitpool_ss = []
lock_264 = threading.Lock()
lock_av1 = threading.Lock()
lock_ss = threading.Lock()
lock_db = threading.Lock()
db = {}
db_last = {}
work = []
dirs_work_sub = []
work_end = False

def str_timedelta(timedelta: datetime.timedelta):
    time = int(timedelta.total_seconds())
    hours, remainder = divmod(time, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f'{hours:02d}:{minutes:02d}:{seconds:02d}'


class cleanPrinter:
    def __init__(self):
        self.check_terminal()

    def check_terminal(self):
        width = shutil.get_terminal_size()[0]
        try:
            if width == self.width:
                return 
        except AttributeError:
            pass
        self.width = width

    def print(self, content, end=None):
        print(' ' * self.width, end='\r')
        if end is None:
            print(content)
        else:
            print(content, end=end)

printer = cleanPrinter()

class progressBar:
    def __init__(self, title = 'Status'):
        self.title = title
        self.title_length = len(title)
        self.check_terminal()
        self.percent = 0
        self.time_start = datetime.datetime.today()
        self.time_spent = time_zero
        self.time_estimate = None
        self.time_spent_str = '00:00:00'
        self.time_estimate_str = '--------'
        self.display()

    def check_terminal(self):
        width = shutil.get_terminal_size()[0]
        try:
            if width == self.width:
                return 
        except AttributeError:
            pass
        self.width = width
        self.bar_length = width - self.title_length - 28
        if self.bar_length <= 0:
            raise ValueError('Terminal too small')

    def display(self, force_update:bool=False):
        if self.percent == 1:
            print(' ' * self.width, end='\r')
        else:
            bar_complete = int(self.percent * self.bar_length)
            bar_incomplete = self.bar_length - bar_complete
            if self.percent == 0 or bar_complete != self.bar_complete or force_update:
                print(''.join([
                    self.title,
                    ' ',
                    ''.join(['█' for i in range(bar_complete)]),
                    ''.join(['-' for i in range(bar_incomplete)]),
                    ' S:',
                    self.time_spent_str,
                    ' R:',
                    self.time_estimate_str,
                    ' '
                    f'{int(self.percent * 100)}%'.rjust(4)
                ]),
                    end='\r'
                )
                self.bar_complete = bar_complete
    def update_timer(self):
        time_now = datetime.datetime.today()
        time_spent = time_now - self.time_start
        if time_spent - self.time_spent > time_second and self.percent > 0:
            self.time_spent = time_spent
            self.time_spent_str = str_timedelta(time_spent)
            self.time_estimate = time_spent / self.percent - time_spent
            self.time_estimate_str = str_timedelta(self.time_estimate)
            return True
        return False

    def pre_display(self):
        self.check_terminal()
        self.display(self.update_timer())

    def set(self, percent):
        self.percent = max(min(percent, 1), 0)
        self.pre_display()
        # if self.percent == 1:
        #     print()

    def add(self, delta_percent):
        self.percent = max(min(self.percent + delta_percent, 1), 0)
        self.pre_display()
        # if self.percent == 1:
        #     print()

def ffmpeg_time_size_poller(p: subprocess.Popen, stream_type:str, size_allow:int=None, progress_bar:progressBar=None, target_time:datetime.timedelta=None):
    if size_allow is not None:
        inefficient = False
    check_time = size_allow is None or (size_allow is not None and progress_bar is not None)
    reader = p.stderr
    if check_time:
        t = time_zero
        percent = 1
    while p.poll() is None:
        chars = []
        while True:
            char = reader.read(1)
            if char in (b'\r', b''):
                break
            elif char != b'\n':
                chars.append(char)
        if chars:
            line = b''.join(chars).decode('utf-8')
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
                        percent = t/target_time
                        progress_bar.set(percent)
            if size_allow is not None:
                m = reg_running.search(line)
                if m:
                    size_produded = int(m[1])
                    if size_produded >= size_allow or (
                        check_time and t > time_ten_seconds and size_produded/size_allow >= percent):
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

def stream_info(file_raw, stream_id, stream_type, stream):
    duration, size = get_duration_and_size(file_raw, stream_id, stream_type)
    if stream_type == 'video':
        lossless = stream['codec_name'] in codec_vlo
        return {
            'type': stream_type,
            'lossless': lossless,
            'width': stream['width'],
            'height': stream['height'],
            'duration': duration,
            'size': size
        }
    else:
        lossless = stream['codec_name'] in codec_alo
        return {
            'type': stream_type,
            'lossless': lossless,
            'duration': duration,
            'size': size
        }

def stream_copy(dir_work_sub, prefix, file_raw, stream_id, file_out, prompt_title, file_done):
    printer.print(f'{prompt_title} Transcode inefficient, copying raw stream instead')
    file_copy = pathlib.Path(
        dir_work_sub,
        f'{prefix}_copy.nut'
    )
    while subprocess.run((
        'ffmpeg', '-i', file_raw, '-c', 'copy', '-map', f'0:{stream_id}', '-y', file_copy
    ), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
        printer.print(f'{prompt_title} Stream copy failed, trying that later')
        time.sleep(5)
    shutil.move(file_copy, file_out)
    printer.print(f'{prompt_title} Stream copy done')
    file_done.touch()

def concat(prefix, concat_list, file_out, prompt_title, file_done):
    printer.print(f'{prompt_title} Transcode done, concating all parts')
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
        printer.print(f'{prompt_title} concating failed, trying that later')
        time.sleep(5)
    shutil.move(file_concat, file_out)
    printer.print(f'{prompt_title} Concating done')
    file_done.touch()

def resolution_shrinker(width, height):
    while width > 1000 and height > 1000:
        width //= 2
        height //= 2
    return width, height

def args_constructor(start, file_raw, file_out, stream_id, stream_type, encode_type, stream_width, stream_height):
    args_in = ('ffmpeg', '-hwaccel', 'auto', '-ss', str(start), '-i', file_raw)
    args_map = ('-map', f'0:{stream_id}')
    args_out = ('-y', file_out)
    if stream_type == 'video':
        if encode_type == 'archive':
            return (*args_in,  '-c:v', 'libx264', '-crf', '18', '-preset', 'veryslow', *args_map, *args_out)
        else:
            if stream_width > 1000 and stream_height > 1000:
                stream_width, stream_height = resolution_shrinker(stream_width, stream_height)
                return (*args_in,  '-c:v', 'libsvtav1', '-qp', '63', '-preset', '8', *args_map, '-filter:v', f'scale={stream_width}x{stream_height}', *args_out)
            else:
                return (*args_in,  '-c:v', 'libsvtav1', '-qp', '63', '-preset', '8', *args_map, *args_out)
    else: # Audio
        if encode_type == 'archive':
            return (*args_in, '-c:a', 'libfdk_aac', '-vbr', '5', *args_map, *args_out)
        else:
            if stream_id == 1:
                return (*args_in, '-c:a', 'libfdk_aac', '-vbr', '1', '-map', '0:a', *args_out)
            else:
                return (*args_in, '-c:a', 'libfdk_aac', '-vbr', '1', '-map', '0:a', '-filter_complex', f'amix=inputs={stream_id}:duration=longest', *args_out)

def wait_write(file_raw:pathlib.Path):
    size_old = file_raw.stat().st_size
    hint = False
    while True:
        opened = False
        for p in psutil.process_iter():
            try:
                for f in p.open_files():
                    if file_raw.samefile(f.path):
                        if not hint:
                            printer.print(f'[Scanner] Jammed, {file_raw} is opened by {p.pid} {p.cmdline()}')
                            hint = True
                        opened = True
                        break
            except (psutil.NoSuchProcess, psutil.AccessDenied, PermissionError):
                pass
        size_new = file_raw.stat().st_size
        if size_new != size_old:
            opened = True
            if not hint:
                printer.print(f'[Scanner] Jammed, {file_raw} is being written')

        if not opened:
            printer.print(f'[Scanner] {file_raw} closed writing, continue scanning')
            break
        size_old = size_new
        time.sleep(5)

def scan_dir(d: pathlib.Path):
    global db
    for i in d.iterdir():
        if i.is_dir():
            scan_dir(i)
        elif i.is_file() and i not in db:
            wait_write(i)
            db_entry = None
            if i not in db:
                r = subprocess.run(('ffprobe', '-show_format', '-show_streams', '-select_streams', 'V', '-of', 'json', i), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                if r.returncode == 0:
                    j = json.loads(r.stdout)
                    j_format =  j['format']['format_name']
                    if not (j_format.endswith('_pipe') or j_format in ('image2', 'tty')) and j['streams']:
                        streams = []
                        video = False
                        for stream_id, s in enumerate(
                            json.loads(subprocess.run(('ffprobe', '-show_format', '-show_streams', '-of', 'json', i), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout)['streams']
                        ):
                            if s['codec_type'] in ('video', 'audio'):
                                if s['codec_type'] == 'video':
                                    video = True
                                streams.append(stream_info(i, stream_id, s['codec_type'], s))
                            else:
                                streams.append(None)
                        if video:
                            db_entry = streams
            with lock_db:
                db[i] = db_entry
    db_write()

def delta_adder(file_check, stream_type, start, size_exist):
    time_delta, size_delta = get_duration_and_size(file_check, 0, stream_type)
    return start + time_delta, size_exist + size_delta

def encoder(
    dir_work_sub: pathlib.Path,
    file_raw: pathlib.Path, file_out: pathlib.Path, file_done: pathlib.Path,
    encode_type: str, 
    stream_id: int, stream_type: str, duration: datetime.timedelta, size_raw: int, lossless: bool,
    stream_width: int, stream_height:int
):  
    if encode_type == 'preview' and stream_type == 'audio':
        prompt_title = f'[{file_raw.name}] E:P S:A:{stream_id}'
    else:
        prompt_title = f'[{file_raw.name}] E:{encode_type[:1].capitalize()} S:{stream_id}:{stream_type[:1].capitalize()}'
    printer.print(f'{prompt_title} Work started')
    if encode_type == 'preview' and stream_type == 'audio':
        prefix = f'{file_raw.stem}_preview_audio'
    else:
        prefix = f'{file_raw.stem}_{encode_type}_{stream_id}_{stream_type}'
    file_concat_pickle = dir_work_sub / f'{prefix}_concat.pkl'
    start = time_zero
    size_exist = 0
    concat_list = []
    check_efficiency = encode_type == 'archive'  and not lossless 
    if file_out.exists() and file_out.stat().st_size:
        printer.print(f'{prompt_title} Output already exists, potentially broken before, trying to recover it')
        time_delta, size_delta = get_duration_and_size(file_out, 0, stream_type)
        if abs(duration - time_delta) < time_second:
            printer.print(f'{prompt_title} Last transcode successful, no need to transcode')
            file_done.touch()
            return
        suffix = 0
        file_check = dir_work_sub / f'{prefix}_{suffix}.nut'
        while file_check.exists() and file_check.stat().st_size:
            start, size_exist = delta_adder(file_check, stream_type, start, size_exist)
            concat_list.append(file_check.name)
            suffix += 1
            file_check = dir_work_sub / f'{prefix}_{suffix}.nut'
        file_recovery = dir_work_sub / f'{prefix}_recovery.nut'
        if stream_type == 'video':
            printer.print(f'{prompt_title} Checking if the interrupt file is usable')
            p = subprocess.run(('ffprobe', '-show_frames', '-select_streams', 'v:0', '-of', 'json', file_out), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            if p.returncode == 0:
                printer.print(f'{prompt_title} Analyzing available frames')
                frames = json.loads(p.stdout)['frames']
                frame_last = 0
                for frame_id, frame in enumerate(reversed(frames)):
                    if frame['key_frame']:
                        frame_last = len(frames) - frame_id - 1
                        break
                if frame_last:
                    printer.print(f'{prompt_title} {frame_last} frames seem usable, trying to recovering those')
                    p = subprocess.Popen(('ffmpeg', '-i', file_out, '-c', 'copy', '-map', '0', '-y', '-vframes', str(frame_last), file_recovery), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                else:
                    printer.print(f'{prompt_title} No frames usable')
        else:
            p = subprocess.Popen(('ffmpeg', '-i', file_out, '-c', 'copy', '-map', '0', '-y', file_recovery), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        if isinstance(p, subprocess.Popen):
            r, t, s = ffmpeg_time_size_poller(p, stream_type)
            if r:
                printer.print(f'{prompt_title} Recovery failed')
            else:
                shutil.move(file_recovery, file_check)
                start += t
                size_exist += s
                concat_list.append(file_check.name)
                printer.print(f'{prompt_title} {t} of failed transcode recovered')
        file_out.unlink()
        with file_concat_pickle.open('wb') as f:
            pickle.dump((concat_list, start, size_exist), f)
    if not concat_list and file_concat_pickle.exists():
        with file_concat_pickle.open('rb') as f:
            concat_list, start, size_exist = pickle.load(f)
    if concat_list:
        # We've already transcoded this
        if check_efficiency and size_raw and size_exist > size_raw * 0.9:
            stream_copy(dir_work_sub, prefix, file_raw, stream_id, file_out, prompt_title, file_done)
            return 
        if start >= duration or duration - start < time_second:
            printer.print(f'{prompt_title} Seems already finished, concating all failed parts')
            concat(prefix, concat_list, file_out, prompt_title, file_done)
            return
    if stream_type == 'video':
        if stream_type == 'archive':
            global waitpool_264
            waitpool, lock = waitpool_264
            lock = lock_264
        else:
            global waitpool_av1
            waitpool = waitpool_av1
            lock = lock_av1
    # Real encoding happenes below
    if check_efficiency:
        size_allow = size_raw * 0.9 - size_exist
    args = args_constructor(start, file_raw, file_out, stream_id, stream_type, encode_type, stream_width, stream_height)
    target_time = duration - start
    while True:
        if stream_type == 'video':
            printer.print(f'{prompt_title} Waiting for CPU resources')
            waiter = threading.Event()
            with lock:
                waitpool.append(waiter)
            waiter.wait()
        printer.print(f'{prompt_title} Transcode started')
        p = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        progress_bar = progressBar(prompt_title)
        if check_efficiency:
            if ffmpeg_time_size_poller(p, stream_type, size_allow, progress_bar, target_time):  # return ture for inefficient
                stream_copy(dir_work_sub, prefix, file_raw, stream_id, file_out, prompt_title, file_done)
                return 
        else:
            ffmpeg_time_size_poller(p, stream_type, progress_bar=progress_bar, target_time=target_time)
        #printer.print(f'{prompt_title} waiting to end')
        if p.wait() == 0:
            if concat_list:
                concat_list.append(file_out.name)
                concat(prefix, concat_list, file_out, prompt_title, file_done)
            else:
                printer.print(f'{prompt_title} Transcode done')
                file_done.touch()
            #printer.print(f'{prompt_title} ended {file_done}')
            break
        printer.print(f'{prompt_title} Transcode failed, returncode: {p.wait()}, retrying that later')
        time.sleep(5)

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
    printer.print(f'[{file_raw.name}] Muxing to {file_out}...')
    while subprocess.run((
        'ffmpeg', *inputs, '-c', 'copy', *mappers, '-map_metadata', '0', '-y', file_cache
    ), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
        printer.print(f'[{file_raw.name}] Muxing failed, retry that later')
        time.sleep(5)
    shutil.move(file_cache, file_out)
    printer.print(f'[{file_raw.name}] Muxing finished')

def clamp(n, minimum, maximum):
    return min(max(n, minimum), maximum)

def screenshooter(
    file_raw: pathlib.Path,
    file_cache: pathlib.Path,
    file_out: pathlib.Path,
    stream_id: int,
    duration: datetime.timedelta,
    stream_width: int,
    stream_height: int
):
    length = clamp(int(math.log(duration.total_seconds() + 1)), 1, 10)
    if length == 1:
        prompt_title = f'[{file_raw.name}]'
        args = ('ffmpeg', '-hwaccel', 'auto', '-i', file_raw, '-map', f'0:{stream_id}', '-vsync', 'passthrough', '-frames:v', '1', '-y', file_cache)
        prompt = f'{prompt_title} Taking screenshot, single frame'
    else:
        prompt_title = f'[{file_raw.name}] S:{stream_id}'
        width = stream_width * length
        height = stream_height * length
        if width > 65535 or length > 65535:
            while width > 65535 or length > 65535:
                width //= 2
                length //= 2
            arg_scale = f',scale={width}x{height}'
        else:
            arg_scale = ''
        tiles = length**2
        time_delta = duration / tiles
        if duration < datetime.timedelta(seconds=tiles*5):
            args = (
                'ffmpeg', '-hwaccel', 'auto', '-i', file_raw, '-map', f'0:{stream_id}', '-filter:v', f'select=eq(n\,0)+gte(t-prev_selected_t\,{time_delta.total_seconds()}),tile={length}x{length}{arg_scale}', '-vsync', 'passthrough', '-frames:v', '1', '-y', file_cache
            )
        else:
            time_start = time_zero
            args_input = []
            args_position = []
            args_mapper = []
            file_id = 0
            for i in range(length):
                for j in range(length):
                    args_input.extend(('-ss', str(time_start), '-i', file_raw))
                    args_mapper.append(f'[{file_id}:{stream_id}]')
                    args_position.append(f'{j*stream_width}_{i*stream_height}')
                    time_start += time_delta
                    file_id += 1
            arg_mapper = ''.join(args_mapper)
            arg_position = '|'.join(args_position)
            args = (
                'ffmpeg', '-hwaccel', 'auto', *args_input, '-filter_complex', f'{arg_mapper}xstack=inputs={tiles}:layout={arg_position}{arg_scale}', '-vsync', 'passthrough', '-frames:v', '1', '-y', file_cache
            )
        prompt = f'{prompt_title} Taking screenshot, {length}x{length} grid, {width}x{height} res, for each {time_delta} segment'
    global waitpool_ss
    while True:
        waiter = threading.Event()
        with lock_ss:
            waitpool_ss.append(waiter)
        waiter.wait()
        printer.print(prompt)
        r = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode
        if r:
            printer.print(f'{prompt_title} Failed to screenshoot, retring that later')
        else:
            break
        time.sleep(5)
    printer.print(f'{prompt_title} Screenshot taken')
    shutil.move(file_cache, file_out)


def cleaner(
    dir_work_sub: pathlib.Path,
    file_raw: pathlib.Path,
    threads: list
):
    for thread in threads:
        thread.join()
    printer.print(f'[{file_raw.name}] Cleaning input and {dir_work_sub}...')
    shutil.rmtree(dir_work_sub)
    file_raw.unlink()
    with lock_db:
        if file_raw in db:
            del db[file_raw]
    db_write()
    global work
    work.remove(file_raw)
    printer.print(f'[{file_raw.name}] Done')

def db_write():
    global db_last
    with lock_db:
        if db != db_last:
            with open(file_db, 'wb') as f:
                pickle.dump(db, f)
            printer.print('[Database] Saved')
            db_last = {i:j for i, j in db.items()}

            
def scheduler():
    global waitpool_264
    global waitpool_av1
    global work_end
    printer.print('[Scheduler] Started')
    while not work_end:
        cpu_percent = psutil.cpu_percent()
        while cpu_percent < 50 and waitpool_264:
            with lock_264:
                waitpool_264.pop(0).set()
            time.sleep(5)
            cpu_percent = psutil.cpu_percent()
        while cpu_percent < 60 and waitpool_av1:
            with lock_av1:
                waitpool_av1.pop(0).set()
            time.sleep(5)
            cpu_percent = psutil.cpu_percent()
        while cpu_percent < 90 and waitpool_ss:
            with lock_ss:
                waitpool_ss.pop(0).set()
            time.sleep(5)
            cpu_percent = psutil.cpu_percent()
        time.sleep(5)
    printer.print('[Scheduler] Exited')

def thread_adder(dir_work_sub, file_raw, encode_type, stream_id, stream_type, stream_duration, stream_size, stream_lossless, stream_width, stream_height, streams, threads, amix=False):
    if amix:
        file_stream = dir_work_sub / f'{file_raw.stem}_preview_audio.nut'
        file_stream_done = dir_work_sub / f'{file_raw.stem}_preview_audio.done'
    else:
        file_stream = dir_work_sub / f'{file_raw.stem}_{encode_type}_{stream_id}_{stream_type}.nut'
        file_stream_done = dir_work_sub / f'{file_raw.stem}_{encode_type}_{stream_id}_{stream_type}.done'
    streams.append(file_stream)
    if not file_stream.exists() and file_stream_done.exists():
        file_stream_done.unlink()
    if not file_stream_done.exists():
        threads.append(threading.Thread(target=encoder, args=(
            dir_work_sub, 
            i, file_stream, file_stream_done,
            encode_type,
            stream_id, stream_type, stream_duration, stream_size, stream_lossless,
            stream_width, stream_height
        )))
        threads[-1].start()
    return streams, threads

def db_cleaner(db):
    db_new = {}
    for i_r, j_r in {i:j for i, j in db.items() if i.exists()}.items():
        name = f'{i_r.stem}.mkv'
        finish = False
        if (dir_archive / name).exists() and (dir_preview / name).exists():
            dir_screenshot_sub = dir_screenshot / f'{i_r.stem}'
            if dir_screenshot_sub.exists():
                if dir_screenshot_sub.is_file():
                    if i_r.exists():
                        finish = True
                elif dir_screenshot_sub.is_dir():
                    finish = True
                    for stream_id, stream in enumerate(j_r):
                        if stream is not None and stream['type'] == 'video':
                            if not (dir_screenshot_sub / f'{i_r.stem}_{stream_id}.jpg').exists():
                                finish = False
                                break
        if finish and i_r.exists():
            i_r.unlink()
        else:
            db_new[i_r] = j_r
    return db_new

if __name__ == '__main__':
    for dir in ( dir_raw, dir_archive, dir_preview, dir_work, dir_screenshot ):
        if not dir.exists():
            dir.mkdir()
    if file_db.exists():
        with open(file_db, 'rb') as f:
            db = pickle.load(f)
        db_last = {i:j for i,j in db.items()}
    threading.Thread(target = scheduler).start()
    try:
        while True:
            db = db_cleaner(db)
            db_write()
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
                    videos = {}
                    audios = []
                    audios_size = 0
                    audios_duration = time_zero
                    name = f'{i.stem}.mkv'
                    file_archive = dir_archive / name
                    file_preview = dir_preview / name
                    work_archive = not file_archive.exists()
                    work_preview = not file_preview.exists()
                    for stream_id, stream in enumerate(j):
                        if stream is None:
                            if work_archive:
                                streams_archive.append(None)
                            if work_preview:
                                streams_preview.append(None)
                        else:
                            stream_type = stream['type']
                            stream_duration = stream['duration']
                            stream_size = stream['size']
                            stream_lossless = stream['lossless']
                            if stream_type in ('video', 'audio'):
                                if stream_type == 'video':
                                    videos[stream_id] = stream
                                if work_archive:
                                    streams_archive, threads_archive = thread_adder(dir_work_sub, i, 'archive', stream_id, stream_type, stream_duration, stream_size, stream_lossless, 0, 0, streams_archive, threads_archive)
                                if work_preview:
                                    if stream_type == 'video':
                                        streams_preview, threads_preview = thread_adder(dir_work_sub, i, 'preview', stream_id, 'video', stream_duration, stream_size, stream_lossless, stream['width'], stream['height'], streams_preview, threads_preview)
                                    else:
                                        audios.append(stream_id)
                                        audios_size += stream_size
                                        audios_duration = max(stream_duration, audios_duration)
                            else:
                                if work_archive:
                                    streams_archive.append(None)
                                if work_preview:
                                    streams_preview.append(None)
                    threads_screenshot=[]
                    if len(videos) == 1:
                        file_screenshot = dir_screenshot / f'{i.stem}.jpg'
                        stream_id, stream = next(iter(videos.items()))
                        if not file_screenshot.exists():
                            threads_screenshot.append(threading.Thread(target=screenshooter, args=(i, dir_work_sub / f'{i.stem}_screenshot.jpg', file_screenshot, stream_id, stream['duration'], stream['width'], stream['height'])))
                            threads_screenshot[-1].start()
                    else:
                        dir_screenshot_sub = dir_screenshot / i.stem
                        if not dir_screenshot_sub.exists():
                            dir_screenshot_sub.mkdir()
                        for i_r, j_r in videos.items():
                            file_screenshot = dir_screenshot_sub / f'{i.stem}_{i_r}.jpg'
                            if not file_screenshot.exists():
                                threads_screenshot.append(threading.Thread(target=screenshooter, args=(i, dir_work_sub / f'{i.stem}_screenshot_{i_r}.jpg', file_screenshot, i_r, j_r['duration'], j_r['width'], j_r['height'])))
                                threads_screenshot[-1].start()
                    threads_muxer = []
                    if work_archive:
                        threads_muxer.append(threading.Thread(target=muxer, args=(
                            i, dir_work_sub / f'{i.stem}_archive.mkv', file_archive,
                            streams_archive, threads_archive
                        )))
                        threads_muxer[-1].start()
                    if work_preview:
                        if audios:
                            streams_preview, threads_preview = thread_adder(dir_work_sub, i, 'preview', len(audios), 'audio', audios_duration, audios_size, True, 0, 0, streams_preview, threads_preview, True)
                        threads_muxer.append(threading.Thread(target=muxer, args=(
                            i, dir_work_sub / f'{i.stem}_preview.mkv', file_preview,
                            streams_preview, threads_preview
                        )))
                        threads_muxer[-1].start()
                    
                    threading.Thread(target=cleaner, args=(dir_work_sub, i, threads_screenshot + threads_muxer)).start()
                    work.append(i)
            time.sleep(5)
    except KeyboardInterrupt:
        printer.print('[Main] Keyboard Interrupt received, exiting safely...')
        db_write()
        work_end = True