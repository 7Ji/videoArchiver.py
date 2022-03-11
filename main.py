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
lock_work = threading.Lock()
db = {}
db_last = {}
work = []
dirs_work_sub = []
work_end = False
bars = False

class EndExecution(RuntimeError):
    pass

class cleanPrinter:
    '''Fancier printer, the main purpose of this class is to cleanly print message
    '''

    bars = 0
    width = shutil.get_terminal_size()[0]


    @staticmethod
    def check_terminal():
        width = shutil.get_terminal_size()[0]
        if width == cleanPrinter.width:
            return 
        cleanPrinter.width = width


    @staticmethod
    def clear_line():
        '''Always clean the line before printing, this is to avoid the unfinished progress bar being stuck on the screen
        '''
        print(' ' * cleanPrinter.width, end='\r')


    @staticmethod
    def print(content, end=None):
        if cleanPrinter.bars: # Only clear line if there are progress bars being displayed
            cleanPrinter.clear_line()
        if end is None:
            print(content)
        else:
            print(content, end=end)


def str_timedelta(timedelta: datetime.timedelta):
    '''Convert a timedelta object to HH:MM:SS str
    '''
    time = int(timedelta.total_seconds())
    hours, remainder = divmod(time, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f'{hours:02d}:{minutes:02d}:{seconds:02d}'


def clamp(n, minimum, maximum):
    return min(max(n, minimum), maximum)


class progressBar(cleanPrinter):
    '''Progress bar with title, percent, spent/estimate time
    '''
    def __init__(self, title = '[Status]'):
        cleanPrinter.bars += 1
        if title[-1] != ' ':
            title += ' '
        self.title = title
        self.title_length = len(title)
        self.percent = 0
        self.percent_str = '  0%'
        self.bar_complete = -1
        self.bar_complete_str = ''
        self.time_start = datetime.datetime.today()
        self.time_spent = time_zero
        self.time_estimate = None
        self.time_spent_str = ' S:00:00:00'
        self.time_estimate_str = ' R:--:--:--'
        self.display(True)


    def display(self, update=False):
        if self.percent == 1:
            cleanPrinter.clear_line()
            cleanPrinter.bars -= 1
        else:
            width = shutil.get_terminal_size()[0]
            width_update = width != cleanPrinter.width
            if update or width_update:
                if width_update:
                    cleanPrinter.width = width 
                self.display_bar = False
                self.display_percent = False
                self.display_spent = False
                self.display_estimate = False
                length = width - self.title_length - 1  # 1 for bar
                if length > 0:
                    self.display_bar = True
                    if length > 4:
                        self.display_percent = True
                        if length > 15:  # 12 for time_spent_str
                            self.display_spent = True
                            if length > 26:  # 12 for time_estimate_str
                                self.display_estimate = True
                    self.bar_length = cleanPrinter.width - self.title_length - 4 * self.display_percent - 11 * self.display_spent - 11 * self.display_estimate
            if not self.display_bar:
                return
            bar_complete = int(self.percent * self.bar_length)
            if bar_complete != self.bar_complete:
                bar_incomplete = self.bar_length - bar_complete
                self.bar_complete = bar_complete
                self.bar_complete_str = '█' * bar_complete
                self.bar_incomplete_str = '-' * bar_incomplete
                if not update:
                    update = True
            line = [self.title, self.bar_complete_str, self.bar_incomplete_str]
            if self.display_spent:
                time_spent = datetime.datetime.today() - self.time_start
                if time_spent - self.time_spent > time_second:
                    self.time_spent = time_spent
                    self.time_spent_str = ' S:' + str_timedelta(time_spent)
                    if self.display_estimate and self.percent != 0:
                        self.time_estimate = time_spent / self.percent - time_spent
                        self.time_estimate_str = ' R:' + str_timedelta(self.time_estimate)
                    if not update:
                        update = True
                line.append(self.time_spent_str)
            if self.display_estimate:
                line.append(self.time_estimate_str)
            if self.display_percent:
                percent_str = f'{self.percent:>4.0%}'
                if percent_str != self.percent_str:
                    self.percent_str = percent_str
                    if not update:
                        update = True
                line.append(self.percent_str)
            if update:
                print(''.join(line), end='\r')

    def set(self, percent):
        if percent != self.percent:
            self.percent = clamp(percent, 0, 1)
        self.display()

    def add(self, delta_percent):
        if delta_percent != 0:
            self.percent = clamp(self.percent + delta_percent, 0, 1)
        self.display()

def check_end_kill(p:subprocess.Popen):
    '''Check if the work_end flag is set, and if it is, raise the EndExecution exception and kill the Popen object

    used in ffmpeg_dumb_poller (scope: child)
        used in stream_copy (scope: child/encoder)
        used in encoder (scope: child/endoer)
        used in muxer (scope: child/muxer)
        used in screenshooter (scopte: child/screenshooter)
    
    used in ffmpeg_time_size_poller (scope: main + child)
        used in encoder (scope: child/encoder)
        used in get_duration_and_size (wrapper)
            used in stream_info (scope: main)
            used in delta_adder (scope: child/encoder)
            used in encoder (scope: child/encoder)

    scope: child
        Should the work_end flag be captured, raise the EndExecution exception
    '''
    if work_end:
        p.kill()
        raise EndExecution

def check_end():
    '''Check if the work_end flag is set, and if it is, raise the EndExecution exception

    used in stream_copy (scope: child/encoder)
    used in concat (scope: child/encoder)
    used in wait_cpu (scope: child)
        used in encoder (scope: child/encoder)
        used in screenshooter (scope: child/screenshooter)
    used in cleaner (scope: child/cleaner)
    used in db_write (scope: child/cleaner + main)
        used in cleaner (scope: child/cleaner)
        used in main (scope: main)
        used in scan_dir (scope: main)
    used in scheduler (scope: child/scheduler)

    scope: child
        Should the work_end flag be captured, raise the EndExecution exception
    '''
    if work_end:
        raise EndExecution


def ffmpeg_dumb_poller(p: subprocess.Popen):
    '''Polling the ffmpeg endlessly until it is done, do nothing other than trying to capture the work_end flag

    used in stream_copy (scope: child/encoder)
    used in encoder (scope: child/endoer)
    used in muxer (scope: child/muxer)
    used in screenshooter (scopte: child/screenshooter)

    scope: child
        The EndExecution exception could be raised by check_end_kill, we pass it as is
    '''
    while p.poll() is None:
        check_end_kill(p)
        time.sleep(1)
    return p.wait()


def ffmpeg_time_size_poller(p: subprocess.Popen, stream_type:str, size_allow:int=None, progress_bar:progressBar=None, target_time:datetime.timedelta=None):
    '''Polling time and size information from a running ffmpeg subprocess.Popen

    used in encoder (scope: child/encoder)
    used in get_duration_and_size (wrapper)
        used in stream_info (scope: main)
        used in delta_adder (scope: child/encoder)
        used in encoder (scope: child/encoder)

    scope: main
        End when KeyboardException is captured
    scope: child
        The EndExecution exception could be raised by check_end_kill, we pass it as is
    '''
    if size_allow is not None:
        inefficient = False
    check_time = size_allow is None or (size_allow is not None and progress_bar is not None)
    reader = p.stderr
    if check_time:
        t = time_zero
        percent = 1
    while p.poll() is None:
        check_end_kill(p)
        chars = []
        while True:
            check_end_kill(p)
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
    '''Get duration and size from a stream from a media file, just a wrapper, 

    used in stream_info (scope: main)
    used in delta_adder (scope: child/encoder)
    used in encoder (scope: child/encoder)

    scope: main
        End when KeyboardException is captured
    scope: child
        The EndExecution exception could be raised by ffmpeg_time_size_poller, we pass it as is
    '''
    p = subprocess.Popen(('ffmpeg', '-i', media, '-c', 'copy', '-map', f'0:{stream_id}', '-f', 'null', '-'), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    r, t, s = ffmpeg_time_size_poller(p, stream_type)
    return t, s


def stream_info(file_raw, stream_id, stream_type, stream):
    '''Get stream info, 

    used in scan_dir (scope: main)

    scope: main
        End when KeyboardException is captured
    '''
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
    '''Copy a stream from a media file as is, 

    used in encoder (scope: child/encoder)

    scope: child
        The EndExecution exception could be raised by ffmpeg_dumb_poller and check_end, we pass it as is
    '''
    cleanPrinter.print(f'{prompt_title} Transcode inefficient, copying raw stream instead')
    file_copy = pathlib.Path(
        dir_work_sub,
        f'{prefix}_copy.nut'
    )
    args = ('ffmpeg', '-i', file_raw, '-c', 'copy', '-map', f'0:{stream_id}', '-y', file_copy)
    while ffmpeg_dumb_poller(subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)):
        check_end()
        cleanPrinter.print(f'{prompt_title} Stream copy failed, trying that later')
        time.sleep(5)
    shutil.move(file_copy, file_out)
    cleanPrinter.print(f'{prompt_title} Stream copy done')
    file_done.touch()


def concat(prefix, concat_list, file_out, prompt_title, file_done):
    '''Concating multiple parts of a media file

    used in encoder (scope: child/encoder)

    scope: child
        The EndExecution exception could be raised by check_end and ffmpeg_dumb_poller, we pass it as is
    '''
    cleanPrinter.print(f'{prompt_title} Transcode done, concating all parts')
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
            check_end()
            f.write(f'file {file}\n')
    args = ('ffmpeg', '-f', 'concat', '-safe', '0', '-i', file_list, '-c', 'copy', '-map', '0', '-y', file_concat)
    while ffmpeg_dumb_poller(subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)):
        check_end()
        cleanPrinter.print(f'{prompt_title} concating failed, trying that later')
        time.sleep(5)
    shutil.move(file_concat, file_out)
    cleanPrinter.print(f'{prompt_title} Concating done')
    file_done.touch()


def args_constructor(start, file_raw, file_out, stream_id, stream_type, encode_type, stream_width, stream_height):
    '''Counstructing ffmpeg args for encoder
    
    used in encoder (scope: child/encoder)
    
    scope: child
        No exception should be raised, the outer thread is ended by other things
    '''
    args_in = ('ffmpeg', '-hwaccel', 'auto', '-ss', str(start), '-i', file_raw)
    args_map = ('-map', f'0:{stream_id}')
    args_out = ('-y', file_out)
    if stream_type == 'video':
        if encode_type == 'archive':
            return (*args_in,  '-c:v', 'libx264', '-crf', '18', '-preset', 'veryslow', *args_map, *args_out)
        else:
            args_codec = ('-c:v', 'libsvtav1', '-qp', '63', '-preset', '8')
            if stream_width > 1000 and stream_height > 1000:
                while stream_width > 1000 and stream_height > 1000:
                    stream_width //= 2
                    stream_height //= 2
                return (*args_in,  *args_codec, *args_map, '-filter:v', f'scale={stream_width}x{stream_height}', *args_out)
            else:
                return (*args_in,  *args_codec, *args_map, *args_out)
    else: # Audio
        if encode_type == 'archive':
            return (*args_in, '-c:a', 'libfdk_aac', '-vbr', '5', *args_map, *args_out)
        else:
            args_codec = ('-c:a', 'libfdk_aac', '-vbr', '1', '-map', '0:a')
            if stream_id == 1:
                return (*args_in, *args_codec, *args_out)
            else:
                return (*args_in, *args_codec, '-filter_complex', f'amix=inputs={stream_id}:duration=longest', *args_out)


def wait_close(file_raw:pathlib.Path):
    '''Check if a file is being opened, if it is, wait until it's closed

    used in scan_dir (scope: main)

    scope: main
        End when KeyboardException is captured
    '''
    size_old = file_raw.stat().st_size
    hint = False
    while True:
        opened = False
        for p in psutil.process_iter():
            try:
                for f in p.open_files():
                    if file_raw.samefile(f.path):
                        if not hint:
                            cleanPrinter.print(f'[Scanner] Jammed, {file_raw} is opened by {p.pid} {p.cmdline()}')
                            hint = True
                        opened = True
                        break
            except (psutil.NoSuchProcess, psutil.AccessDenied, PermissionError):
                pass
        size_new = file_raw.stat().st_size
        if size_new != size_old:
            opened = True
            if not hint:
                cleanPrinter.print(f'[Scanner] Jammed, {file_raw} is being written')
        if not opened:
            if hint:
                cleanPrinter.print(f'[Scanner] {file_raw} closed writing, continue scanning')
            break
        size_old = size_new
        time.sleep(5)


def scan_dir(d: pathlib.Path):
    '''Recursively scan dirs, 

    used in main (scope: main)
    used in scan_dir recursively (scope: main)
    
    scope: main
        End when KeyboardException is captured
    '''
    global db
    for i in d.iterdir():
        if i.is_dir():
            scan_dir(i)
        elif i.is_file() and i not in db:
            wait_close(i)
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
    '''Get duration and size from a file, add them to existing value, return the total

    used in encoder (scope: child/encoder)

    scope: child
        The EndExecution exception could be raised by get_duration_and_size, we pass it as is
    '''
    time_delta, size_delta = get_duration_and_size(file_check, 0, stream_type)
    return start + time_delta, size_exist + size_delta


def wait_cpu(lock, waitpool):
    '''Wait for CPU resource,

    used in encoder (scope: child/encoder)
    used in screenshooter (scope: child/screenshooter)

    scope: child
        The EndExecution exception could be raised by wait_end, we pass it as is
    '''
    waiter = threading.Event()
    check_end()
    with lock:
        check_end()
        waitpool.append(waiter)
    waiter.wait()
    check_end()

def encoder(
    dir_work_sub: pathlib.Path,
    file_raw: pathlib.Path, file_out: pathlib.Path, file_done: pathlib.Path,
    encode_type: str, 
    stream_id: int, stream_type: str, duration: datetime.timedelta, size_raw: int, lossless: bool,
    stream_width: int, stream_height:int
):  
    '''Encoding certain stream in a media file

    encoder itself (scope: child/encoder)

    scope: child-main
        As the invoker of other child functions, EndExecution exception raised by child functions will be captured here:
            get_duration_and_size
            delta_adder
            ffmpeg_dumb_poller
            ffmpeg_time_size_poller
            stream_copy
            concat
            wait_cpu

        Should that, or the end_work flag be captured, return to end this thread
    '''
    try:
        if encode_type == 'preview' and stream_type == 'audio':
            prompt_title = f'[{file_raw.name}] E:P S:A:{stream_id}'
        else:
            prompt_title = f'[{file_raw.name}] E:{encode_type[:1].capitalize()} S:{stream_id}:{stream_type[:1].capitalize()}'
        cleanPrinter.print(f'{prompt_title} Work started')
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
            cleanPrinter.print(f'{prompt_title} Output already exists, potentially broken before, trying to recover it')
            time_delta, size_delta = get_duration_and_size(file_out, 0, stream_type)
            if abs(duration - time_delta) < time_second:
                cleanPrinter.print(f'{prompt_title} Last transcode successful, no need to transcode')
                file_done.touch()
                return
            suffix = 0
            file_check = dir_work_sub / f'{prefix}_{suffix}.nut'
            while file_check.exists() and file_check.stat().st_size:
                if work_end:
                    return
                start, size_exist = delta_adder(file_check, stream_type, start, size_exist)
                concat_list.append(file_check.name)
                suffix += 1
                file_check = dir_work_sub / f'{prefix}_{suffix}.nut'
            file_recovery = dir_work_sub / f'{prefix}_recovery.nut'
            poll = False
            if stream_type == 'video':
                cleanPrinter.print(f'{prompt_title} Checking if the interrupt file is usable')
                p = subprocess.Popen(('ffprobe', '-show_frames', '-select_streams', 'v:0', '-of', 'json', file_out), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                if ffmpeg_dumb_poller(p) == 0:
                    cleanPrinter.print(f'{prompt_title} Analyzing available frames')
                    frames = json.loads(p.stdout)['frames']
                    frame_last = 0
                    for frame_id, frame in enumerate(reversed(frames)):
                        if work_end:
                            return
                        if frame['key_frame']:
                            frame_last = len(frames) - frame_id - 1
                            break
                    if frame_last:
                        cleanPrinter.print(f'{prompt_title} {frame_last} frames seem usable, trying to recovering those')
                        p = subprocess.Popen(('ffmpeg', '-i', file_out, '-c', 'copy', '-map', '0', '-y', '-vframes', str(frame_last), file_recovery), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                        poll = True
                    else:
                        cleanPrinter.print(f'{prompt_title} No frames usable')
            else:
                poll = True
                p = subprocess.Popen(('ffmpeg', '-i', file_out, '-c', 'copy', '-map', '0', '-y', file_recovery), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            if poll:
                r, t, s = ffmpeg_time_size_poller(p, stream_type)
                if r:
                    cleanPrinter.print(f'{prompt_title} Recovery failed')
                else:
                    shutil.move(file_recovery, file_check)
                    start += t
                    size_exist += s
                    concat_list.append(file_check.name)
                    cleanPrinter.print(f'{prompt_title} {t} of failed transcode recovered')
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
                cleanPrinter.print(f'{prompt_title} Seems already finished, concating all failed parts')
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
            if work_end:
                return
            if stream_type == 'video':
                cleanPrinter.print(f'{prompt_title} Waiting for CPU resources')
                wait_cpu(lock, waitpool)
            cleanPrinter.print(f'{prompt_title} Transcode started')
            p = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            progress_bar = progressBar(prompt_title)
            if check_efficiency:
                if ffmpeg_time_size_poller(p, stream_type, size_allow, progress_bar, target_time):  # return ture for inefficient
                    stream_copy(dir_work_sub, prefix, file_raw, stream_id, file_out, prompt_title, file_done)
                    return
            else:
                ffmpeg_time_size_poller(p, stream_type, progress_bar=progress_bar, target_time=target_time)
            #cleanPrinter.print(f'{prompt_title} waiting to end')
            if p.wait() == 0:
                if concat_list:
                    concat_list.append(file_out.name)
                    concat(prefix, concat_list, file_out, prompt_title, file_done)
                else:
                    cleanPrinter.print(f'{prompt_title} Transcode done')
                    file_done.touch()
                #cleanPrinter.print(f'{prompt_title} ended {file_done}')
                break
            cleanPrinter.print(f'{prompt_title} Transcode failed, returncode: {p.wait()}, retrying that later')
            time.sleep(5)
    except EndExecution:
        return


def join(thread: threading.Thread):
    '''Cleanly join a thread, just an invoker so that the work_end flag can be captured

    used in muxer (scope: child/muxer)
    used in cleaner (scope: child/cleaner)

    scope: child
        The EndExecution flag could be raised by check_end, we pass it as is
    '''
    check_end()
    thread.join()
    check_end()


def muxer(
    file_raw: pathlib.Path,
    file_cache: pathlib.Path,
    file_out: pathlib.Path,
    streams: list,
    threads: list = None,
):
    '''Muxing finished video/audio streams and non va streams from raw file to a new mkv container

    muxer itself (scopte: child/muxer)

    scope: child-main
        As the invoker of other child functions, EndExecution exception raised by child functions will be captured here:
            join
            ffmpeg_dumb_poller

        Should that, or the end_work flag be captured, return to end this thread
    '''
    try:
        if threads is not None and threads:
            for thread in threads:
                join(thread)
        inputs = ['-i', file_raw]
        input_id = 0
        mappers = []
        for stream_id, stream in enumerate(streams):
            if work_end:
                return
            if stream is None:
                mappers += ['-map', f'0:{stream_id}']
            else:
                inputs += ['-i', stream]
                input_id += 1
                mappers += ['-map', f'{input_id}']
        cleanPrinter.print(f'[{file_raw.name}] Muxing to {file_out}...')
        args = ('ffmpeg', *inputs, '-c', 'copy', *mappers, '-map_metadata', '0', '-y', file_cache)
        while ffmpeg_dumb_poller(subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)):
            if work_end:
                return
            cleanPrinter.print(f'[{file_raw.name}] Muxing failed, retry that later')
            time.sleep(5)
    except EndExecution:
        return
    shutil.move(file_cache, file_out)
    cleanPrinter.print(f'[{file_raw.name}] Muxing finished')


def screenshooter(
    file_raw: pathlib.Path,
    file_cache: pathlib.Path,
    file_out: pathlib.Path,
    stream_id: int,
    duration: datetime.timedelta,
    stream_width: int,
    stream_height: int
):
    '''Taking screenshot for video stream, according to its duration, making mosaic screenshot

    screenshooter itself (scope: child/screenshooter)

    scope: child-main
        As the invoker of other child functions, EndExecution exception raised by child functions will be captured here:
            cpu_wait
            ffmpeg_dumb_poller

        Should that, or the end_work flag be captured, return to end this thread
    '''
    args_ffmpeg = ('ffmpeg', '-hwaccel', 'auto')
    args_out = ('-vsync', 'passthrough', '-frames:v', '1', '-y', file_cache)
    length = clamp(int(math.log(duration.total_seconds()/2 + 1)), 1, 10)
    if length == 1:
        prompt_title = f'[{file_raw.name}]'
        args = (*args_ffmpeg, '-ss', str(duration/2), '-i', file_raw, '-map', f'0:{stream_id}', *args_out)
        prompt = f'{prompt_title} Taking screenshot, single frame'
    else:
        prompt_title = f'[{file_raw.name}] S:{stream_id}'
        width = stream_width * length
        height = stream_height * length
        if width > 65535 or length > 65535:
            while width > 65535 or length > 65535:
                if work_end:
                    return
                width //= 2
                length //= 2
            arg_scale = f',scale={width}x{height}'
        else:
            arg_scale = ''
        tiles = length**2
        time_delta = duration / tiles
        if duration < datetime.timedelta(seconds=tiles*5):
            args = (
                *args_ffmpeg, '-i', file_raw, '-map', f'0:{stream_id}', '-filter:v', f'select=eq(n\,0)+gte(t-prev_selected_t\,{time_delta.total_seconds()}),tile={length}x{length}{arg_scale}', *args_out
            )
        else:
            time_start = time_zero
            args_input = []
            args_position = []
            args_mapper = []
            file_id = 0
            for i in range(length):
                for j in range(length):
                    if work_end:
                        return
                    args_input.extend(('-ss', str(time_start), '-i', file_raw))
                    args_mapper.append(f'[{file_id}:{stream_id}]')
                    args_position.append(f'{j*stream_width}_{i*stream_height}')
                    time_start += time_delta
                    file_id += 1
            arg_mapper = ''.join(args_mapper)
            arg_position = '|'.join(args_position)
            args = (
                *args_ffmpeg, *args_input, '-filter_complex', f'{arg_mapper}xstack=inputs={tiles}:layout={arg_position}{arg_scale}', *args_out
            )
        prompt = f'{prompt_title} Taking screenshot, {length}x{length} grid, for each {time_delta} segment, {width}x{height} res'
    global waitpool_ss
    try:
        while True:
            wait_cpu(lock_ss, waitpool_ss)
            cleanPrinter.print(prompt)
            if ffmpeg_dumb_poller(subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)):
                cleanPrinter.print(f'{prompt_title} Failed to screenshoot, retring that later')
            else:
                break
            time.sleep(5)
    except EndExecution:
        return
    cleanPrinter.print(f'{prompt_title} Screenshot taken')
    shutil.move(file_cache, file_out)


def cleaner(
    dir_work_sub: pathlib.Path,
    file_raw: pathlib.Path,
    threads: list
):
    '''Cleaning the work directory and the raw file

    cleaner itself (scope: child/cleaner)

    scope: child-main
        As the invoker of other child functions, EndExecution exception raised by child functions will be captured here:
            join
            check_end
            db_write

        Should that, or the end_work flag be captured, return to end this thread
    '''
    try:
        for thread in threads:
            join(thread)
        cleanPrinter.print(f'[{file_raw.name}] Cleaning input and {dir_work_sub}...')
        shutil.rmtree(dir_work_sub)
        file_raw.unlink()
        check_end()
        with lock_db:
            check_end()
            if file_raw in db:
                del db[file_raw]
        db_write()
        global work
        check_end()
        with lock_work:
            check_end()
            work.remove(file_raw)
    except EndExecution:
        return
    cleanPrinter.print(f'[{file_raw.name}] Done')

def db_write():
    '''Write the db if it's updated

    used in cleaner (scope: child/cleaner)
    used in main (scope: main)
    used in scan_dir (scope: main)
    
    scope: child
        The EndExecution exception could be raised by wait_end, we pass it as is

    scope: main
        End when KeyboardException is captured
    '''
    global db_last
    check_end()
    with lock_db:
        check_end()
        if db != db_last:
            check_end()
            with open(file_db, 'wb') as f:
                check_end()
                pickle.dump(db, f)
            cleanPrinter.print('[Database] Saved')
            db_last = {i:j for i, j in db.items()}

            
def scheduler():
    '''Schedule the CPU resource and wake up the sleeping threads if promising CPU resource is available

    scheduler itself (scope: child/scheduler)

    scope: child-main
        As the invoker of other child functions, EndExecution exception raised by child functions will be captured here:
            check_end

        Should that, or the end_work flag be captured, end CPU resource polling and wake up all sleeping threads so they can capture the end_work flag and end their work.
    '''
    global waitpool_264
    global waitpool_av1
    global work_end
    cleanPrinter.print('[Scheduler] Started')
    while not work_end:
        cpu_percent = psutil.cpu_percent()
        try:
            while cpu_percent < 50 and waitpool_264:
                check_end()
                with lock_264:
                    check_end()
                    waitpool_264.pop(0).set()
                time.sleep(5)
                cpu_percent = psutil.cpu_percent()
            while cpu_percent < 60 and waitpool_av1:
                check_end()
                with lock_av1:
                    check_end()
                    waitpool_av1.pop(0).set()
                time.sleep(5)
                cpu_percent = psutil.cpu_percent()
            while cpu_percent < 90 and waitpool_ss:
                check_end()
                with lock_ss:
                    check_end()
                    waitpool_ss.pop(0).set()
                time.sleep(5)
                cpu_percent = psutil.cpu_percent()
        except EndExecution:
            break
        time.sleep(5)
    for waitpool in waitpool_264, waitpool_av1, waitpool_ss:
        while waitpool:
            waitpool.pop(0).set()
    cleanPrinter.print('[Scheduler] Exited')

def thread_adder(dir_work_sub, file_raw, encode_type, stream_id, stream_type, stream_duration, stream_size, stream_lossless, stream_width, stream_height, streams, threads, amix=False):
    '''Add a certainer encoder thread to threads, and start it just then.

    used in main (scope: main)
    
    scope: main
        End when KeyboardException is captured
    '''
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
    '''Cleaning the db, remove files not existing or already finished

    used in main (scope: main)
    
    scope: main
        End when KeyboardException is captured
    '''
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
                    with lock_work:
                        work.append(i)
            time.sleep(5)
    except KeyboardInterrupt:
        cleanPrinter.print('[Main] Keyboard Interrupt received, exiting safely...')
        db_write()
        work_end = True