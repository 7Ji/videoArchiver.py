if __name__ != '__main__':
    raise RuntimeError('REFUSED to be imported, you should only run this as sciprt! (i.e. python videoArchiver.py, or python -m videoArchiver) Most functions are heavily dependent on global variables only set in the main body to reduce memory usage. If you really want to test this as a module, you could capture this RuntimeError, but videoArchiver.py is not guaranteed to not fuck up your computer')
    

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
import logging
# The following modules are written
import duration

time_zero = datetime.timedelta()
time_second = datetime.timedelta(seconds=1)

class EndExecution(RuntimeError):
    pass


class CleanPrinter:
    """Fancier printer, the main purpose of this class is to cleanly print message
    """

    bars = 0
    lock_bar = threading.Lock()
    width = shutil.get_terminal_size()[0]

    @staticmethod
    def clear_line():
        """Always clean the line before printing, this is to avoid the unfinished progress bar being stuck on the screen
        """
        print(' ' * CleanPrinter.width, end='\r')

    @staticmethod
    def print(content, loglevel=logging.INFO, end=None):
        if CleanPrinter.bars: # Only clear line if there are progress bars being displayed
            CleanPrinter.clear_line()
        if end is None:
            print(content)
        else:
            print(content, end=end)
        match loglevel:
            case logging.CRITICAL:
                logging.critical(content)
            case logging.ERROR:
                logging.error(content)
            case logging.WARNING:
                logging.warning(content)
            case logging.INFO:
                logging.info(content)
            case logging.DEBUG:
                logging.debug(content)
            case _:
                pass


def clamp(n, minimum, maximum):
    return min(max(n, minimum), maximum)

class LoggingWrapper:
    """Wrapper for printing and logging, ease the pain to type prompt_title every time
    """
    def __init__(self, title='[Title]'):
        if title[-1] != ' ':
            title += ' '
        self.title = title

    def critical(self, content):
        CleanPrinter.print(f'{self.title}{content}', loglevel=logging.CRITICAL)

    def error(self, content):
        CleanPrinter.print(f'{self.title}{content}', loglevel=logging.ERROR)

    def warning(self, content):
        CleanPrinter.print(f'{self.title}{content}', loglevel=logging.WARNING)

    def info(self, content):
        CleanPrinter.print(f'{self.title}{content}', loglevel=logging.INFO)

    def debug(self, content):
        logging.debug(f'{self.title}{content}')



class ProgressBar(CleanPrinter):
    """Progress bar with title, percent, spent/estimate time
    """
    def __init__(self, log: LoggingWrapper = '[Title]'):
        with CleanPrinter.lock_bar:
            CleanPrinter.bars += 1
        self.log = log
        log.debug('Encoder started')
        self.title_length = len(log.title)
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
            CleanPrinter.clear_line()
            with CleanPrinter.lock_bar:
                CleanPrinter.bars -= 1
            self.log.debug('Encoder exited')
        else:
            width = shutil.get_terminal_size()[0]
            width_update = width != CleanPrinter.width
            if update or width_update:
                if width_update:
                    CleanPrinter.width = width 
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
                    self.bar_length = CleanPrinter.width - self.title_length - 4 * self.display_percent - 11 * self.display_spent - 11 * self.display_estimate
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
            line = [self.log.title, self.bar_complete_str, self.bar_incomplete_str]
            if self.display_spent:
                time_spent = datetime.datetime.today() - self.time_start
                if time_spent - self.time_spent > time_second:
                    self.time_spent = time_spent
                    self.time_spent_str = ' S:' + str_from_time(time_spent)
                    if self.display_estimate and self.percent != 0:
                        self.time_estimate = time_spent / self.percent - time_spent
                        self.time_estimate_str = ' R:' + str_from_time(self.time_estimate)
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
        self.log.debug(f'Encoding at {self.percent:%}')
        self.display()

    def set_fraction(self, numerator, denominator):
        if type(numerator) == type(denominator):
            percent = numerator / denominator
            if percent != self.percent:
                self.percent = clamp(percent, 0, 1)
            self.log.debug(f'Encoding {numerator}/{denominator} at {self.percent:%}')
        else:
            self.log.debug(f'Encoding at {self.percent:%}')
        self.display()


# def check_end_kill(p:subprocess.Popen):
#     """Check if the work_end flag is set, and if it is, raise the EndExecution exception and kill the Popen object

#     used in ffmpeg_dumb_poller (scope: child)
#         used in stream_copy (scope: child/encoder)
#         used in encoder (scope: child/endoer)
#         used in muxer (scope: child/muxer)
#         used in screenshooter (scopte: child/screenshooter)
    
#     used in ffmpeg_time_size_poller (scope: main + child)
#         used in encoder (scope: child/encoder)
#         used in get_duration_and_size (wrapper)
#             used in stream_info (scope: main)
#             used in delta_adder (scope: child/encoder)
#             used in encoder (scope: child/encoder)

#     used in encoder (scope: child/encoder)
#     used in muxer (scope: child/ muxer)
#     used in scneenshooter (scope: child/screenshooter)

#     scope: child
#         Should the work_end flag be captured, raise the EndExecution exception
#     """
#     if work_end:
#         p.kill()
#         logging.debug(f'[Subprocess] Killed {p}')
#         raise EndExecution

# def check_end():
#     """Check if the work_end flag is set, and if it is, raise the EndExecution exception

#     used in stream_copy (scope: child/encoder)
#     used in concat (scope: child/encoder)
#     used in wait_cpu (scope: child)
#         used in encoder (scope: child/encoder)
#         used in screenshooter (scope: child/screenshooter)
#     used in cleaner (scope: child/cleaner)
#     used in db_write (scope: child/cleaner + main)
#         used in cleaner (scope: child/cleaner)
#         used in main (scope: main)
#         used in scan_dir (scope: main)
#     used in scheduler (scope: child/scheduler)

#     scope: child
#         Should the work_end flag be captured, raise the EndExecution exception
#     """
#     if work_end:
#         raise EndExecution


class Ffprobe:
    path = pathlib.Path(shutil.which('ffprobe'))
    log = LoggingWrapper('[Subprocess]')
    def __init__(self, args):
        self.p = subprocess.Popen((self.path, *args), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        self.poll_stdout()

    def poll_dumb(self):
        while self.p.poll() is None:
            Checker.is_end(self.p)
            Checker.sleep(1)
        self.returncode = self.p.wait()

    def poll_stdout(self):
        chars = []
        reader = self.p.stdout
        while True:
            Checker.is_end(self.p)
            char = reader.read(1)
            if char == b'':
                break
            chars.append(char)
        self.stdout = b''.join(chars)
        self.returncode = self.p.wait()

class Ffmpeg(Ffprobe):  # Note: as for GTX1070 (Pascal), nvenc accepts at most 3 h264 encoding work

    path = pathlib.Path(shutil.which('ffmpeg'))
    reg_complete = {
        'video': re.compile(r'video:(\d+)kB'),
        'audio': re.compile(r'audio:(\d+)kB')
    }
    reg_running = re.compile(r'size= *(\d+)kB')
    reg_time = re.compile(r' time=([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{2}) ')
    time_ten_seconds = datetime.timedelta(seconds=10)

    def __init__(self, args, null=False):
        if null:
            self.p = subprocess.Popen((self.path, *args), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            self.p = subprocess.Popen((self.path, *args), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    def poll_dumb(self):
        """Polling the ffmpeg endlessly until it is done, do nothing other than trying to capture the work_end flag

        used in stream_copy (scope: child/encoder)
        used in encoder (scope: child/endoer)
        used in muxer (scope: child/muxer)
        used in screenshooter (scopte: child/screenshooter)

        scope: child
            The EndExecution exception could be raised by check_end_kill, we pass it as is
        """
        super().poll_dumb()
        return self.returncode
    
    def poll_time_size(self, stream_type:str, size_allow:int=None, progress_bar:ProgressBar=None, target_time:datetime.timedelta=None):
        """Polling time and size information from a running ffmpeg subprocess.Popen

        used in encoder (scope: child/encoder)
        used in get_duration_and_size (wrapper)
            used in stream_info (scope: main)
            used in delta_adder (scope: child/encoder)
            used in encoder (scope: child/encoder)

        scope: main
            End when KeyboardException is captured
        scope: child
            The EndExecution exception could be raised by check_end_kill, we pass it as is
        """
        Ffmpeg.log.debug(f'Started {self.p}')
        if size_allow is not None:
            inefficient = False
        check_time = size_allow is None or (size_allow is not None and progress_bar is not None)
        reader = self.p.stderr
        if check_time:
            t = time_zero
            percent = 1
        while self.p.poll() is None:
            Checker.is_end(self.p)
            chars = []
            while True:
                Checker.is_end(self.p)
                char = reader.read(1)
                if char in (b'\r', b''):
                    break
                elif char != b'\n':
                    chars.append(char)
            if chars:
                line = b''.join(chars).decode('utf-8')
                if check_time:
                    m = Ffmpeg.reg_time.search(line)
                    if m:
                        t = m[1]
                        if progress_bar is not None:
                            t = time_from_str(t)
                            percent = t/target_time
                            progress_bar.set_fraction(t, target_time)
                if size_allow is not None:
                    m = Ffmpeg.reg_running.search(line)
                    if m:
                        size_produded = int(m[1])
                        if size_produded >= size_allow or (
                            check_time and t > Ffmpeg.time_ten_seconds and size_produded/size_allow >= percent):
                            inefficient = True
                            self.p.kill()
                            break
                    pass
            else:
                self.p.kill()
                break
        Ffmpeg.log.debug(f'Ended {self.p}')
        m = Ffmpeg.reg_running.search(line)
        if m:
            s = int(m[1])
            if size_allow is not None and s >= size_allow:
                inefficient = True
        else:
            m = Ffmpeg.reg_complete[stream_type].search(line)
            if m:
                s = int(m[1])
                if size_allow is not None and s >= size_allow:
                    inefficient = True
        if progress_bar is not None:
            progress_bar.set(1)
        self.returncode = self.p.wait()
        if size_allow is None:
            if progress_bar is None:
                return self.returncode, time_from_str(t), s
            else:
                return self.returncode, t, s
        else:
            return inefficient


class Stream:
    lossless = {
        'video': ('012v', '8bps', 'aasc', 'alias_pix', 'apng', 'avrp', 'avui', 'ayuv', 'bitpacked', 'bmp', 'bmv_video', 'brender_pix', 'cdtoons', 'cllc', 'cscd', 'dpx', 'dxa', 'dxtory', 'ffv1', 'ffvhuff', 'fits', 'flashsv', 'flic', 'fmvc', 'fraps', 'frwu', 'gif', 'huffyuv', 'hymt', 'lagarith', 'ljpeg', 'loco', 'm101', 'magicyuv', 'mscc', 'msp2', 'msrle', 'mszh', 'mwsc', 'pam', 'pbm', 'pcx', 'pfm', 'pgm', 'pgmyuv', 'pgx', 'png', 'ppm', 'psd', 'qdraw', 'qtrle', 'r10k', 'r210', 'rawvideo', 'rscc', 'screenpresso', 'sgi', 'sgirle', 'sheervideo', 'srgc', 'sunrast', 'svg', 'targa', 'targa_y216', 'tiff', 'tscc', 'utvideo', 'v210', 'v210x', 'v308', 'v408', 'v410', 'vble', 'vmnc', 'wcmv', 'wrapped_avframe', 'xbm', 'xpm', 'xwd', 'y41p', 'ylc', 'yuv4', 'zerocodec', 'zlib', 'zmbv'),
        'audio': ('alac', 'ape', 'atrac3al', 'atrac3pal', 'dst', 'flac', 'mlp', 'mp4als', 'pcm_bluray', 'pcm_dvd', 'pcm_f16le', 'pcm_f24le', 'pcm_f32be', 'pcm_f32le', 'pcm_f64be', 'pcm_f64le', 'pcm_lxf', 'pcm_s16be', 'pcm_s16be_planar', 'pcm_s16le', 'pcm_s16le_planar', 'pcm_s24be', 'pcm_s24daud', 'pcm_s24le', 'pcm_s24le_planar', 'pcm_s32be', 'pcm_s32le', 'pcm_s32le_planar', 'pcm_s64be', 'pcm_s64le', 'pcm_s8', 'pcm_s8_planar', 'pcm_sga', 'pcm_u16be', 'pcm_u16le', 'pcm_u24be', 'pcm_u24le', 'pcm_u32be', 'pcm_u32le', 'pcm_u8', 'ralf', 's302m', 'shorten', 'tak', 'truehd', 'tta', 'wmalossless')
    }

    @staticmethod
    def get_duration_and_size(path: pathlib.Path, s_id: int, s_type: str):
        """Get duration and size from a stream from a media file, just a wrapper, 

        used in stream_info (scope: main)
        used in delta_adder (scope: child/encoder)
        used in encoder (scope: child/encoder)

        scope: main
            End when KeyboardException is captured
        scope: child
            The EndExecution exception could be raised by ffmpeg_time_size_poller, we pass it as is
        """
        #ffmpeg.popen('-i', media, '-c', 'copy', '-map', f'0:{stream_id}', '-f', 'null', '-')
        r, t, s = Ffmpeg(('-i', path, '-c', 'copy', '-map', f'0:{s_id}', '-f', 'null', '-'), ).poll_time_size(s_type)
        return t, s

    def __init__(self, path:pathlib.Path, stream_id:int, stream_info:dict):
        self.path = path
        self.id = stream_id
        if stream_info['codec_type'] in ('video', 'audio'):
            self.type = stream_info['codec_type']
            self.duration, self.size = Stream.get_duration_and_size(path, self.id, self.type)
            self.lossless = Stream.lossless[self.type]
            if self.type == 'video':
                self.width = stream_info['width']
                self.height = stream_info['height']
                if 'side_data_list' in stream_info and 'rotation' in stream_info['side_data_list'][0]:
                    self.rotation = stream_info['side_data_list'][0]['rotation']
                else:
                    self.rotation = None
        else:
            self.type = None

    def copy(self, log, dir_work_sub, prefix, file_out, file_done):
        """Copy a stream from a media file as is, 

        used in encoder (scope: child/encoder)

        scope: child
            The EndExecution exception could be raised by ffmpeg_dumb_poller and check_end, we pass it as is
        """
        log.info('Transcode inefficient, copying raw stream instead')
        file_copy = pathlib.Path(
            dir_work_sub,
            f'{prefix}_copy.nut'
        )
        args = ('-i', self.path, '-c', 'copy', '-map', f'0:{self.id}', '-y', file_copy)
        while Ffmpeg(args, null=True).poll_dumb():
            Checker.is_end()
            log.warning('Stream copy failed, trying that later')
            Checker.sleep(5)
        shutil.move(file_copy, file_out)
        log.info('Stream copy done')
        file_done.touch()


def concat(log, prefix, concat_list, file_out, file_done):
    """Concating multiple parts of a media file

    used in encoder (scope: child/encoder)

    scope: child
        The EndExecution exception could be raised by check_end and ffmpeg_dumb_poller, we pass it as is
    """
    log.info('Transcode done, concating all parts')
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
            Checker.is_end()
            f.write(f'file {file}\n')
    args = ('-f', 'concat', '-safe', '0', '-i', file_list, '-c', 'copy', '-map', '0', '-y', file_concat)
    while Ffmpeg(args, null=True).poll_dumb():
        Checker.is_end()
        log.warning('Concating failed, trying that later')
        Checker.sleep(5)
    shutil.move(file_concat, file_out)
    log.info('Concating done')
    file_done.touch()


def args_constructor(start, file_raw, file_out, stream_id, stream_type, encode_type, stream_width, stream_height):
    """Counstructing ffmpeg args for encoder
    
    used in encoder (scope: child/encoder)
    
    scope: child
        No exception should be raised, the outer thread is ended by other things
    """
    args_in = ('-hwaccel', 'auto', '-ss', str(start), '-i', file_raw)
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
    """Check if a file is being opened, if it is, wait until it's closed

    used in scan_dir (scope: main)

    scope: main
        End when KeyboardException is captured
    """
    size_old = file_raw.stat().st_size
    hint = False
    log = LoggingWrapper('[Scanner]')
    while True:
        opened = False
        for p in psutil.process_iter():
            try:
                for f in p.open_files():
                    if file_raw.samefile(f.path):
                        if not hint:
                            log.warning(f'Jammed, {file_raw} is opened by {p.pid} {p.cmdline()}')
                            hint = True
                        opened = True
                        break
            except (psutil.NoSuchProcess, psutil.AccessDenied, PermissionError):
                pass
        size_new = file_raw.stat().st_size
        if size_new != size_old:
            opened = True
            if not hint:
                log.warning(f'Jammed, {file_raw} is being written')
        if not opened:
            if hint:
                log.info(f'{file_raw} closed writing, continue scanning')
            break
        size_old = size_new
        Checker.sleep(5)


def scan_dir(d: pathlib.Path):
    """Recursively scan dirs, 

    used in main (scope: main)
    used in scan_dir recursively (scope: main)
    
    scope: main
        End when KeyboardException is captured
    """
    log_scanner.debug(f'Scanning {d}')
    for i in d.iterdir():
        if i.is_dir():
            scan_dir(i)
        elif i.is_file() and i not in Database.dict():
            wait_close(i)
            db_entry = None
            if i not in Database.dict():
                log_scanner.info(f'Discovered {i}')
                try:
                    p = Ffprobe(('-show_format', '-show_streams', '-select_streams', 'V', '-of', 'json', i))
                    if p.returncode == 0:
                        j = json.loads(p.stdout)
                        j_format =  j['format']['format_name']
                        if not (j_format.endswith('_pipe') or j_format in ('image2', 'tty')) and j['streams']:
                            streams = []
                            video = False
                            for s_id, s in enumerate(
                                json.loads(Ffprobe(('-show_streams', '-of', 'json', i)).stdout)['streams']
                            ):
                                log_scanner.info(f'Getting stream information for stream {stream_id} from {i}')
                                streams.append(Stream(i, s_id, s))
                                if not video and s['codec_type'] == 'video':
                                    video = True
                            if video:
                                log_scanner.info(f'Added {i} to db, {len(streams)} streams')
                                log_scanner.debug(f'{i} streams: {streams}')
                                db_entry = streams
                except EndExecution:
                    return
            Database.add(i, db_entry)


# def delta_adder(file_check, stream_type, start, size_exist):
#     """Get duration and size from a file, add them to existing value, return the total

#     used in encoder (scope: child/encoder)

#     scope: child
#         The EndExecution exception could be raised by get_duration_and_size, we pass it as is
#     """
#     time_delta, size_delta = get_duration_and_size(file_check, 0, stream_type)
#     return start + time_delta, size_exist + size_delta


def encoder(
    dir_work_sub: pathlib.Path,
    stream: Stream, file_out: pathlib.Path, file_done: pathlib.Path,
    encode_type: str, 
):  
    """Encoding certain stream in a media file

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

        Should that, return to end this thread
    """
    log = LoggingWrapper(f'[{stream.path.name}]')
    try:
        if encode_type == 'preview' and stream.type == 'audio':
            log = LoggingWrapper(f'[{stream.path.name}] E:P S:A:{stream.id}')
        else:
            log = LoggingWrapper(f'[{stream.path.name}] E:{encode_type[:1].capitalize()} S:{stream.id}:{stream.type[:1].capitalize()}')
        log.info('Work started')
        if encode_type == 'preview' and stream.type == 'audio':
            prefix = f'{stream.path.stem}_preview_audio'
        else:
            prefix = f'{stream.path.stem}_{encode_type}_{stream.id}_{stream.type}'
        file_concat_pickle = dir_work_sub / f'{prefix}_concat.pkl'
        start = time_zero
        size_exist = 0
        concat_list = []
        check_efficiency = encode_type == 'archive'  and not stream.lossless 
        if file_out.exists() and file_out.stat().st_size:
            log.warning('Output already exists, potentially broken before, trying to recover it')
            time_delta, size_delta = Stream.get_duration_and_size(file_out, 0, stream.type)
            log.debug(f'Recovery: {file_out}, stream.duration: {time_delta}, size: {size_delta}kB')
            if abs(stream.duration - time_delta) < time_second:
                log.info('Last transcode successful, no need to transcode')
                file_done.touch()
                return
            suffix = 0
            file_check = dir_work_sub / f'{prefix}_{suffix}.nut'
            while file_check.exists() and file_check.stat().st_size:
                Checker.is_end()
                time_delta, size_delta = Stream.get_duration_and_size(file_check, 0, stream.type)
                start += time_delta
                size_exist += size_delta
                log.warning(f'Recovery: {file_check}, stream.duration: {time_delta}, size: {size_delta}kB. Total: stream.duration:{start}, size: {size_exist}kB')
                concat_list.append(file_check.name)
                suffix += 1
                file_check = dir_work_sub / f'{prefix}_{suffix}.nut'
            log.info(f'Recovering last part to {file_check}')
            file_recovery = dir_work_sub / f'{prefix}_recovery.nut'
            poll = False
            if stream.type == 'video':
                log.info('Checking if the interrupt file is usable')
                p = Ffprobe(('-show_frames', '-select_streams', 'v:0', '-of', 'json', file_out))
                if p.returncode == 0:
                    log.info('Analyzing available frames')
                    frames = json.loads(p.stdout)['frames']
                    log.debug(f'{len(frames)} frames found in {file_out}')
                    frame_last = 0
                    for frame_id, frame in enumerate(reversed(frames)):
                        Checker.is_end()
                        if frame['key_frame']:
                            frame_last = len(frames) - frame_id - 1
                            break
                    log.debug(f'Last GOP start at frame: {frame_last}')
                    if frame_last:
                        log.info(f'{frame_last} frames seem usable, trying to recovering those')
                        poll = True
                        p = Ffmpeg(('-i', file_out, '-c', 'copy', '-map', '0', '-y', '-vframes', str(frame_last), file_recovery))
                    else:
                        log.info('No frames usable')
            else:
                poll = True
                p = Ffmpeg(('-i', file_out, '-c', 'copy', '-map', '0', '-y', file_recovery))
            if poll:
                r, t, s = p.poll_time_size(stream.type)
                if r:
                    log.warning('Recovery failed')
                else:
                    shutil.move(file_recovery, file_check)
                    start += t
                    size_exist += s
                    concat_list.append(file_check.name)
                    log.info(f'{t} of failed transcode recovered. Total: stream.duration:{start}, size{size_exist}')
            file_out.unlink()
            with file_concat_pickle.open('wb') as f:
                pickle.dump((concat_list, start, size_exist), f)
        if not concat_list and file_concat_pickle.exists():
            with file_concat_pickle.open('rb') as f:
                concat_list, start, size_exist = pickle.load(f)
        if concat_list:
            # We've already transcoded this
            if check_efficiency and stream.size and size_exist > stream.size * 0.9:
                stream.copy(log, dir_work_sub, prefix, file_out, file_done)
                return 
            if start >= stream.duration or stream.duration - start < time_second:
                log.info(f'Seems already finished, concating all failed parts')
                concat(log, prefix, concat_list, file_out, file_done)
                return
        # Real encoding happenes below
        if check_efficiency:
            size_allow = stream.size * 0.9 - size_exist
        args = args_constructor(start, stream.path, file_out, stream.id, stream.type, encode_type, stream.width, stream.height)
        target_time = stream.duration - start
        while True:
            Checker.is_end()
            if stream.type == 'video':
                Pool.wait(log, encode_type)
            p = Ffmpeg(args)
            progress_bar = ProgressBar(log)
            if check_efficiency:
                if p.poll_time_size(stream.type, size_allow, progress_bar, target_time):  # return ture for inefficient
                    stream.copy(log, dir_work_sub, prefix, file_out, file_done)
                    return
            else:
                p.poll_time_size(stream.type, progress_bar=progress_bar, target_time=target_time)
            #CleanPrinter.print(f'{prompt_title} waiting to end')
            if p.returncode == 0:
                if concat_list:
                    concat_list.append(file_out.name)
                    concat(log, prefix, concat_list, file_out, file_done)
                else:
                    log.info('Transcode done')
                    file_done.touch()
                #CleanPrinter.print(f'{prompt_title} ended {file_done}')
                break
            log.info(f'Transcode failed, returncode: {p.returncode}, retrying that later')
            Checker.sleep(5)
    except EndExecution:
        log.debug(f'Ending thread {threading.current_thread()}')
        return


def join(thread: threading.Thread):
    """Cleanly join a thread, just an invoker so that the work_end flag can be captured

    used in muxer (scope: child/muxer)
    used in cleaner (scope: child/cleaner)

    scope: child
        The EndExecution flag could be raised by check_end, we pass it as is
    """
    Checker.is_end()
    thread.join()
    Checker.is_end()


def muxer(
    file_raw: pathlib.Path,
    file_cache: pathlib.Path,
    file_out: pathlib.Path,
    streams: list,
    threads: list = None,
):
    """Muxing finished video/audio streams and non va streams from raw file to a new mkv container

    muxer itself (scopte: child/muxer)

    scope: child-main
        As the invoker of other child functions, EndExecution exception raised by child functions will be captured here:
            join
            check_end
            ffmpeg_dumb_poller

        Should that, return to end this thread
    """
    log = LoggingWrapper(f'[{file_raw.name}]')
    try:
        if threads is not None and threads:
            for thread in threads:
                join(thread)
        inputs = ['-i', file_raw]
        input_id = 0
        mappers = []
        for stream_id, stream in enumerate(streams):
            Checker.is_end()
            if stream is None:
                mappers += ['-map', f'0:{stream_id}']
            else:
                inputs += ['-i', stream]
                input_id += 1
                mappers += ['-map', f'{input_id}']
        log.info(f'Muxing to {file_out}...')
        args = (*inputs, '-c', 'copy', *mappers, '-map_metadata', '0', '-y', file_cache)
        while Ffmpeg(args, null=True).poll_dumb():
            Checker.is_end()
            log.warning(f'Muxing failed, retry that later')
            Checker.sleep(5)
    except EndExecution:
        log.debug(f'Ending thread {threading.current_thread()}')
        return
    shutil.move(file_cache, file_out)
    log.info(f'Muxed to {file_out}')


def screenshooter(
    file_raw: pathlib.Path,
    file_cache: pathlib.Path,
    file_out: pathlib.Path,
    stream_id: int,
    duration: datetime.timedelta,
    stream_width: int,
    stream_height: int,
    rotation: int
):
    """Taking screenshot for video stream, according to its duration, making mosaic screenshot

    screenshooter itself (scope: child/screenshooter)

    scope: child-main
        As the invoker of other child functions, EndExecution exception raised by child functions will be captured here:
            cpu_wait
            ffmpeg_dumb_poller
            check_end

        Should that, return to end this thread
    """
    try:
        args_hwaccel = ('-hwaccel', 'auto')
        args_out = ('-vsync', 'passthrough', '-frames:v', '1', '-y', file_cache)
        length = clamp(int(math.log(duration.total_seconds()/2 + 1)), 1, 10)
        if length == 1:
            log = LoggingWrapper(f'[{file_raw.name}]')
            args = (*args_hwaccel, '-ss', str(duration/2), '-i', file_raw, '-map', f'0:{stream_id}', *args_out)
            prompt = 'Taking screenshot, single frame'
        else:
            log = LoggingWrapper(f'[{file_raw.name}] S:{stream_id}')
            if rotation in (90, -90):
                stream_width, stream_height = stream_height, stream_width
            width = stream_width * length
            height = stream_height * length
            if width > 65535 or length > 65535:
                while width > 65535 or length > 65535:
                    Checker.is_end()
                    width //= 2
                    length //= 2
                arg_scale = f',scale={width}x{height}'
            else:
                arg_scale = ''
            tiles = length**2
            time_delta = duration / tiles
            if duration < datetime.timedelta(seconds=tiles*5):
                args = (
                    *args_hwaccel, '-i', file_raw, '-map', f'0:{stream_id}', '-filter:v', f'select=eq(n\,0)+gte(t-prev_selected_t\,{time_delta.total_seconds()}),tile={length}x{length}{arg_scale}', *args_out
                )
            else:
                time_start = time_zero
                args_input = []
                args_position = []
                args_mapper = []
                file_id = 0
                for i in range(length):
                    for j in range(length):
                        Checker.is_end()
                        args_input.extend(('-ss', str(time_start), '-i', file_raw))
                        args_mapper.append(f'[{file_id}:{stream_id}]')
                        args_position.append(f'{j*stream_width}_{i*stream_height}')
                        time_start += time_delta
                        file_id += 1
                arg_mapper = ''.join(args_mapper)
                arg_position = '|'.join(args_position)
                args = (
                    *args_hwaccel, *args_input, '-filter_complex', f'{arg_mapper}xstack=inputs={tiles}:layout={arg_position}{arg_scale}', *args_out
                )
            prompt = f'Taking screenshot, {length}x{length} grid, for each {time_delta} segment, {width}x{height} res'
        while True:
            Pool.wait(log, 'ss')
            log.info(prompt)
            if Ffmpeg(args, null=True).poll_dumb():
                log.warning('Failed to screenshoot, retring that later')
            else:
                break
            Checker.sleep(5)
    except EndExecution:
        log.debug(f'Ending thread {threading.current_thread()}')
        return
    log.info('Screenshot taken')
    shutil.move(file_cache, file_out)


def cleaner(
    dir_work_sub: pathlib.Path,
    file_raw: pathlib.Path,
    threads: list
):
    """Cleaning the work directory and the raw file

    cleaner itself (scope: child/cleaner)

    scope: child-main
        As the invoker of other child functions, EndExecution exception raised by child functions will be captured here:
            join
            check_end
            db_write

        Should that, or the end_work flag be captured, return to end this thread
    """
    log = LoggingWrapper(f'[{file_raw.name}]')
    try:
        for thread in threads:
            join(thread)
        log.info(f'Cleaning input and {dir_work_sub}...')
        shutil.rmtree(dir_work_sub)
        file_raw.unlink()
        Checker.is_end()
        Database.remove(file_raw)
        Pool.Work.remove(file_raw)
    except EndExecution:
        log.debug(f'Ending thread {threading.current_thread()}')
        return
    log.info('Done')


class Checker:
    end_flag = False
    @staticmethod
    def is_end(p:subprocess.Popen=None):
        if Checker.end_flag:
            if p is not None:
                p.kill()
            raise EndExecution

    @staticmethod
    def end():
        Checker.end_flag = True

    @staticmethod
    def sleep(t:int=5):
        while t:
            Checker.is_end()
            time.sleep(1)
            t -= 1


class Database:
    lock = threading.RLock()
    log = LoggingWrapper('[Database]')
    db = {}
    backend = ''
    
    @staticmethod
    def add(key, value):
        Checker.is_end()
        with Database.lock:
            Checker.is_end()
            if key not in Database.db:
                Database.db[key] = value
                Database.log.info(f'Added {key}')
                Database.write()

    @staticmethod
    def remove(key):
        Checker.is_end()
        with Database.lock:
            Checker.is_end()
            if key in Database.db:
                del Database.db[key]
                Database.log.info(f'Removed {key}')
                Database.write()

    @staticmethod
    def dict():
        """Return the database as dict, currently this just means return the db
        """
        return Database.db

    @staticmethod
    def read():
        if Database.backend.exists():
            Checker.is_end()
            with open(Database.backend, 'rb') as f:
                Checker.is_end()
                Database.db = pickle.load(f)

    @staticmethod       
    def write():
        """Write the db if it's updated

        used in cleaner (scope: child/cleaner)
        used in main (scope: main)
        used in scan_dir (scope: main)
        
        scope: child
            The EndExecution exception could be raised by wait_end, we pass it as is

        scope: main
            End when KeyboardException is captured
        """
        Checker.is_end()
        with Database.lock:
            Checker.is_end()
            Database.log.info('Updated')
            Checker.is_end()
            with open(Database.backend, 'wb') as f:
                Checker.is_end()
                pickle.dump(Database.db, f)
            Database.log.info('Saved')

    @staticmethod
    def clean():
        """Cleaning the db, remove files not existing or already finished

        used in main (scope: main)
        
        scope: main
            End when KeyboardException is captured
        """
        Checker.is_end()
        with Database.lock:
            Checker.is_end()
            db_new = {}
            for i_r, j_r in {i:j for i, j in Database.db.items() if i.exists()}.items():
                Checker.is_end()
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
                                Checker.is_end()
                                if stream is not None and stream['type'] == 'video':
                                    if not (dir_screenshot_sub / f'{i_r.stem}_{stream_id}.jpg').exists():
                                        finish = False
                                        break
                if finish and i_r.exists():
                    i_r.unlink()
                    Database.log.warning(f'Purged already finished video {i_r}')
                else:
                    db_new[i_r] = j_r
            if Database.db != db_new:
                Database.db = db_new
                Database.write()
        


class Pool:
    pool_264, pool_av1, pool_ss, pool_work = ([] for i in range(4))
    lock_264, lock_av1, lock_ss, lock_work = (threading.Lock() for i in range(4))
    cpu_percent = 0
    cpu_264 = 50
    cpu_av1 = 60
    cpu_ss = 90
    prompt_264 = 'Waked up an x264 encoder'
    prompt_av1 = 'Waked up an av1 encoder'
    prompt_ss = 'Waked up a screenshooter'
    log = LoggingWrapper('[Scheduler]')
    e = threading.Event()

    @staticmethod
    def wake():
        Pool.e.set()

    @staticmethod
    def update_cpu_percent():
        Pool.cpu_percent = psutil.cpu_percent()
        Pool.log.debug(f'CPU usage: {Pool.cpu_percent}')

    @staticmethod
    def waker(pool, lock, cpu_need, prompt):
        while Pool.cpu_percent < cpu_need and pool:
            Checker.is_end()
            with lock:
                Checker.is_end()
                pool.pop(0).set()
                Pool.log.info(prompt)
            Checker.sleep(5)
            Pool.update_cpu_percent()

    @staticmethod
    def wait(log, style):
        """Wait for CPU resource,

        used in encoder (scope: child/encoder)
        used in screenshooter (scope: child/screenshooter)

        scope: child
            The EndExecution exception could be raised by wait_end, we pass it as is
        """
        match style:
            case '264' | 'archive':
                lock = Pool.lock_264
                pool = Pool.pool_264
            case 'av1' | 'preview':
                lock = Pool.lock_av1
                pool = Pool.pool_av1
            case 'ss' | 'screenshot':
                lock = Pool.lock_ss
                pool = Pool.pool_ss
        log.info('Waiting for CPU resources')
        waiter = threading.Event()
        log.debug(f'Waiting for {waiter} to be set')
        Checker.is_end()
        with lock:
            Checker.is_end()
            pool.append(waiter)
        Pool.e.set()
        waiter.wait()
        Checker.is_end()
        log.info('Waked up')
    

    class Work:
        @staticmethod
        def add(entry):
            with Pool.lock_work:
                Pool.pool_work.append(entry)

        @staticmethod
        def remove(entry):
            with Pool.lock_work:
                Pool.pool_work.remove(entry)

        @staticmethod
        def query(entry):
            with Pool.lock_work:
                if entry in Pool.pool_work:
                    return True
            return False

    @staticmethod
    def scheduler():
        Pool.log.info('Started')
        while not work_end:
            try:
                with Pool.lock_264, Pool.lock_av1, Pool.lock_ss:
                    if not Pool.pool_264 and not Pool.pool_av1 and not Pool.pool_ss:
                        Pool.e.clear()
                Checker.is_end()
                Pool.e.wait()
                Checker.is_end()
                Checker.sleep(5)
                Pool.update_cpu_percent()
                Pool.waker(Pool.pool_264, Pool.lock_264, Pool.cpu_264, Pool.prompt_264)
                Pool.waker(Pool.pool_av1, Pool.lock_av1, Pool.cpu_av1, Pool.prompt_av1)
                Pool.waker(Pool.pool_ss, Pool.lock_ss, Pool.cpu_ss, Pool.prompt_ss)
            except EndExecution:
                break
        Pool.log.warning('Terminating, waking up all sleeping threads so they can end themselvies')
        for waitpool in Pool.pool_264, Pool.pool_av1, Pool.pool_ss:
            while waitpool:
                waiter = waitpool.pop(0)
                waiter.set()
                Pool.log.debug(f'Emergency wakeup: {waiter}')
        Pool.log.debug(f'Ending thread {threading.current_thread()}')



def thread_adder(dir_work_sub, file_raw, encode_type, stream_id, stream_type, stream_duration, stream_size, stream_lossless, stream_width, stream_height, streams, threads, amix=False):
    """Add a certainer encoder thread to threads, and start it just then.

    used in main (scope: main)
    
    scope: main
        End when KeyboardException is captured
    """
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
        log_main.debug(f'Spawned thread {threads[-1]}')
        threads[-1].start()
    return streams, threads


# The main function starts here
if __name__ == '__main__':
    dir_raw = pathlib.Path('raw')
    dir_archive = pathlib.Path('archive')
    dir_preview = pathlib.Path('preview')
    dir_screenshot = pathlib.Path('screenshot')
    dir_work = pathlib.Path('work')
    dir_log = pathlib.Path('log')
    for dir in ( dir_raw, dir_archive, dir_preview, dir_screenshot, dir_work, dir_log):
        if not dir.exists():
            dir.mkdir()
    logging.basicConfig(
        filename=dir_log / f'{datetime.datetime.today().strftime("%Y%m%d_%H%M%S")}.log', 
        format='%(asctime)s %(levelname)s: %(message)s',
        level=logging.DEBUG
    )
    work, dirs_work_sub = ([] for i in range(2))
    work_end = False
    Database.backend = dir_work / 'db.pkl'
    Database.read()
    log_main = LoggingWrapper('[Main]')
    log_scanner = LoggingWrapper('[Scanner]')
    log_main.info('Started')
    threading.Thread(target = Pool.scheduler).start()
    try:
        while True:
            Database.clean()
            scan_dir(dir_raw)
            for i, j in Database.dict().items():
                if j is not None and not Pool.Work.query(i):
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
                    threads_archive, threads_preview, threads_muxer, streams_archive, streams_preview, audios =  ([] for i in range(6))
                    videos = {}
                    audios_size = 0
                    audios_duration = time_zero
                    name = f'{i.stem}.mkv'
                    file_archive = dir_archive / name
                    file_preview = dir_preview / name
                    work_archive = not file_archive.exists()
                    work_preview = not file_preview.exists()
                    for stream_id, stream in enumerate(j):
                        if stream.type is None:
                            if work_archive:
                                streams_archive.append(None)
                            if work_preview:
                                streams_preview.append(None)
                        else:
                            if stream['type'] == 'video':
                                videos[stream_id] = stream
                            if work_archive:
                                streams_archive, threads_archive = thread_adder(dir_work_sub, i, 'archive', stream_id, stream['type'], stream['duration'], stream['size'], stream['lossless'], 0, 0, streams_archive, threads_archive)
                            if work_preview:
                                if stream['type'] == 'video':
                                    streams_preview, threads_preview = thread_adder(dir_work_sub, i, 'preview', stream_id, 'video', stream['duration'], stream['size'], stream['lossless'], stream['width'], stream['height'], streams_preview, threads_preview)
                                else:
                                    audios.append(stream_id)
                                    audios_size += stream['size']
                                    audios_duration = max(stream['duration'], audios_duration)
                    threads_screenshot=[]
                    if len(videos) == 1:
                        file_screenshot = dir_screenshot / f'{i.stem}.jpg'
                        stream_id, stream = next(iter(videos.items()))
                        if not file_screenshot.exists():
                            threads_screenshot.append(threading.Thread(target=screenshooter, args=(i, dir_work_sub / f'{i.stem}_screenshot.jpg', file_screenshot, stream_id, stream['duration'], stream['width'], stream['height'], stream['rotation'])))
                            log_main.debug(f'Spawned thread {threads_screenshot[-1]}')
                            threads_screenshot[-1].start()
                    else:
                        dir_screenshot_sub = dir_screenshot / i.stem
                        if not dir_screenshot_sub.exists():
                            dir_screenshot_sub.mkdir()
                        for i_r, j_r in videos.items():
                            file_screenshot = dir_screenshot_sub / f'{i.stem}_{i_r}.jpg'
                            if not file_screenshot.exists():
                                threads_screenshot.append(threading.Thread(target=screenshooter, args=(i, dir_work_sub / f'{i.stem}_screenshot_{i_r}.jpg', file_screenshot, i_r, j_r['duration'], j_r['width'], j_r['height'], j_r['rotation'])))
                                log_main.debug(f'Spawned thread {threads_screenshot[-1]}')
                                threads_screenshot[-1].start()
                    threads_muxer = []
                    if work_archive:
                        threads_muxer.append(threading.Thread(target=muxer, args=(
                            i, dir_work_sub / f'{i.stem}_archive.mkv', file_archive,
                            streams_archive, threads_archive
                        )))
                        log_main.debug(f'Spawned thread {threads_muxer[-1]}')
                        threads_muxer[-1].start()
                    if work_preview:
                        if audios:
                            streams_preview, threads_preview = thread_adder(dir_work_sub, i, 'preview', len(audios), 'audio', audios_duration, audios_size, True, 0, 0, streams_preview, threads_preview, True)
                        threads_muxer.append(threading.Thread(target=muxer, args=(
                            i, dir_work_sub / f'{i.stem}_preview.mkv', file_preview,
                            streams_preview, threads_preview
                        )))
                        log_main.debug(f'Spawned thread {threads_muxer[-1]}')
                        threads_muxer[-1].start()
                    thread_cleaner = threading.Thread(target=cleaner, args=(dir_work_sub, i, threads_screenshot + threads_muxer))
                    log_main.debug(f'Spawned thread {thread_cleaner}')
                    thread_cleaner.start()
                    Pool.Work.add(i)
            time.sleep(5)
    except KeyboardInterrupt:
        log_main.warning('Keyboard Interrupt received, exiting safely...')
        for thread in threading.enumerate(): 
            log_main.debug(f'Alive thread before exiting: {thread.name}')
        Database.write()
        Pool.wake()
        Checker.end()
        hint = False
        wait = 0
        while threading.active_count() > 1:
            if not hint:
                log_main.info(f'Waiting for other threads to end...')
                for thread in threading.enumerate():
                    if thread != threading.current_thread():
                        log_main.debug(f'Waiting for thread to end: {thread}')
            if wait <= 50:
                time.sleep(0.1)
                wait += 1
            else:
                time.sleep(5)
            hint = True
        log_main.warning('Exiting...')
        log_main.debug(f'Ending thread {threading.current_thread()}')