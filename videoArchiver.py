if __name__ != '__main__':
    raise RuntimeError('REFUSED to be imported, you should only run this as sciprt! (i.e. python videoArchiver.py, or python -m videoArchiver) Most functions are heavily dependent on global variables only set in the main body to reduce memory usage. If you really want to test this as a module, you could capture this RuntimeError, but videoArchiver.py is not guaranteed to not fuck up your computer')
    

import datetime
import threading
import subprocess
import json
import re
import pathlib
import shutil
import psutil
import time
import pickle
import math
import logging
import contextlib


class EndExecution(RuntimeError):
    pass

class NotVideo(TypeError):
    pass


class CleanPrinter:
    """Fancier printer, the main purpose of this class is to cleanly print message
    """

    bars = 0
    lock_bar = threading.Lock()
    width = shutil.get_terminal_size()[0]

    @classmethod
    def check_terminal(cls):
        width = shutil.get_terminal_size()[0]
        if width == cls.width:
            return 
        cls.width = width

    @classmethod
    def clear_line(cls):
        """Always clean the line before printing, this is to avoid the unfinished progress bar being stuck on the screen
        """
        print(' ' * cls.width, end='\r')

    @classmethod
    def print(cls, content, loglevel=logging.INFO, end=None):
        cls.check_terminal()
        if cls.bars: # Only clear line if there are progress bars being displayed
            cls.clear_line()
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


def clamp(n, minimum, maximum):
    return min(max(n, minimum), maximum)

class Duration:

    reg_dhms = re.compile(r'(\d+):(\d+):(\d+):(\d+\.?\d*)')
    reg_hms = re.compile(r'(\d+):(\d+):(\d+\.?\d*)')
    reg_ms = re.compile(r'(\d+):(\d+\.?\d*)')
    reg_s = re.compile(r'(\d+\.?\d*)')

    @staticmethod
    def __str_to_time(time: str):
        if time.count(':') > 3:
            raise ValueError(f'{time} has too many ":"')
        m = Duration.reg_dhms.search(time)
        if m:
            return int(m[1]) * 86400 + int(m[2]) * 3600 + int(m[3]) * 60 + float(m[3])
        m = Duration.reg_hms.search(time)
        if m:
            return int(m[1]) * 3600 + int(m[2]) * 60 + float(m[3])
        m = Duration.reg_ms.search(time)
        if m:
            return int(m[1]) * 60 + float(m[2])
        m = Duration.reg_s.search(time)
        if m:
            return float(m[1])
        raise ValueError(f'{time} is not a valid str to convert to time')

    def __init__(self, time: str | int | float = 0):
        if isinstance(time, str):
            self.time = Duration.__str_to_time(time)
        elif isinstance(time, int | float):
            self.time = time
        else:
            raise ValueError(f'Can not initialize a Duration object with {type(time)}')

    def seconds(self):
        return self.time

    def hms(self):
        hours, remainder = divmod(self.time, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f'{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}'

    def __repr__(self):
        return f'{self.__class__.__module__}.{self.__class__.__qualname__}({self.time})'

    def __bool__(self):
        return self.time != 0

    def __str__(self):
        hours, remainder = divmod(self.time, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f'{int(hours):02d}:{int(minutes):02d}:{seconds:0>12.9f}'

    def __int__(self):
        return int(self.time)

    def __float__(self):
        return float(self.time)

    def __abs__(self):
        return Duration(abs(self.time))

    def __round__(self, ndigits = None):
        return Duration(round(self.time, ndigits))

    @staticmethod
    def __complain_comparasion(action, other):
        raise ValueError(f'Can not {action} Duration with {type(other)}')

    def __add__(self, other:  int | float | str):
        if isinstance(other, Duration):
            return Duration(self.time + other.time)
        elif isinstance(other, int | float):
            return Duration(self.time + other)
        elif isinstance(other, str):
            return Duration(self.time + Duration(other).time)
        Duration.__complain_comparasion('add', other)

    def __sub__(self, other):
        if isinstance(other, Duration):
            return Duration(self.time - other.time)
        elif isinstance(other, int | float):
            return Duration(self.time - other)
        elif isinstance(other, str):
            return Duration(self.time - Duration(other).time)
        Duration.__complain_comparasion('substract', other)

    def __mul__(self, other: int | float):
        if isinstance(other, int | float):
            return Duration(self.time * other)
        Duration.__complain_comparasion('multiply', other)

    def __truediv__(self, other: int | float):
        if isinstance(other, int | float):
            return Duration(self.time / other)
        Duration.__complain_comparasion('divide', other)

    def __floordiv__(self, other: int | float):
        if isinstance(other, int | float):
            return Duration(self.time // other)
        Duration.__complain_comparasion('divide', other)

    def __divmod__(self, other: int | float):
        if isinstance(other, int | float):
            time, remainder = divmod(self.time, other)
            return Duration(time), Duration(remainder)
        Duration.__complain_comparasion('divide', other)

    def __lt__(self, other):
        if isinstance(other, Duration):
            if self.time < other.time:
                return True
            else:
                return False
        elif isinstance(other, int | float):
            if self.time < other:
                return True
            else:
                return False
        elif isinstance(other, str):
            if self.time < Duration(other).time:
                return True
            else:
                return False
        Duration.__complain_comparasion('compare', other)

    def __le__(self, other):
        if isinstance(other, Duration):
            if self.time <= other.time:
                return True
            else:
                return False
        elif isinstance(other, int | float):
            if self.time <= other:
                return True
            else:
                return False
        elif isinstance(other, str):
            if self.time <= Duration(other).time:
                return True
            else:
                return False
        Duration.__complain_comparasion('compare', other)

    def __eq__(self, other):
        if isinstance(other, Duration):
            if self.time == other.time:
                return True
            else:
                return False
        elif isinstance(other, int | float):
            if self.time == other:
                return True
            else:
                return False
        elif isinstance(other, str):
            if self.time == Duration(other).time:
                return True
            else:
                return False
        Duration.__complain_comparasion('compare', other)

    def __ne__(self, other):
        if isinstance(other, Duration):
            if self.time != other.time:
                return True
            else:
                return False
        elif isinstance(other, int | float):
            if self.time != other:
                return True
            else:
                return False
        elif isinstance(other, str):
            if self.time != Duration(other).time:
                return True
            else:
                return False
        Duration.__complain_comparasion('compare', other)

    def __gt__(self, other):
        if isinstance(other, Duration):
            if self.time > other.time:
                return True
            else:
                return False
        elif isinstance(other, int | float):
            if self.time > other:
                return True
            else:
                return False
        elif isinstance(other, str):
            if self.time > Duration(other).time:
                return True
            else:
                return False
        Duration.__complain_comparasion('compare', other)

    def __ge__(self, other):
        if isinstance(other, Duration):
            if self.time >= other.time:
                return True
            else:
                return False
        elif isinstance(other, int | float):
            if self.time >= other:
                return True
            else:
                return False
        elif isinstance(other, str):
            if self.time >= Duration(other).time:
                return True
            else:
                return False
        Duration.__complain_comparasion('compare', other)


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
        self.time_spent = Duration(0)
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


class Ffprobe:
    path = pathlib.Path(shutil.which('ffprobe'))
    def __init__(self, args):
        self.p = subprocess.Popen((self.path, *args), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        self.poll_stdout()

    def poll_dumb(self):
        while self.p.poll() is None:
            Checker.is_end(self.p)
            Checker.sleep(1)
        self.returncode = self.p.wait()
        return self.returncode

    def poll_stdout(self):
        chars = []
        reader = self.p.stdout
        while True:
            Checker.is_end(self.p)
            char = reader.read(100)
            if char == b'':
                break
            chars.append(char)
        self.stdout = b''.join(chars)
        self.returncode = self.p.wait()


class Ffmpeg(Ffprobe):  # Note: as for GTX1070 (Pascal), nvenc accepts at most 3 h264 encoding work

    path = pathlib.Path(shutil.which('ffmpeg'))
    log = LoggingWrapper('[Subprocess]')
    reg_complete = {
        'video': re.compile(r'video:(\d+)kB'), 
        'audio': re.compile(r'audio:(\d+)kB')
    }
    reg_running = re.compile(r'size= *(\d+)kB')
    reg_time = re.compile(r' time=([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{2}) ')
    args_video_archive = '-c:v', 'libx264', '-crf', '23', '-preset', 'veryslow'
    args_video_preview = '-c:v', 'libsvtav1', '-qp', '63', '-preset', '8'
    args_audio_archive = '-c:a', 'libopus', '-b:a', '128000'
    args_audio_preview = '-c:a', 'libopus', '-b:a', '24000'

    @staticmethod
    def _log_cutter(log:list):
        log = b''.join(log).split(b'\r\n')
        for paragraph_id, paragraph in enumerate(reversed(log)):
            lines = paragraph.split(b'\r')
            if len(lines) > 1:
                break
        return log[-paragraph_id], lines

    def __init__(self, args, null=False):
        self.null = null
        if null:
            self.p = subprocess.Popen((self.path, *args), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            self.p = subprocess.Popen((self.path, *args), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    # Multiple pollers are used, this is to save resource used to check the if statement hell
    def poll_dumb(self):  
        """Polling the ffmpeg endlessly until it is done, do nothing other than trying to capture the work_end flag

        used in stream_copy (scope: child/encoder)
        used in encoder (scope: child/endoer)
        used in muxer (scope: child/muxer)
        used in screenshooter (scopte: child/screenshooter)

        scope: child
            The EndExecution exception could be raised by check_end_kill, we pass it as is
        """
        if self.null:
            super().poll_dumb()
        else:
            while self.p.stderr.read(100) != b'':
                Checker.is_end(self.p)
            self.returncode = self.p.wait()
        return self.returncode

    def _refuse_null(self):
        if self.null:
            self.p.kill()
            raise ValueError('Polling from void')

    def poll_time(self):  
        self._refuse_null()
        chars = []
        Ffmpeg.log.debug(f'Started {self.p}')
        while True:
            Checker.is_end(self.p)
            char = self.p.stderr.read(100)
            if char == b'':
                break
            chars.append(char)
        Ffmpeg.log.debug(f'Ended {self.p}')
        self.returncode = self.p.wait()
        if self.returncode == 0:
            last, lines = Ffmpeg._log_cutter(chars)
            for line in reversed(lines):
                t = Ffmpeg.reg_time.search(line.decode('utf-8'))
                if t:
                    return self.returncode, Duration(t[1])
        return self.returncode, None

    def poll_time_size(self, stream_type:str='video'):
        self._refuse_null()
        chars = []
        Ffmpeg.log.debug(f'Started {self.p}')
        while True:
            Checker.is_end(self.p)
            char = self.p.stderr.read(100)
            if char == b'':
                break
            chars.append(char)
        Ffmpeg.log.debug(f'Ended {self.p}')
        self.returncode = self.p.wait()
        if self.returncode == 0:
            last, lines = Ffmpeg._log_cutter(chars)
            s = Ffmpeg.reg_complete[stream_type].search(last.decode('utf-8'))
            if s:
                for line in reversed(lines): # This line has frame=xxx, fps=xxx, size=xxx
                    t = Ffmpeg.reg_time.search(line.decode('utf-8'))
                    if t:
                        return self.returncode, Duration(t[1]), int(s[1]) * 1024
        return self.returncode, None, None

    def poll_size(self, stream_type:str='video',size_allow:int=0, file_out:pathlib.Path=None):
        self._refuse_null()
        Ffmpeg.log.debug(f'Started {self.p}')
        while True:
            Checker.is_end(self.p)
            if file_out.stat().size() >= size_allow:
                self.p.kill()
                return None, True
            char = self.p.stderr.read(100)
            if char == b'':
                break
        Ffmpeg.log.debug(f'Ended {self.p}')
        self.returncode = self.p.wait()
        return self.returncode, file_out.stat().size() >= size_allow

    def poll_time_size_limit(self, stream_type:str, size_allow:int=0, file_out:pathlib.Path=None):
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
        self._refuse_null()
        Ffmpeg.log.debug(f'Started {self.p}')
        if size_allow is not None:
            if file_out is None:
                self.p.kill()
                raise ValueError('No path given but trying to limit the file size')
        chars = []
        while True:
            Checker.is_end(self.p)
            if size_allow and file_out.stat().st_size >= size_allow:
                self.p.kill()
                return True
            char = self.p.stderr.read(100)
            if char == b'':
                break
            chars.append(char)
        last, lines = Ffmpeg._log_cutter(chars)
        if file_out is None:
            s = Ffmpeg.reg_complete[stream_type].search(last)
        else:
            s = file_out.stat().st_size
        t = size_allow is None
        for line in reversed(lines): # This line has frame=xxx, fps=xxx, size=xxx
            if s is None:
                s = Ffmpeg.reg_running.search(line)
            if t is None:
                t = Ffmpeg.reg_time.search(line)
            if s is not None and t is not None:
                break
        size = file_out.stat().st_size
        if size_allow:
            if size >= size_allow:
                return True
            else:
                return False
        Ffmpeg.log.debug(f'Ended {self.p}')
        return self.p.wait(), Duration(t), size


class Video:
    def __init__(self, path:pathlib.Path):
        self.raw = path
        self.name = path.stem
        p = Ffprobe(('-show_format', '-show_streams', '-select_streams', 'V', '-of', 'json', path))
        if p.returncode == 0:
            j = json.loads(p.stdout)
            j_format =  j['format']['format_name']
            if not (j_format.endswith('_pipe') or j_format in ('image2', 'tty')) and j['streams']:
                self.streams = []
                self.audios = []
                self.videos = []
                for stream_id, stream in enumerate(
                    json.loads(Ffprobe(('-show_streams', '-of', 'json', path)).stdout)['streams']
                ):
                    #log_scanner.info(f'Getting stream information for stream {stream_id} from {i}')
                    stream_type = stream['codec_type'] 
                    if stream_type in ('video', 'audio'):
                        stream_duration, stream_size = Stream.get_duration_and_size(path, stream_id, stream_type)
                        stream = Stream(self, stream_id, stream_type, stream_duration, stream_size, stream)
                        self.streams.append(stream)
                        if stream_type == 'video':
                            self.videos.append(stream)
                        else:
                            self.audios.append(stream)
                    else:
                        self.streams.append(None)
                if self.videos:
                    self._len = len(self.streams)
                    return
        raise NotVideo

    def __iter__(self):
        return iter(self.streams)

    def __str__(self):
        return str(self.raw)

    def __repr__(self):
        return f'{self.__class__.__module__}.{self.__class__.__qualname__}({self.raw})'

    def __len__(self):
        return self._len

    def __getitem__(self, key):
        if not isinstance(key, int) and (key > self.len or key < -self.len - 1):
            raise KeyError(f'{key} is not a valid stream id')
        return self.streams[key]

    def get_audio_count(self):
        return len(self.audios)

    def start(self, dir_work_sub:pathlib.Path):
        self.work = dir_work_sub  # The work dir is also used as a key
        self.archive = dir_archive / f'{self.name}.mkv'  # It's determined here so the output path can be updated before encoding
        self.preview = dir_preview / f'{self.name}.mkv'
        self.key = Pool.add_threads()
        streams_archive, streams_preview = ([] for i in range(2))
        amix = len(self.audios) > 1
        work_archive = not self.archive.exists()
        work_preview = not self.preview.exists()
        for stream in self.streams:
            if stream is None:
                if work_archive:
                    streams_archive.append(None)
                if work_preview:
                    streams_preview.append(None)
            else:
                if work_archive:
                    streams_archive.append(stream.prepare('archive'))
                if stream.type == 'video' or not amix and work_preview:
                    streams_preview.append(stream.prepare('preview'))
        if amix and work_preview:
            streams_preview.append(self.audios[0].prepare('preview', True))
        if work_archive:
            thread = threading.Thread(target=self.mux, args=('archive', streams_archive))
            Pool.add_thread('muxer', self.key, thread)
            log_main.debug(f'Spawned thread {thread}')
        if work_preview:
            thread = threading.Thread(target=self.mux, args=('preview', streams_preview))
            Pool.add_thread('muxer', self.key, thread)
            log_main.debug(f'Spawned thread {thread}')
        if len(self.videos) == 1:
            file_screenshot = dir_screenshot / f'{self.name}.jpg'
            if not file_screenshot.exists():
                thread = threading.Thread(target=self.videos[0].screenshot)
                Pool.add_thread('screenshot', self.key, thread)
                log_main.debug(f'Spawned thread {thread}')
        else:
            dir_screenshot_sub = dir_screenshot / self.name
            if not dir_screenshot_sub.exists():
                dir_screenshot_sub.mkdir()
            for stream in self.videos:
                file_screenshot = dir_screenshot_sub / f'{self.name}_{stream.id}.jpg'
                if not file_screenshot.exists():
                    thread = threading.Thread(target=stream.screenshot, args=(dir_screenshot_sub, ))
                    Pool.add_thread('screenshot', self.key, thread)
                    log_main.debug(f'Spawned thread {thread}')
        thread = threading.Thread(target=self.clean)
        thread.start()
        log_main.debug(f'Spawned thread {thread}')
        Pool.add_work(self.raw)

    def mux(self, encode_type:str, streams: list):
        log = LoggingWrapper(f'[{self.name}] M:{encode_type.capitalize()[:1]}')
        try:
            for thread in Pool.get_threads(encode_type, self.key):
                Checker.join(thread)
            inputs = ['-i', self.raw]
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
            log.info(f'Muxing to {self.archive}...')
            file_work = self.work / f'{self.name}_{encode_type}.mkv'
            args = (*inputs, '-c', 'copy', *mappers, '-map_metadata', '0', '-y', file_work)
            while Ffmpeg(args, null=True).poll_dumb():
                Checker.is_end()
                log.warning(f'Muxing failed, retry that later')
                Checker.sleep(5)
        except EndExecution:
            log.debug(f'Ending thread {threading.current_thread()}')
            return
        if encode_type == 'archive':
            file_out = self.archive
        else:
            file_out = self.preview
        shutil.move(file_work, file_out)
        log.info(f'Muxed to {file_out}')


    def clean(self):
        """Cleaning the work directory and the raw file

        cleaner itself (scope: child/cleaner)

        scope: child-main
            As the invoker of other child functions, EndExecution exception raised by child functions will be captured here:
                join
                check_end
                db_write

            Should that, or the end_work flag be captured, return to end this thread
        """
        log = LoggingWrapper(f'[{self.name}] CLR')
        try:
            for thread in Pool.get_threads('muxer', self.key):
                Checker.join(thread)
            for thread in Pool.get_threads('screenshot', self.key):
                Checker.join(thread)
            log.info(f'Cleaning input and {self.work}...')
            shutil.rmtree(self.work)
            self.raw.unlink()
            Checker.is_end()
            db.remove(self.raw)
            Pool.remove_work(self.raw)
        except EndExecution:
            log.debug(f'Ending thread {threading.current_thread()}')
            return
        log.info('Done')


class Stream:
    lossless = {
        'video': ('012v', '8bps', 'aasc', 'alias_pix', 'apng', 'avrp', 'avui', 'ayuv', 'bitpacked', 'bmp', 'bmv_video', 'brender_pix', 'cdtoons', 'cllc', 'cscd', 'dpx', 'dxa', 'dxtory', 'ffv1', 'ffvhuff', 'fits', 'flashsv', 'flic', 'fmvc', 'fraps', 'frwu', 'gif', 'huffyuv', 'hymt', 'lagarith', 'ljpeg', 'loco', 'm101', 'magicyuv', 'mscc', 'msp2', 'msrle', 'mszh', 'mwsc', 'pam', 'pbm', 'pcx', 'pfm', 'pgm', 'pgmyuv', 'pgx', 'png', 'ppm', 'psd', 'qdraw', 'qtrle', 'r10k', 'r210', 'rawvideo', 'rscc', 'screenpresso', 'sgi', 'sgirle', 'sheervideo', 'srgc', 'sunrast', 'svg', 'targa', 'targa_y216', 'tiff', 'tscc', 'utvideo', 'v210', 'v210x', 'v308', 'v408', 'v410', 'vble', 'vmnc', 'wcmv', 'wrapped_avframe', 'xbm', 'xpm', 'xwd', 'y41p', 'ylc', 'yuv4', 'zerocodec', 'zlib', 'zmbv'),
        'audio': ('alac', 'ape', 'atrac3al', 'atrac3pal', 'dst', 'flac', 'mlp', 'mp4als', 'pcm_bluray', 'pcm_dvd', 'pcm_f16le', 'pcm_f24le', 'pcm_f32be', 'pcm_f32le', 'pcm_f64be', 'pcm_f64le', 'pcm_lxf', 'pcm_s16be', 'pcm_s16be_planar', 'pcm_s16le', 'pcm_s16le_planar', 'pcm_s24be', 'pcm_s24daud', 'pcm_s24le', 'pcm_s24le_planar', 'pcm_s32be', 'pcm_s32le', 'pcm_s32le_planar', 'pcm_s64be', 'pcm_s64le', 'pcm_s8', 'pcm_s8_planar', 'pcm_sga', 'pcm_u16be', 'pcm_u16le', 'pcm_u24be', 'pcm_u24le', 'pcm_u32be', 'pcm_u32le', 'pcm_u8', 'ralf', 's302m', 'shorten', 'tak', 'truehd', 'tta', 'wmalossless')
    }

    @staticmethod
    def get_duration(path:pathlib.Path, stream_id:int=0):
        for _ in range(3):
            r, t = Ffmpeg(('-i', path, '-c', 'copy', '-map', f'0:{stream_id}', '-f', 'null', '-'), ).poll_time()
            if r == 0:
                return t
            Checker.sleep(5)
        return Duration(0)

    @staticmethod
    def get_duration_and_size(path: pathlib.Path, stream_id: int=0, stream_type: int=0):
        """Get duration and size from a stream from a media file, just a wrapper, 

        used in stream_info (scope: main)
        used in delta_adder (scope: child/encoder)
        used in encoder (scope: child/encoder)

        scope: main
            End when KeyboardException is captured
        scope: child
            The EndExecution exception could be raised by ffmpeg_time_size_poller, we pass it as is
        """
        for _ in range(3):
            r, t, s = Ffmpeg(('-i', path, '-c', 'copy', '-map', f'0:{stream_id}', '-f', 'null', '-'), ).poll_time_size(stream_type)
            if r == 0:
                return t, s
            Checker.sleep(5)
        return Duration(0), 0

    def __init__(self, parent:Video, stream_id:int, stream_type:str, stream_duration:Duration, stream_size:int, stream_info:dict):
        self.parent = parent
        self.id = stream_id
        self.type = stream_type
        self.duration = stream_duration
        self.size = stream_size
        self.lossless = Stream.lossless[self.type]
        if self.type == 'video':
            self.width = stream_info['width']
            self.height = stream_info['height']
            if 'side_data_list' in stream_info and 'rotation' in stream_info['side_data_list'][0]:
                self.rotation = stream_info['side_data_list'][0]['rotation']
            else:
                self.rotation = None

    def __copy(self, log:LoggingWrapper, prefix:str, file_out:pathlib.Path, file_done:pathlib.Path):
        log.info('Transcode inefficient, copying raw stream instead')
        file_copy = self.parent.work / f'{prefix}_copy.nut'
        args = ('-i', self.parent.path, '-c', 'copy', '-map', f'0:{self.id}', '-y', file_copy)
        while Ffmpeg(args, null=True).poll_dumb():
            Checker.is_end()
            log.warning('Stream copy failed, trying that later')
            Checker.sleep(5)
        shutil.move(file_copy, file_out)
        log.info('Stream copy done')
        file_done.touch()

    def __concat(self, log:LoggingWrapper, prefix:str, concat_list:list, file_out:pathlib.Path, file_done:pathlib.Path):
        log.info('Transcode done, concating all parts')
        file_list = self.parent.work / f'{prefix}.list'
        file_concat = self.parent.work / f'{prefix}_concat.nut'
        with Checker.context(open(file_list, 'w')) as f:
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

    def prepare(self, encode_type:str, amix:bool=False):
        if amix:
            file_out = self.parent.work / f'{self.parent.name}_preview_amix.nut'
            file_done = self.parent.work / f'{self.parent.name}_preview_amix.done'
        else:
            file_out = self.parent.work / f'{self.parent.name}_archive_{self.id}_{self.type}.nut'
            file_done = self.parent.work / f'{self.parent.name}_archive_{self.id}_{self.type}.done'
        if file_done.exists():
            if file_out.exists():
                return file_out
            else:
                file_done.unlink()
        thread = threading.Thread(target=self.encode, args=(encode_type, file_out, file_done, amix))
        Pool.add_thread(encode_type, self.parent.key, thread)
        log_main.debug(f'Spawned thread {thread}')
        return file_out

    def encode(self, encode_type:str, file_out:pathlib.Path, file_done:pathlib.Path, amix:bool=False):
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
        if amix:
            log = LoggingWrapper(f'[{self.parent.name}] E:P S:Amix')
            prefix = f'{self.parent.name}_preview_audio'
        else:
            log = LoggingWrapper(f'[{self.parent.name}] E:{encode_type[:1].upper()} S:{self.id}:{self.type[:1].upper()}')
            prefix = f'{self.parent.name}_{encode_type}_{self.id}_{self.type}'
        log.info('Work started')
        check_efficiency = encode_type == 'archive'  and not self.lossless 
        concat_list = []
        start = Duration()
        size_exist = 0
        file_concat_pickle = self.parent.work / f'{prefix}_concat.pkl'
        try:
            # Recovery
            if file_out.exists():
                size_exist = file_out.stat().st_size
                if size_exist:
                    log.warning('Output already exists, potentially broken before, trying to recover it')
                    time_delta = Stream.get_duration(file_out)
                    if time_delta:
                        log.debug(f'Recovery needed: {file_out}')
                        if abs(self.duration - time_delta) < 1:
                            log.info('Last transcode successful, no need to transcode')
                            file_done.touch()
                            return
                        log.warning(f'Finding all failed parts of {file_out}')
                        suffix = 0
                        file_check = self.parent.work / f'{prefix}_{suffix}.nut'
                        while file_check.exists():
                            Checker.is_end()
                            log.warning(f'Found a failed part: {file_check}')
                            size_delta = file_check.stat().st_size
                            if size_delta:
                                time_delta = Stream.get_duration(file_check)
                                if time_delta:
                                    start += time_delta
                                    size_exist += size_delta
                                    log.warning(f'Recovered: {file_check}, Duration: {time_delta}, size: {size_delta}. Total: Duration:{start}, size: {size_exist}')
                                    concat_list.append(file_check.name)
                                else:
                                    log.warning(f'Unable to recovery: {file_check}, skipped')
                            suffix += 1
                            file_check = self.parent.work / f'{prefix}_{suffix}.nut'
                        log.info(f'Recovered last part will be saved to {file_check}')
                        file_recovery = self.parent.work / f'{prefix}_recovery.nut'
                        for _ in range(3):
                            Checker.is_end()
                            if self.type == 'video':
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
                                        log.info(f'Recovering {frame_last} usable video frames')
                                        p = Ffmpeg(('-i', file_out, '-c', 'copy', '-map', '0', '-y', '-vframes', str(frame_last), file_recovery))
                                    else:
                                        log.info('No frames usable')
                                        break
                            else:
                                log.info('Recovering usable audio frames')
                                p = Ffmpeg(('-i', file_out, '-c', 'copy', '-map', '0', '-y', file_recovery))
                            if isinstance(p, Ffmpeg):
                                r, t = p.poll_time()
                                if r:
                                    log.warning('Recovery failed, retrying that later')
                                else:
                                    start += t
                                    size_exist += file_recovery.stat().st_size
                                    shutil.move(file_recovery, file_check)
                                    concat_list.append(file_check.name)
                                    log.info(f'{t} of failed transcode recovered. Total: duration:{start}, size{size_exist}')
                                    break
                        with Checker.context(open(file_concat_pickle, 'wb')) as f:
                            pickle.dump((concat_list, start, size_exist), f)
                file_out.unlink()      
            # Check if recovered last time
            if not concat_list and file_concat_pickle.exists():
                with file_concat_pickle.open('rb') as f:
                    concat_list, start, size_exist = pickle.load(f)
            # If recovered, check if there is the need to continue recovery
            if concat_list:
                # We've already transcoded this
                if check_efficiency and size_exist > self.size * 0.9:
                    self.__copy(log, prefix, file_out, file_done)
                    return 
                if start >= self.duration or self.duration - start < 1:
                    self.__concat(log, prefix, concat_list, file_out, file_done)
                    return
            # Real encoding happenes below
            if check_efficiency:
                size_allow = self.size * 0.9 - size_exist
            arg_map = f'0:{self.id}'
            args_filter = ()
            if self.type == 'video':
                if encode_type == 'archive':
                    args_codec = Ffmpeg.args_video_archive
                else:
                    args_codec = Ffmpeg.args_video_preview
                    if self.width > 1000 and self.height > 1000:
                        stream_width = self.width
                        stream_height = self.height
                        while stream_width > 1000 and stream_height > 1000:
                            stream_width //= 2
                            stream_height //= 2
                        args_filter = '-filter:v', f'scale={stream_width}x{stream_height}'
            else: # Audio
                if encode_type == 'archive':
                    args_codec = Ffmpeg.args_audio_archive
                else:
                    arg_map = '0:a'
                    args_codec = Ffmpeg.args_audio_preview
                    args_filter = '-filter_complex', f'amix=inputs={self.parent.get_audio_count()}:duration=longest'
            args = '-ss', str(start), '-i', self.parent.raw, *args_codec, *args_filter, '-map', arg_map, '-y', file_out
            while True:
                log.info('Transcode started')
                Checker.is_end()
                if self.type == 'video':
                    Pool.wait(log, encode_type)
                p = Ffmpeg(args)
                if check_efficiency:
                    r, inefficient = p.poll_size(self.type, size_allow, file_out)
                    if inefficient:
                        self.__copy(log, prefix, file_out, file_done)
                        return
                else:
                    p.poll_dumb()
                if p.returncode == 0:
                    if concat_list:
                        concat_list.append(file_out.name)
                        self.__concat(log, prefix, concat_list, file_out, file_done)
                    else:
                        log.info('Transcode done')
                        file_done.touch()
                    break
                log.warning(f'Transcode failed, returncode: {p.returncode}, retrying that later')
                Checker.sleep(5)
        except EndExecution:
            log.debug(f'Ending thread {threading.current_thread()}')
            return

    def screenshot(self, dir_screenshot_sub:pathlib.Path=None):
        """Taking screenshot for video stream, according to its duration, making mosaic screenshot

        screenshooter itself (scope: child/screenshooter)

        scope: child-main
            As the invoker of other child functions, EndExecution exception raised by child functions will be captured here:
                cpu_wait
                ffmpeg_dumb_poller
                check_end

            Should that, return to end this thread
        """
        if dir_screenshot_sub is None:
            name = f'{self.parent.name}.jpg'
            file_out = dir_screenshot / name
            file_work = self.parent.work / name
        else:
            name = f'{self.parent.name}_{self.id}.jpg'
            file_out = dir_screenshot_sub / name
            file_work = self.parent.work / name
        args_out = ('-vsync', 'passthrough', '-frames:v', '1', '-y', file_work)
        length = clamp(int(math.log(self.duration.seconds()/2 + 1)), 1, 10)
        if length == 1:
            log = LoggingWrapper(f'[{self.parent.name}] S')
            args = ('-ss', str(self.duration/2), '-i', self.parent.raw, '-map', f'0:{self.id}', *args_out)
            prompt = 'Taking screenshot, single frame'
        else:
            log = LoggingWrapper(f'[{self.parent.name}] S:{self.id}')
            if self.rotation in (90, -90):
                stream_width = self.width
                stream_height = self.height
            else:
                stream_width = self.width
                stream_height = self.height
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
            time_delta = self.duration / tiles
            if self.duration < tiles:
                args = (
                    '-i', self.parent.raw, '-map', f'0:{self.id}', '-filter:v', f'select=eq(n\,0)+gte(t-prev_selected_t\,{time_delta.seconds()}),tile={length}x{length}{arg_scale}', *args_out
                )
            else:
                time_start = Duration(0)
                args_input = []
                args_position = []
                args_mapper = []
                file_id = 0
                for i in range(length):
                    for j in range(length):
                        Checker.is_end()
                        args_input.extend(('-ss', str(time_start), '-i', self.parent.raw))
                        args_mapper.append(f'[{file_id}:{self.id}]')
                        args_position.append(f'{j*stream_width}_{i*stream_height}')
                        time_start += time_delta
                        file_id += 1
                arg_mapper = ''.join(args_mapper)
                arg_position = '|'.join(args_position)
                args = (
                    *args_input, '-filter_complex', f'{arg_mapper}xstack=inputs={tiles}:layout={arg_position}{arg_scale}', *args_out
                )
            prompt = f'Taking screenshot, {length}x{length} grid, for each {time_delta} segment, {width}x{height} res'
        try:
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
        shutil.move(file_work, file_out)


class Checker:
    end_flag = False
    @classmethod
    def is_end(cls, p:subprocess.Popen=None):
        if cls.end_flag:
            if p is not None:
                p.kill()
            raise EndExecution

    @classmethod
    def join(cls, thread: threading.Thread):
        """Cleanly join a thread, just an invoker so that the work_end flag can be captured

        used in muxer (scope: child/muxer)
        used in cleaner (scope: child/cleaner)

        scope: child
            The EndExecution flag could be raised by check_end, we pass it as is
        """
        if cls.end_flag:
            raise EndExecution
        thread.join()
        if cls.end_flag:
            raise EndExecution

    @classmethod
    def end(cls):
        cls.end_flag = True

    @classmethod
    def sleep(cls, t:int=5):
        while t:
            cls.is_end()
            time.sleep(1)
            t -= 1

    @contextlib.contextmanager
    def context(manager, p:subprocess.Popen=None):
        if Checker.end_flag:
            raise EndExecution
        with manager as f:
            yield f
        if Checker.end_flag:
            raise EndExecution


class Database:

    def __init__(self, backend):
        self.lock = threading.RLock()
        self.log = LoggingWrapper('[Database]')
        self.backend = backend
        self.db = {}
        self.read()

    def __iter__(self):
        with Checker.context(self.lock):
            return iter(self.db)

    def dict(self):
        """Return the database as dict, currently this just means return the db
        """
        with Checker.context(self.lock):
            return self.db

    def items(self):
        with Checker.context(self.lock):
            return self.db.items()

    def read(self):
        with Checker.context(self.lock):
            if self.backend.exists():
                Checker.is_end()
                with open(self.backend, 'rb') as f:
                    Checker.is_end()
                    self.db = pickle.load(f)

    def write(self):
        """Write the db if it's updated

        used in cleaner (scope: child/cleaner)
        used in main (scope: main)
        used in scan_dir (scope: main)
        
        scope: child
            The EndExecution exception could be raised by wait_end, we pass it as is

        scope: main
            End when KeyboardException is captured
        """
        with Checker.context(self.lock):
            self.log.info('Updated')
            with Checker.context(open(self.backend, 'wb')) as f:
                pickle.dump(self.db, f)
            self.log.info('Saved')

    def add(self, key, value):
        with Checker.context(self.lock):
            if key not in self.db:
                self.db[key] = value
                self.log.info(f'Added {key}')
                self.write()

    def remove(self, key):
        with Checker.context(self.lock):
            if key in self.db:
                del self.db[key]
                self.log.info(f'Removed {key}')
                self.write()

    
    def query(self, key):
        with Checker.context(self.lock):
            if key in self.db:
                return True
            return False


    def clean(self):
        """Cleaning the db, remove files not existing or already finished

        used in main (scope: main)
        
        scope: main
            End when KeyboardException is captured
        """
        with Checker.context(self.lock):
            db_new = {}
            for i_r, j_r in {i:j for i, j in self.db.items() if i.exists()}.items():
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
                    self.log.warning(f'Purged already finished video {i_r}')
                else:
                    db_new[i_r] = j_r
            if self.db != db_new:
                self.db = db_new
                self.write()


class Pool:
    _pool_264, _pool_av1, _pool_ss, _pool_work, _threads_archive, _threads_preview, _threads_muxer, _threads_screenshot = ([] for i in range(8))
    _threads_id = -1
    _lock_264, _lock_av1, _lock_ss, _lock_work = (threading.Lock() for i in range(4))
    _cpu_percent = 0
    _cpu_264 = 50
    _cpu_av1 = 60
    _cpu_ss = 90
    _prompt_264 = 'Waked up an x264 encoder'
    _prompt_av1 = 'Waked up an av1 encoder'
    _prompt_ss = 'Waked up a screenshooter'
    _log = LoggingWrapper('[Scheduler]')
    _event_scheduler = threading.Event()

    @classmethod
    def wake(cls):
        cls._event_scheduler.set()

    @classmethod
    def _update_cpu_percent(cls):
        cls._cpu_percent = psutil.cpu_percent()
        cls._log.debug(f'CPU usage: {cls._cpu_percent}')

    @classmethod
    def _waker(cls, pool, lock, cpu_need, prompt):
        while cls._cpu_percent < cpu_need and pool:
            Checker.is_end()
            with lock:
                Checker.is_end()
                pool.pop(0).set()
                cls._log.info(prompt)
            Checker.sleep(5)
            cls._update_cpu_percent()

    @classmethod
    def wait(cls, log, style):
        """Wait for CPU resource,

        used in encoder (scope: child/encoder)
        used in screenshooter (scope: child/screenshooter)

        scope: child
            The EndExecution exception could be raised by wait_end, we pass it as is
        """
        match style:
            case '264' | 'archive':
                lock = cls._lock_264
                pool = cls._pool_264
            case 'av1' | 'preview':
                lock = cls._lock_av1
                pool = cls._pool_av1
            case 'ss' | 'screenshot':
                lock = cls._lock_ss
                pool = cls._pool_ss
        log.info('Waiting for CPU resources')
        waiter = threading.Event()
        log.debug(f'Waiting for {waiter} to be set')
        with Checker.context(lock):
            pool.append(waiter)
        cls._event_scheduler.set()
        waiter.wait()
        Checker.is_end()
        log.info('Waked up')

    @classmethod
    def add_work(cls, entry):
        with Checker.context(cls._lock_work):
            cls._pool_work.append(entry)

    @classmethod
    def remove_work(cls, entry):
        with Checker.context(cls._lock_work):
            cls._pool_work.remove(entry)

    @classmethod
    def query_work(cls, entry):
        with Checker.context(cls._lock_work):
            if entry in cls._pool_work:
                return True
        return False

    @classmethod
    def add_threads(cls):
        cls._threads_archive.append([])
        cls._threads_preview.append([])
        cls._threads_muxer.append([])
        cls._threads_screenshot.append([])
        cls._threads_id += 1
        return cls._threads_id

    @classmethod
    def add_thread(cls, pool, threads_id, thread):
        match pool:
            case 'archive':
                cls._threads_archive[threads_id].append(thread)
            case 'preview':
                cls._threads_preview[threads_id].append(thread)
            case 'muxer':
                cls._threads_muxer[threads_id].append(thread)
            case 'screenshot':
                cls._threads_screenshot[threads_id].append(thread)
        thread.start()

    @classmethod
    def get_threads(cls, pool, threads_id):
        match pool:
            case 'archive':
                return cls._threads_archive[threads_id]
            case 'preview':
                return cls._threads_preview[threads_id]
            case 'muxer':
                return cls._threads_muxer[threads_id]
            case 'screenshot':
                return cls._threads_preview[threads_id]

    @classmethod
    def scheduler(cls):
        cls._log.info('Started')
        while not work_end:
            try:
                with Checker.context(cls._lock_264), Checker.context(cls._lock_av1), Checker.context(cls._lock_ss):
                    if not cls._pool_264 and not cls._pool_av1 and not cls._pool_ss:
                        cls._event_scheduler.clear()
                Checker.is_end()
                cls._event_scheduler.wait()
                Checker.is_end()
                Checker.sleep(5)
                cls._update_cpu_percent()
                cls._waker(cls._pool_264, cls._lock_264, cls._cpu_264, cls._prompt_264)
                cls._waker(cls._pool_av1, cls._lock_av1, cls._cpu_av1, cls._prompt_av1)
                cls._waker(cls._pool_ss, cls._lock_ss, cls._cpu_ss, cls._prompt_ss)
            except EndExecution:
                break
        cls._log.warning('Terminating, waking up all sleeping threads so they can end themselvies')
        for waitpool in cls._pool_264, cls._pool_av1, cls._pool_ss:
            while waitpool:
                waiter = waitpool.pop(0)
                waiter.set()
                cls._log.debug(f'Emergency wakeup: {waiter}')
        cls._log.debug(f'Ending thread {threading.current_thread()}')


def wait_close(file_raw:pathlib.Path):
    """Check if a file is being opened, if it is, wait until it's closed

    used in scan_dir (scope: main)

    scope: main
        End when KeyboardException is captured
    """
    size_old = file_raw.stat().st_size
    hint = False
    while True:
        opened = False
        for p in psutil.process_iter():
            try:
                for f in p.open_files():
                    if file_raw.samefile(f.path):
                        if not hint:
                            log_scanner.warning(f'Jammed, {file_raw} is opened by {p.pid} {p.cmdline()}')
                            hint = True
                        opened = True
                        break
            except (psutil.NoSuchProcess, psutil.AccessDenied, PermissionError):
                pass
        size_new = file_raw.stat().st_size
        if size_new != size_old:
            opened = True
            if not hint:
                log_scanner.warning(f'Jammed, {file_raw} is being written')
        if not opened:
            if hint:
                log_scanner.info(f'{file_raw} closed writing, continue scanning')
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
        elif i.is_file() and not db.query(i):
            wait_close(i)
            if not db.query(i):
                log_scanner.info(f'Discovered {i}')
                try:
                    db_entry = Video(i)
                    log_scanner.info(f'Added {i} to db')
                    log_scanner.debug(f'{i} streams: {db_entry.streams}')
                except NotVideo:
                    db_entry = None
                except EndExecution:
                    return
            db.add(i, db_entry)


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
    db = Database(dir_work / 'db.pkl')
    log_main = LoggingWrapper('[Main]')
    log_scanner = LoggingWrapper('[Scanner]')
    log_main.info('Started')
    threading.Thread(target = Pool.scheduler).start()
    try:
        while True:
            db.clean()
            scan_dir(dir_raw)
            for path, video in db.items():
                if video is not None and not Pool.query_work(path):
                    dir_work_sub = dir_work / video.name
                    if dir_work_sub in dirs_work_sub:
                        suffix = 0
                        while dir_work_sub in dirs_work_sub or (dir_work_sub.exists() and not dir_work_sub.is_dir()):
                            dir_work_sub = dir_work / (video.name + str(suffix))
                    if not dir_work_sub.exists():
                        dir_work_sub.mkdir()
                    video.start(dir_work_sub)
                    Pool.add_work(path)
            time.sleep(5)
    except KeyboardInterrupt:
        log_main.warning('Keyboard Interrupt received, exiting safely...')
        for thread in threading.enumerate(): 
            log_main.debug(f'Alive thread before exiting: {thread.name}')
        db.write()
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