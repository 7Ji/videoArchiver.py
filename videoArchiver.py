if __name__ != '__main__':
    raise RuntimeError('REFUSED to be imported, you should only run this as sciprt! (i.e. python videoArchiver.py, or python -m videoArchiver) Most functions are heavily dependent on global variables only set in the main body to reduce memory usage. If you really want to test this as a module, you could capture this RuntimeError, but videoArchiver.py is not guaranteed to not fuck up your computer')
    

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

    @staticmethod
    def check_terminal():
        width = shutil.get_terminal_size()[0]
        if width == CleanPrinter.width:
            return 
        CleanPrinter.width = width

    @staticmethod
    def clear_line():
        """Always clean the line before printing, this is to avoid the unfinished progress bar being stuck on the screen
        """
        print(' ' * CleanPrinter.width, end='\r')

    @staticmethod
    def print(content, loglevel=logging.INFO, end=None):
        CleanPrinter.check_terminal()
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
    args_video_archive = ('-c:v', 'libx264', '-crf', '23', '-preset', 'veryslow')
    args_video_preview = ('-c:v', 'libsvtav1', '-qp', '63', '-preset', '8')
    args_audio_archive = ('-c:a', 'libfdk_aac', '-vbr', '5')
    args_audio_preview = ('-c:a', 'libfdk_aac', '-vbr', '1', '-map', '0:a')

    @staticmethod
    def __log_cutter(log):
        log = b''.join(log).split(b'\n')
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

    def __refuse_null(self):
        if self.null:
            self.p.kill()
            raise ValueError('Polling from void')

    def poll_time(self):  
        self.__refuse_null()
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
            last, lines = Ffmpeg.__log_cutter(chars)
            for line in reversed(lines):
                t = Ffmpeg.reg_time.search(line)
                if t:
                    return self.returncode, Duration(t[1])
        return self.returncode, None

    def poll_time_size(self, stream_type:str='video'):
        self.__refuse_null()
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
            last, lines = Ffmpeg.__log_cutter(chars)
            s = Ffmpeg.reg_complete[stream_type].search(last)
            if s:
                for line in reversed(lines): # This line has frame=xxx, fps=xxx, size=xxx
                    t = Ffmpeg.reg_time.search(line)
                    if t:
                        return self.returncode, Duration(t[1]), int(s[1]) * 1024
        return self.returncode, None, None

    def poll_size(self, stream_type:str='video',size_allow:int=0, file_out:pathlib.Path=None):
        self.__refuse_null()
        chars = []
        Ffmpeg.log.debug(f'Started {self.p}')
        while True:
            Checker.is_end(self.p)
            if file_out.stat().size() >= size_allow:
                self.p.kill()
                return None, True
            char = self.p.stderr.read(100)
            if char == b'':
                break
            chars.append(char)
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
        self.__refuse_null()
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
        last, lines = Ffmpeg.__log_cutter(chars)
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
                video = False
                for stream_id, stream in enumerate(
                    json.loads(Ffprobe(('-show_self.streams', '-of', 'json', i)).stdout)['self.streams']
                ):
                    #log_scanner.info(f'Getting stream information for stream {stream_id} from {i}')
                    stream_type = stream['codec_type'] 
                    if stream_type in ('video', 'audio'):
                        stream_duration, stream_size = Stream.get_duration_and_size(path, stream_id, stream_type)
                        stream = Stream(stream_id, stream_type, stream_duration, stream_size, stream)
                        if stream_type == 'video':
                            self.videos.append(stream)
                        else:
                            self.audios.append(stream)
                        self.streams.append(stream)
                    else:
                        self.streams.append(None)
                    if not video and stream_type == 'video':
                        video = True
                if video:
                    self.__len = len(self.streams)
                    return
        raise NotVideo

    def __iter__(self):
        return iter(self.streams)

    def __str__(self):
        return str(self.raw)

    def __repr__(self):
        return f'{self.__class__.__module__}.{self.__class__.__qualname__}({self.raw})'

    def __len__(self):
        return self.__len

    def __getitem__(self, key):
        if not isinstance(key, int) and (key > self.len or key < -self.len - 1):
            raise KeyError(f'{key} is not a valid stream id')
        return self.streams[key]

    def prepare(self, dir_work_sub:pathlib.Path):
        self.work = dir_work_sub
        self.archive = dir_archive / f'{self.name}.mkv'  # It's determined here so the output path can be updated before encoding
        self.preview = dir_preview / f'{self.name}.mkv'
        self.threads_archive, self.threads_preview, self.threads_muxer, streams_archive, streams_preview, audios =  ([] for i in range(6))
        videos = {}
        amix = len(self.audios) > 1
        audios_size = 0
        audios_duration = Duration()
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
                    threads_archive.append(threading.Thread(target=stream.encode, args=('archive')))
                    threads_archive[-1].start()
                if work_preview and not amix:
                    threads_preview.append(threading.Thread(target=stream.encode, args=('preview')))
                    threads_preview[-1].start()
        if amix:
            threads_preview.append(threading.Thread(target=self.audios[0].encode, args=('preview', True)))
            self.amix()
        if len(self.videos) == 1:
            file_screenshot = dir_screenshot / f'{i.stem}.jpg'
            if not file_screenshot.exists():
                threads_screenshot.append(threading.Thread(target=screenshot, args=(i, dir_work_sub / f'{i.stem}_screenshot.jpg', file_screenshot, stream_id, stream['duration'], stream['width'], stream['height'], stream['rotation'])))
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

    def amix(self, audios):
        pass

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
                    Checker.join(thread)
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
                Checker.join(thread)
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


class Stream:
    # VIDEO = 0
    # AUDIO = 1
    # SUBTITLE = 2
    # DATA = 3
    # ATTACHMENT = 4
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

    def __sum_parts(self):
        self.__log.warning(f'Finding all failed parts of {self.__file_out}')
        suffix = 0
        file_check = self.parent.work / f'{self.__prefix}_{suffix}.nut'
        while file_check.exists():
            Checker.is_end()
            self.__log.warning(f'Found a failed part: {file_check}')
            size_delta = file_check.stat().st_size
            if size_delta:
                time_delta = Stream.get_duration(file_check)
                if time_delta:
                    self.__start += time_delta
                    self.__size_exist += size_delta
                    self.__log.warning(f'Recovered: {file_check}, Duration: {time_delta}, size: {size_delta}. Total: Duration:{self.__start}, size: {self.__size_exist}')
                    self.__concat_list.append(file_check.name)
                else:
                    self.__log.warning(f'Unable to recovery: {file_check}, skipped')
            suffix += 1
            file_check = self.parent.work / f'{self.__prefix}_{suffix}.nut'
        self.__log.info(f'Recovered last part will be saved to {file_check}')
        return file_check

    def __extract_frames(self):
        file_work = self.__sum_parts()
        file_recovery = self.parent.work / f'{self.__prefix}_recovery.nut'
        for _ in range(3):
            Checker.is_end()
            if self.type == 'video':
                self.__log.info('Checking if the interrupt file is usable')
                p = Ffprobe(('-show_frames', '-select_streams', 'v:0', '-of', 'json', self.__file_out))
                if p.returncode == 0:
                    self.__log.info('Analyzing available frames')
                    frames = json.loads(p.stdout)['frames']
                    self.__log.debug(f'{len(frames)} frames found in {self.__file_out}')
                    frame_last = 0
                    for frame_id, frame in enumerate(reversed(frames)):
                        Checker.is_end()
                        if frame['key_frame']:
                            frame_last = len(frames) - frame_id - 1
                            break
                    self.__log.debug(f'Last GOP start at frame: {frame_last}')
                    if frame_last:
                        self.__log.info(f'Recovering {frame_last} usable video frames')
                        p = Ffmpeg(('-i', self.__file_out, '-c', 'copy', '-map', '0', '-y', '-vframes', str(frame_last), file_recovery))
                    else:
                        self.__log.info('No frames usable')
                        break
            else:
                self.__log.info('Recovering usable audio frames')
                p = Ffmpeg(('-i', self.__file_out, '-c', 'copy', '-map', '0', '-y', file_recovery))
            if isinstance(p, Ffmpeg):
                r, t = p.poll_time(self.type)
                if r:
                    self.__log.warning('Recovery failed, retrying that later')
                else:
                    self.__start += t
                    self.__size_exist += file_recovery.stat().st_size
                    shutil.move(file_recovery, file_work)
                    self.__concat_list.append(file_work.name)
                    self.__log.info(f'{t} of failed transcode recovered. Total: duration:{self.__start}, size{self.__size_exist}')
                    break

    def __recovery(self):
        if self.__file_out.exists():
            self.__size_exist = self.__file_out.stat().st_size
            if self.__size_exist:
                self.__log.warning('Output already exists, potentially broken before, trying to recover it')
                time_delta = Stream.get_duration(self.__file_out)
                if time_delta:
                    self.__log.debug(f'Recovery needed: {self.__file_out}')
                    if abs(self.duration - time_delta) < 1:
                        self.__log.info('Last transcode successful, no need to transcode')
                        self.__file_done.touch()
                        return
                    self.__extract_frames()
                    self.__file_out.unlink()
                    with Checker.context(open(self.__file_concat_pickle, 'wb')) as f:
                        pickle.dump((self.__concat_list, self.__start, self.__size_exist), f)
            self.__file_out.unlink()      
        # Check if recovered last time
        if not self.__concat_list and self.__file_concat_pickle.exists():
            with self.__file_concat_pickle.open('rb') as f:
                self.__concat_list, self.__start, self.__size_exist = pickle.load(f)

    def __copy(self):
        self.__log.info('Transcode inefficient, copying raw stream instead')
        file_copy = self.parent.work / f'{self.__prefix}_copy.nut'
        args = ('-i', self.parent.path, '-c', 'copy', '-map', f'0:{self.id}', '-y', file_copy)
        while Ffmpeg(args, null=True).poll_dumb():
            Checker.is_end()
            self.__log.warning('Stream copy failed, trying that later')
            Checker.sleep(5)
        shutil.move(file_copy, self.__file_out)
        self.__log.info('Stream copy done')
        self.__file_done.touch()

    def __concat(self):
        self.__log.info('Transcode done, concating all parts')
        file_list = self.parent.work / f'{self.__prefix}.list'
        file_concat = self.parent.work / f'{self.__prefix}_concat.nut'
        with Checker.context(open(file_list, 'w')) as f:
            for file in self.__concat_list:
                Checker.is_end()
                f.write(f'file {file}\n')
        args = ('-f', 'concat', '-safe', '0', '-i', file_list, '-c', 'copy', '-map', '0', '-y', file_concat)
        while Ffmpeg(args, null=True).poll_dumb():
            Checker.is_end()
            self.__log.warning('Concating failed, trying that later')
            Checker.sleep(5)
        shutil.move(file_concat, self.__file_out)
        self.__log.info('Concating done')
        self.__file_done.touch()

    def __args_constructor(self):
        args_in = ('-ss', str(self.__start), '-i', self.parent.raw)
        if self.type != 'audio' and encode_type != 'preview':
            args_map = ('-map', f'0:{self.id}')
        args_out = ('-y', self.__file_out)
        if self.type == 'video':
            if encode_type == 'archive':
                args = (*args_in,  *Ffmpeg.args_video_archive, *args_map, *args_out)
            else:
                if self.width > 1000 and self.height > 1000:
                    stream_width = self.width
                    stream_height = self.height
                    while stream_width > 1000 and stream_height > 1000:
                        stream_width //= 2
                        stream_height //= 2
                    args=(*args_in, *Ffmpeg.args_video_preview, *args_map, '-filter:v', f'scale={stream_width}x{stream_height}', *args_out)
                else:
                    args=(*args_in, *Ffmpeg.args_video_preview, *args_map, *args_out)
        else: # Audio
            if encode_type == 'archive':
                args=(*args_in, *Ffmpeg.args_audio_archive, *args_map, *args_out)
            else:
                if self.id == 1:
                    args=(*args_in, *Ffmpeg.args_audio_preview, *args_out)
                else:
                    args=(*args_in, *Ffmpeg.args_audio_preview, '-filter_complex', f'amix=inputs={len(self.parent)}:duration=longest', *args_out)
        self.__args = args

    def encode(self, encode_type:str, amix:bool=False):
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
        self.__file_out = self.parent.work / f'{self.parent.name}_{encode_type}_{self.id}_{self.type}.nut'
        self.__file_done = self.parent.work / f'{self.parent.name}_{encode_type}_{self.id}_{self.type}.done'
        if amix:
            self.__log = LoggingWrapper(f'[{self.parent.name}] E:P S:Amix')
            self.__prefix = f'{self.parent.name}_preview_audio'
        else:
            self.__log = LoggingWrapper(f'[{self.parent.name}] E:{encode_type[:1].capitalize()} S:{self.id}:{self.type[:1].capitalize()}')
            self.__prefix = f'{self.parent.name}_{encode_type}_{self.id}_{self.type}'
        self.__log.info('Work started')
        self.__file_concat_pickle = self.parent.work / f'{self.__prefix}_concat.pkl'
        check_efficiency = encode_type == 'archive'  and not self.lossless 
        self.__start = Duration()
        self.__size_exist = 0
        self.__concat_list = []
        try:
            # Recovery
            self.__recovery()
            # If recovered, check if there is the need to continue recovery
            if self.__concat_list:
                # We've already transcoded this
                if check_efficiency and self.__size_exist > self.size * 0.9:
                    self.__copy()
                    return 
                if self.__start >= self.duration or self.duration - self.__start < Duration(1):
                    self.__concat()
                    return
            # Real encoding happenes below
            
            if check_efficiency:
                size_allow = self.size * 0.9 - self.__size_exist
            self.__args_constructor()
            while True:
                self.__log.info('Transcode started')
                Checker.is_end()
                if self.type == 'video':
                    Pool.wait(self.__log, encode_type)
                p = Ffmpeg(self.__args)
                if check_efficiency:
                    r, inefficient = p.poll_size(self.type, size_allow, self.__file_out)
                    if inefficient:
                        self.__copy()
                        return
                else:
                    p.poll_dumb()
                if p.returncode == 0:
                    if self.__concat_list:
                        self.__concat_list.append(self.__file_out.name)
                        self.__concat()
                    else:
                        self.__log.info('Transcode done')
                        self.__file_done.touch()
                    break
                self.__log.warning(f'Transcode failed, returncode: {p.returncode}, retrying that later')
                Checker.sleep(5)
        except EndExecution:
            self.__log.debug(f'Ending thread {threading.current_thread()}')
            return

    def screenshot(self, dir_screenshot_sub: pathlib.Path=None):
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
            args = ('-ss', str(self.duration/2), '-i', self.parent.path, '-map', f'0:{self.id}', *args_out)
            prompt = 'Taking screenshot, single frame'
        else:
            log = LoggingWrapper(f'[{self.parent.name}] S:{self.id}')
            if self.rotation in (90, -90):
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
            time_delta = self.duration / tiles
            if self.duration < tiles:
                args = (
                    '-i', self.parent.path, '-map', f'0:{self.id}', '-filter:v', f'select=eq(n\,0)+gte(t-prev_selected_t\,{time_delta.seconds()}),tile={length}x{length}{arg_scale}', *args_out
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
                        args_input.extend(('-ss', str(time_start), '-i', self.parent.path))
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
        elif i.is_file() and not db.query(i):
            wait_close(i)
            db_entry = None
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
                Checker.join(thread)
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
            Checker.join(thread)
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
    def join(thread: threading.Thread):
        """Cleanly join a thread, just an invoker so that the work_end flag can be captured

        used in muxer (scope: child/muxer)
        used in cleaner (scope: child/cleaner)

        scope: child
            The EndExecution flag could be raised by check_end, we pass it as is
        """
        if Checker.end_flag:
            raise EndExecution
        thread.join()
        if Checker.end_flag:
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
        self.lock = threading.Lock()
        self.log = LoggingWrapper('[Database]')
        self.backend = backend
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
            else:
                self.db = {}

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
        with Checker.context(lock):
            pool.append(waiter)
        Pool.e.set()
        waiter.wait()
        Checker.is_end()
        log.info('Waked up')
    

    class Work:
        @staticmethod
        def add(entry):
            with Checker.context(Pool.lock_work):
                Pool.pool_work.append(entry)

        @staticmethod
        def remove(entry):
            with Checker.context(Pool.lock_work):
                Pool.pool_work.remove(entry)

        @staticmethod
        def query(entry):
            with Checker.context(Pool.lock_work):
                if entry in Pool.pool_work:
                    return True
            return False


    @staticmethod
    def scheduler():
        Pool.log.info('Started')
        while not work_end:
            try:
                with Checker.context(Pool.lock_264), Checker.context(Pool.lock_av1), Checker.context(Pool.lock_ss):
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
    db = Database(dir_work / 'db.pkl')
    log_main = LoggingWrapper('[Main]')
    log_scanner = LoggingWrapper('[Scanner]')
    log_main.info('Started')
    threading.Thread(target = Pool.scheduler).start()
    try:
        while True:
            db.clean()
            scan_dir(dir_raw)
            for i, j in db.items():
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
                    threads_screenshot=[]
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