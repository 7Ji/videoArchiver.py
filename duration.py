import datetime
import pathlib
import subprocess
import re
reg_complete = {
    'video': re.compile(r'video:(\d+)kB'),
    'audio': re.compile(r'audio:(\d+)kB')
}
reg_running = re.compile(r'size= *(\d+)kB')
reg_time = re.compile(r' time=([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{2}) ')
def get_duration_and_size(media: pathlib.Path, stream_id: int, stream_type: str):
    try:
        p = subprocess.Popen(('ffmpeg', '-i', media, '-c', 'copy', '-map', f'0:{stream_id}', '-f', 'null', '-'), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
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
                m = reg_time.search(line)
                if m:
                    t = m[1]
        m = reg_complete[stream_type].search(line)
        if m:
            s = int(m[1])
        p.wait()
    except KeyboardInterrupt:
        p.kill()
    return datetime.timedelta(
        hours = int(t[:2]),
        minutes = int(t[3:5]),
        seconds = float(t[6:])
    ), s

print(get_duration_and_size('E:\\Python\\220302_SizeCutter\\testFile.mkv', 0, 'video'))

