import re
class Duration:
    reg = re.compile()
    def __init__(self, time):
        self.update(time)

    def update(self, time: str | int | float):
        if isinstance(time, str):

            self.time = int(time[:2]) * 3600 + int(time[3:5]) * 60 + float(time[6:])
        elif isinstance(time, int | float):
            self.time = time
        else:
            raise ValueError(f'Can not initialize a Duration object with {type(time)}')

    def __str__(self):
        time = int(self.time)
        hours, remainder = divmod(time, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f'{hours:02d}:{minutes:02d}:{seconds:02d}'

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

    
    # object.__le__(self, other)
    # object.__eq__(self, other)¶
    # object.__ne__(self, other)
    # object.__gt__(self, other)
    # object.__ge__(self, other)