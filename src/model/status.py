import enum
class Status(enum.Enum):
    OK = 0
    WARNING = 1
    FAILED = 2
    UNKNOWN = -1

def get_color_hex_for_status(status: Status):
    if status == Status.OK:
        return "#00FF00"
    elif status == Status.WARNING:
        return "#FFFF00"
    elif status == Status.FAILED:
        return "#FF0000"
    else:
        return "#bcbcbc"