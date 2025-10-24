def convert_time(date_8: str, tm: str = "15:00:00") -> str:
    return f"{date_8[0:4]}-{date_8[4:6]}-{date_8[6:8]} {tm}"
