import re

def convert_to_int_time(time_str):
    # 定义正则表达式模式
    pattern = r'^(?:(\d+)-)?(?:([01]?\d|2[0-3]):)?([0-5]?\d):([0-5]?\d)$'

    # 匹配并提取天数、小时、分钟和秒数
    match = re.match(pattern, time_str)
    if match:
        days = int(match.group(1) or 0)
        hours = int(match.group(2) or 0)
        minutes = int(match.group(3) or 0)
        seconds = int(match.group(4) or 0)

        print(f"days: {days}, hours: {hours}, minutes: {minutes}, seconds: {seconds}")

        # 将时间转换为整数表示
        int_time = days * 24 * 3600 + hours * 3600 + minutes * 60 + seconds
        return int_time
    else:
        return "Invalid time format"

# 示例用法 0:06 , 5:56:05, 1-06:36:29
time_str = ["0:06", "100-01:59:06", "30:06", "5:56:05", "12:56:05", "1-06:36:29", "10-06:36:29"]
for t in time_str:
    int_time = convert_to_int_time(t)
    print(int_time)