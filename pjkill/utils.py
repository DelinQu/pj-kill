import subprocess
import pandas as pd
import re

NV_SMI_CMD = "srun -p {} -w {} nvidia-smi"
PS_PID_CMD = "srun -p {} -w {} ps -o user,time,cmd -p {}"

def get_nvsmi(partition="optimal", node=None):
    try:
        ret = subprocess.check_output(NV_SMI_CMD.format(partition, node), shell=True).decode("ascii")
        header = ["PID", "IDX", "USER", "Memory", "Util", "TIME", "CMD"]
        gpu_data = {k: {} for k in range(8)}
        prc_data = {k: [] for k in header}
        idx = 0
        for line in ret.split("\n"):
            # gpu occupy
            if "MiB / " in line:
                mems = list(filter(lambda x: "MiB" in x, line.split()))
                utls = list(filter(lambda x: "%" in x, line.split()))
                gpu_data[idx]["Memory"] = float(mems[0][:-3]) / float(mems[1][:-3])
                gpu_data[idx]["Util"] = float(utls[0][:-1])
                idx += 1

        for line in ret.split("\n"):            
            # process
            if "N/A  N/A" in line:
                idx = int(line.split()[1])
                pid = line.split()[4]
                
                ret = subprocess.check_output(PS_PID_CMD.format(partition, node, pid), shell=True).decode("ascii")
                data = ret.split("\n")[1].split()
                prc_data["PID"].append(pid)
                prc_data["IDX"].append(idx)
                prc_data["USER"].append(data[0])
                prc_data["TIME"].append(data[1])
                prc_data["CMD"].append(data[2])
                prc_data["Memory"].append(100 * gpu_data[idx]["Memory"])
                prc_data["Util"].append(gpu_data[idx]["Util"])
    except:
        print("** Warning: NVIDIA SMI error!")
        prc_data = {}
    
    return pd.DataFrame(prc_data)

def sec_runtime(time_str):
    pattern = r"^(?:(\d+)-)?(?:([01]?\d|2[0-3]):)?([0-5]?\d):([0-5]?\d)$"
    match = re.match(pattern, time_str)
    if match:
        days = int(match.group(1) or 0)
        hours = int(match.group(2) or 0)
        minutes = int(match.group(3) or 0)
        seconds = int(match.group(4) or 0)
        sec_time = days * 24 * 3600 + hours * 3600 + minutes * 60 + seconds
        return sec_time
    else:
        return "Invalid time format"

if __name__=="__main__":
    ret = get_nvsmi(partition="optimal", node="SH-IDC1-10-140-1-164")
    print(ret)