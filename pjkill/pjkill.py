#!/usr/bin/env python3
# coding=utf-8
from __future__ import print_function, annotations
from apscheduler.schedulers.blocking import BlockingScheduler
from rich.console import Console
from collections import Counter
from omegaconf import OmegaConf
from datetime import datetime
from rich.table import Table
from typing import Dict
from enum import Enum
import pandas as pd
import subprocess
from pathlib import Path
import argparse
import sys
import re
import os

# from logger import get_logger
from pjkill.logger import get_logger

# * version info
VERSION = "0.0.2"

# * table and states
SAFE_STATE, KILL_STATE = "🎄", "🧨"
STYLES = ["cyan", "magenta", "green", "yellow", "red", "blue", "cyan", "magenta", "green", "yellow", "cyan", "green", "magenta", "green", "yellow"] * 5

# * JOBID VIRTUAL_PARTITION NAME QUOTA_TYPE USER PHX_PRIORITY ST TIME NODES TOTAL_GRES NODELIST(REASON)
HEADER = ["jobid", "partition", "name", "quota_type", "user", "priority", "time", "total_gres", "node"]
valids = [0, 1, 2, 3, 4, 5, 7, 9, 10]

# * commands
SEQ_CMD = "squeue -p {} | grep {} | grep {}"  # * squeue -p optimal | grep reserved | grep $USER
KILL_CMD = "echo {} | sudo -S scancel {}"  # * sudo scancel 123456
SCT_CMD = "scontrol show job -ddd {}"  # * scontrol show job -ddd 123456
BST_CMD = "echo {} | sudo -S scontrol write batch_script {}"  # * scontrol write batchscript
CINFO_CMD = "cinfo -p {} occupy-reserved" # * show nodes of a specific partition

# NV_SMI_CMD = "srun -p {} -w {} nvidia-smi"
SWATCH_CMD = "swatch -n {} nv always"
PS_PID_CMD = "srun -p {} -w {} ps -o user,time,cmd -p {}"

# * kill rules
TARGETS = ["jupyter"]  # * kill the job if targets in name & runtime > timeout
class CMT(Enum):
    SAFE = "safe"
    JP_TIMEOUT = "JP_TIMEOUT"
    JP_MAX_JOB = "JP_MAX_JOB"
    JP_MAX_GPU = "JP_MAX_GPU"
    MAX_PD = "MAX_PD"
    MAX_GPU = "MAX_GPU"

def get_reserved_nodes(partition):
    NODE_LIST = []
    try:
        lines = subprocess.check_output(CINFO_CMD.format(partition), shell=True).decode("utf-8")
        lines = lines.split("\n")
        for line in lines[1:-1]:
            node = line.split(' ')[0]
            NODE_LIST.append(node)
    except:
        NODE_LIST = None
    print(f"== get node of {partition}: {NODE_LIST}")
    return NODE_LIST

def get_nvsmi(partition="optimal", node=None):
    try:
        ret = subprocess.check_output(SWATCH_CMD.format(node), shell=True).decode("ascii")
        header = ["PID", "NODE", "IDX", "USER", "Memory", "Util", "TIME", "CMD"]
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

        # * process
        idx_list, pid_list = [], [] 
        for line in ret.split("\n"):            
            if "N/A  N/A" in line:
                print(line)
                idx_list.append(int(line.split()[1]))
                pid_list.append(line.split()[4])
        
        # * sort according to pid
        sorted_pairs = sorted(zip(pid_list, idx_list), key = lambda x: int(x[0]))
        pid_list, idx_list = map(list, zip(*sorted_pairs))
        cmd = PS_PID_CMD.format(partition, node, " ".join(pid_list))
        print(f"PS_PID_CMD {cmd}")
        ret = subprocess.check_output(cmd, shell=True).decode("ascii")
        data = [x.split() for x in filter(lambda x: len(x) > 50, ret.split("\n"))]
        
        ptr, pad_data = 0, data[:1]
        for i in range(len(pid_list))[1:]:
            if pid_list[i] != pid_list[i-1]: ptr += 1
            pad_data.append(data[ptr])
        
        for pid, idx, line in zip(pid_list, idx_list, pad_data):
            data = line
            prc_data["PID"].append(str(pid))
            prc_data["IDX"].append(str(idx))
            prc_data["NODE"].append(node)
            prc_data["USER"].append(data[0])
            prc_data["TIME"].append(data[1])
            prc_data["CMD"].append(data[2])
            prc_data["Memory"].append(100 * gpu_data[idx]["Memory"])
            prc_data["Util"].append(gpu_data[idx]["Util"])
    except:
        print("** Warning: NVIDIA SMI error!")
        prc_data = {}
    
    return pd.DataFrame(prc_data).sort_values(by="IDX", ascending=True, ignore_index=True)

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

def get_jobs(user="$", partition="optimal", type="reserved", logger=None) -> dict:
    """get all the job infomation of nodes"""
    try:
        lines = subprocess.check_output(SEQ_CMD.format(partition, type, user), shell=True).decode("ascii")
        logger.info(lines)
        lines = lines.split("\n")
    except:
        return {}
    
    spt_fn = lambda s: list(filter(lambda x: x != "", s.split(" ")))
    HEADER = spt_fn(lines[0]) # ['JOBID', 'VIRTUAL_PARTITION', 'NAME', 'QUOTA_TYPE', 'USER', 'PHX_PRIORITY', 'ST', 'TIME', 'NODES', 'TOTAL_GRES', 'NODELIST(REASON)']
    jobs = {k: [] for k in HEADER + ["KTIME"]}
    for line in lines[1:-1]:
        values = spt_fn(line)
        for k, v in zip(HEADER, values):
            jobs[k].append(v)
            if k == "TIME": jobs["KTIME"].append(sec_runtime(v))
    
    # * add IDX
    jobs["IDX"] = []
    for jobid in jobs["JOBID"]:
        ret = subprocess.check_output(SCT_CMD.format(jobid), shell=True).decode("utf-8")
        matches = re.search(r"IDX:([\d,]+)", ret)
        jobs["IDX"].append(matches.group(1) if matches else "")
    
    jobs = pd.DataFrame(jobs)
    # TYPES = ["reserved", "spot"]
    # jobs = jobs[jobs['quota_type'].isin(TYPES)]
    
    jobs["KST"] = SAFE_STATE
    jobs["CMT"] = "SAFE"
    return jobs

def get_user_gpu(jobs: pd.DataFrame):
    users = jobs["USER"].unique().tolist()
    user_ngpu = {
        'user': users,
        'reserved': [0] * len(users),
        'spot': [0] * len(users)
    }
    for i, user in enumerate(jobs["USER"]):
        idx = user_ngpu['user'].index(user)
        user_ngpu[jobs["quota_type"][i]][idx] += int(jobs["TOTAL_GRES"][i].split(":")[-1])
    df = pd.DataFrame(user_ngpu)
    return df.sort_values(by="reserved", ascending=False, ignore_index=True)

def is_sbatch(cmd):
    return "/mnt/" in cmd and (" " not in cmd)

def is_target_job(jobid, SUDO_PASSWD):
    """is the job jupyter"""
    try:
        ret = subprocess.check_output(SCT_CMD.format(jobid), shell=True).decode("utf-8")
        cmd = ret.split("Command=")[1].split("\n")[0]

        in_target = any([(t in cmd) for t in TARGETS])

        # * if use a command
        if is_sbatch(cmd):
            ret = subprocess.check_output(BST_CMD.format(SUDO_PASSWD, jobid), shell=True).decode("ascii")
            script = ret.split(" ")[-1].split("\n")[0]

            ret = subprocess.check_output("cat {}".format(script), shell=True).decode("utf-8")
            in_target = in_target or any([(t in ret) for t in TARGETS])

            # * remove the batch script
            os.remove(script)
    except:
        in_target = False
        cmd = "** Warning: An error was detected in this job."

    return in_target, cmd

def kill_jp_jobs(jobs: pd.DataFrame, cfg, SUDO_PASSWD, unkill=True, logger=None):
    """kill the timeout process"""
    if jobs.empty:
        print(f"** Jupyter Job Killer Receive an empty job DataFrame! **")
        return jobs

    # * sort all job key:value by time
    jobs = jobs.sort_values(by="KTIME", ascending=False, ignore_index=True)

    # * query job target and count user number in target
    job_valids, cmds = zip(*map(is_target_job, jobs["JOBID"], [SUDO_PASSWD] * len(jobs["JOBID"])))
    user_njob = Counter([user for user, valid in zip(jobs["USER"], job_valids) if valid]) # * {'user1': 2, 'user2': 1}
    timeout = cfg["jp_timeout"]

    for i, job in jobs.iterrows():
        user, in_target, cmd, jobid = job["USER"], job_valids[i], cmds[i], job["JOBID"]
        """ * kill the job if in target and:
        1. runtime > timeout
        2. or gpu > max_ngpu_every_jp
        3. or user_njob > max_jp_njob
        """
        gpus = int(job["TOTAL_GRES"].split(":")[-1])
        if in_target and (job["KTIME"] > timeout * 3600 or gpus > cfg["max_ngpu_every_jp"] or user_njob[user] > cfg["max_jp_njob"]):
            try:
                if not unkill:
                    subprocess.check_output(KILL_CMD.format(SUDO_PASSWD, jobid), shell=True)
                jobs.at[i, "KST"] = KILL_STATE
                jobs.at[i, "CMT"] = CMT.JP_TIMEOUT.value if job["KTIME"] > timeout * 3600 else CMT.JP_MAX_GPU.value if gpus > cfg["max_ngpu_every_jp"] else CMT.JP_MAX_JOB.value

                if user_njob[user] > cfg["max_jp_njob"]:
                    user_njob[user] -= 1
                logger.info(f"== jpyter jobid: [{jobid}], cmd: [{cmd}], was killed")
            except:
                logger.info(f"** jpyter jobid: {jobid}, cmd: {cmd}, killing failed!")
        else:
            logger.info(f"== jpyter jobid: [{jobid}], cmd: [{cmd}], stays safe")
    return jobs

def kill_cuda_jobs(jobs: pd.DataFrame, cfg, SUDO_PASSWD, unkill=True, logger=None):
    """kill the timeout process"""
    if jobs.empty:
        print(f"** CUDA Job Killer Receive an empty job DataFrame! **")
        return jobs

    # * sort all job key:value by time
    jobs = jobs.sort_values(by="KTIME", ascending=False, ignore_index=True)

    # * query job target and count user number in target
    job_valids, cmds = zip(*map(is_target_job, jobs["JOBID"], [SUDO_PASSWD] * len(jobs["JOBID"])))
    user_njob = Counter([user for user, valid in zip(jobs["USER"], job_valids) if valid]) # * {'user1': 2, 'user2': 1}
    timeout = cfg["jp_timeout"]

    for i, job in jobs.iterrows():
        user, in_target, cmd, jobid = job["USER"], job_valids[i], cmds[i], job["JOBID"]
        """ * kill the job if in target and:
        1. runtime > timeout
        2. or gpu > max_ngpu_every_jp
        3. or user_njob > max_jp_njob
        """
        gpus = int(job["TOTAL_GRES"].split(":")[-1])
        if in_target and (job["KTIME"] > timeout * 3600 or gpus > cfg["max_ngpu_every_jp"] or user_njob[user] > cfg["max_jp_njob"]):
            try:
                if not unkill:
                    subprocess.check_output(KILL_CMD.format(SUDO_PASSWD, jobid), shell=True)
                jobs.at[i, "KST"] = KILL_STATE
                jobs.at[i, "CMT"] = CMT.JP_TIMEOUT.value if job["KTIME"] > timeout * 3600 else CMT.JP_MAX_GPU.value if gpus > cfg["max_ngpu_every_jp"] else CMT.JP_MAX_JOB.value

                if user_njob[user] > cfg["max_jp_njob"]:
                    user_njob[user] -= 1
                logger.info(f"== jpyter jobid: [{jobid}], cmd: [{cmd}], was killed")
            except:
                logger.info(f"** jpyter jobid: {jobid}, cmd: {cmd}, killing failed!")
        else:
            logger.info(f"== jpyter jobid: [{jobid}], cmd: [{cmd}], stays safe")
    return jobs

def kill_norm_jobs(jobs: pd.DataFrame, cfg, SUDO_PASSWD, unkill=True, logger=None):
    """kill the num exceed jobs"""
    if jobs.empty:
        print(f"** Norm Job Killer Receive an empty job DataFrame! **")
        return jobs

    # * sort all job key:value by time
    jobs = jobs.sort_values(by="KTIME", ascending=True, ignore_index=True)

    # * query job target and count user number in target
    user_ngpu = {k: 0 for k in jobs["USER"].unique().tolist()}
    user_exceed = {k: False for k in jobs["USER"].unique().tolist()}
    job_valids = [False] * len(jobs["JOBID"])

    for i, job in jobs.iterrows():
        if job["KST"] == SAFE_STATE and job["ST"] != "PD" and job["PHX_PRIORITY"] != "P0":
            user_ngpu[job["USER"]] += int(job["TOTAL_GRES"].split(":")[-1])
            job_valids[i] = True
    
    # * kill exceed the number limit jobs
    for i, job in jobs.iterrows():
        """ kill the job if exceed the number limit. """
        jobid, user = job["JOBID"], job["USER"]
        if user_ngpu[user] > cfg["max_ngpu"] and job_valids[i]:
            try:
                if not unkill:
                    subprocess.check_output(KILL_CMD.format(SUDO_PASSWD, jobid), shell=True)
                jobs.at[i, "KST"] = KILL_STATE
                jobs.at[i, "CMT"] = CMT.MAX_GPU.value
                logger.info(f"== jobid: [{jobid}], exceeded maxgpu number, was killed")
                user_ngpu[user] -= int(job["TOTAL_GRES"].split(":")[-1])
                user_exceed[user] = True # mark as user_exceed
            except:
                logger.info(f"** jobid: {jobid}, exceeded maxgpu number, but killing failed!")
        else:
            logger.info(f"== jobid: [{jobid}], under maxgpu number, stays safe")

    # * kill PD jobs
    pdjob_valids = [False] * len(jobs["JOBID"])
    for i, job in jobs.iterrows():
        if job["KST"] == SAFE_STATE and job["ST"] == "PD" and job["PHX_PRIORITY"] != "P0": pdjob_valids[i] = True
    user_npdjob = Counter([user for user, valid in zip(jobs["USER"], pdjob_valids) if valid])

    for i, job in jobs.iterrows():
        jobid, user = job["JOBID"], job["USER"]
        if user_exceed[user] and user_npdjob[user] > cfg["max_exce_pd"]:
            try:
                if not unkill:
                    subprocess.check_output(KILL_CMD.format(SUDO_PASSWD, jobid), shell=True)
                jobs.at[i, "KST"], jobs.at[i, "CMT"] = KILL_STATE, CMT.MAX_PD.value
                user_npdjob[user] -= 1
                logger.info(f"== jobid: [{jobid}], kill PD exceeding maxgpu number")
            except:
                logger.info(f"** jobid: {jobid}, PD exceeded maxgpu number, but killing failed!")
        else:
            logger.info(f"== jobid: [{jobid}], under max PD number, stays safe")
    return jobs

def viz_jobs(jobs, title="PJKILL"):
    """visualize the jobs"""
    console = Console()
    table = Table(title=title, show_header=True, header_style="bold magenta")
    
    for i, k in enumerate(jobs.keys()):
        table.add_column(k, style=STYLES[i])

    for _, row in jobs.iterrows():
        table.add_row(*[str(row[k]) for k in jobs.keys()])
    console.print(table)

def viz_user_ngpu(user_ngpu):
    """visualize the user gpu"""
    console = Console()
    table = Table(title="User-GPU Occupation", show_header=True, header_style="bold magenta")
    for i, k in enumerate(user_ngpu.keys()):
        table.add_column(k, style=STYLES[i])

    for i in range(len(user_ngpu["user"])):
        table.add_row(*[str(user_ngpu[k][i]) for k in user_ngpu.keys()])
    console.print(table)

def threads_job(cfg, logger):
    # * get all jobs
    all_jobs = get_jobs(cfg["user"], cfg["partition"], "$", logger=logger)
    all_jobs["USER"] = all_jobs["USER"].str[:7]
    
    reserved_jobs = all_jobs[all_jobs['QUOTA_TYPE'] == 'reserved'].copy().reset_index(drop=True) 
    spot_jobs = all_jobs[all_jobs['QUOTA_TYPE'] == 'spot'].copy().reset_index(drop=True)
    
    # * get CUDA PROCESS
    # RESERVED_NODE_LIST = get_reserved_nodes(cfg["partition"])
    # # RESERVED_NODE_LIST = ["SH-IDC1-10-140-1-164"]
    # cuda_procs = pd.concat([get_nvsmi(cfg["partition"], node) for node in RESERVED_NODE_LIST])
    # cuda_procs["USER"] = cuda_procs["USER"].str[:7]

    # # split jobs
    # split_reserved_jobs = reserved_jobs.copy(deep=True)
    # split_reserved_jobs['IDX'] = split_reserved_jobs['IDX'].str.split(',')
    # split_reserved_jobs = split_reserved_jobs.explode('IDX')
    # left_merged = pd.merge(split_reserved_jobs, cuda_procs, left_on=['NODELIST(REASON)', 'USER', "IDX"], right_on=['NODE', 'USER', "IDX"], how='left')

    # viz_jobs(reserved_jobs, title="RESERVED JOBS")
    # viz_jobs(cuda_procs)
    # viz_jobs(left_merged)

    # TODO: Add CUDA KILLER with a History Record

    # * kill reserved jobs
    reserved_jobs = kill_jp_jobs(reserved_jobs, cfg.reserved, cfg.SUDO_PASSWD, cfg.unkill, logger)
    reserved_jobs = kill_norm_jobs(reserved_jobs, cfg.reserved, cfg.SUDO_PASSWD, cfg.unkill, logger)

    # * kill spot jobs
    spot_jobs = kill_jp_jobs(spot_jobs, cfg.spot, cfg.SUDO_PASSWD, cfg.unkill, logger)
    spot_jobs = kill_norm_jobs(spot_jobs, cfg.spot, cfg.SUDO_PASSWD, cfg.unkill, logger)
    
    # * visulize
    viz_jobs(reserved_jobs, title="RESERVED JOBS")
    viz_jobs(spot_jobs, title="SPOT JOBS")

def main():
    """Parse and return the arguments."""
    parser = argparse.ArgumentParser(description="sweep all jobs on a partition and kill the timeout process.")
    parser.add_argument("--cfg", type=str, default=f"{os.environ['HOME']}/.pjkill/config.yaml", help=f"pjkill config file, {os.environ['HOME']}/.pjkill/config.yaml by default")
    parser.add_argument("--sweep", action="store_true", help="sweep around every cycle, False by default")
    parser.add_argument("--unkill", action="store_true", help="unkill the job to stay safe False by default")
    parser.add_argument("--version", action="store_true", help="display version and exit, False by default")
    args = vars(parser.parse_args())

    if args["version"]:
        print("pjkill v{}".format(VERSION))
        sys.exit()

    # * load cfg
    cfg = OmegaConf.load(args["cfg"])
    
    # * log, name with PJKILLER + date
    log_path = f'{os.environ["HOME"]}/.pjkill/log'
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    logger = get_logger("PJKILLER", log_path)

    if args["sweep"]:
        # * sweep
        schedule = BlockingScheduler()
        schedule.add_job(threads_job, "interval", seconds=cfg["cycle"] * 60, id="PJ_KILLER", args=[cfg, logger], next_run_time=datetime.now())
        schedule.start()
    else:
        # * single
        threads_job(cfg, logger)

if __name__ == "__main__":
    main()
