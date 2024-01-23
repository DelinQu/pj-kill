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
VERSION = "0.0.1"

# * table and states
# STATES = ["🎄", "🧨"]
SAFE_STATE, KILL_STATE = "🎄", "🧨"
STYLES = ["cyan", "magenta", "green", "yellow", "red", "blue", "cyan", "magenta", "green", "yellow", "cyan"]

# * JOBID VIRTUAL_PARTITION NAME QUOTA_TYPE USER PHX_PRIORITY ST TIME NODES TOTAL_GRES NODELIST(REASON)
HEADER = ["jobid", "partition", "name", "quota_type", "user", "priority", "time", "total_gres", "node"]
valids = [0, 1, 2, 3, 4, 5, 7, 9, 10]

# * commands
SEQ_CMD = "squeue -p {} | grep {} | grep {}"  # * squeue -p optimal | grep reserved | grep $USER
KILL_CMD = "echo {} | sudo -S scancel {}"  # * sudo scancel 123456
SCT_CMD = "scontrol show job -ddd {}"  # * scontrol show job -ddd 123456
BST_CMD = "echo {} | sudo -S scontrol write batch_script {}"  # * scontrol write batchscript
CINFO_CMD = "cinfo -p {} occupy-reserved" # * show nodes of a specific partition

# * kill rules
TARGETS = ["jupyter"]  # * kill the job if targets in name & runtime > timeout

def init_args() -> Dict:
    """Parse and return the arguments."""
    parser = argparse.ArgumentParser(description="sweep all jobs on a partition and kill the timeout process.")
    parser.add_argument("--cfg", type=str, default=f"{os.environ['HOME']}/.pjkill/config.yaml", help=f"pjkill config file, {os.environ['HOME']}/.pjkill/config.yaml by default")
    parser.add_argument("--sweep", action="store_true", help="sweep around every cycle, False by default")
    parser.add_argument("--unkill", action="store_true", help="unkill the job to stay safe False by default")
    parser.add_argument("--version", action="store_true", help="display version and exit, False by default")
    args = vars(parser.parse_args())
    return args

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

def get_nodes(partition):
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

def get_jobs(user="$", partition="optimal", type="reserved", logger=None) -> dict:
    """get all the job infomation of nodes"""
    try:
        lines = subprocess.check_output(SEQ_CMD.format(partition, type, user), shell=True).decode("ascii")
        logger.info(lines)
        lines = lines.split("\n")
    except:
        return {}
    jobs = {k: [] for k in HEADER + ["stime"]}
    TYPES = ["reserved", "spot"]

    for line in lines[1:-1]:
        # * filter out the space and empty string
        values = list(filter(lambda x: x != "", line.split(" ")))
        values = [values[i] for i in valids]
        if "SH-" not in values[-1]: continue
        for k, v in zip(HEADER, values):
            if k == "time":
                jobs["stime"].append(sec_runtime(v))
            jobs[k].append(v)
    jobs = pd.DataFrame(jobs)
    jobs = jobs[jobs['quota_type'].isin(TYPES)]
    jobs["status"] = SAFE_STATE
    return jobs

def get_user_gpu(jobs: pd.DataFrame):
    users = jobs['user'].unique().tolist()
    user_ngpu = {
        'user': users,
        'reserved': [0] * len(users),
        'spot': [0] * len(users)
    }
    for i, user in enumerate(jobs['user']):
        idx = user_ngpu['user'].index(user)
        user_ngpu[jobs["quota_type"][i]][idx] += int(jobs["total_gres"][i].split(":")[-1])
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
        cmd = "Warning: An error was detected in this job."

    return in_target, cmd

def kill_jp_jobs(timeout, jobs: pd.DataFrame, cfg, logger=None):
    """kill the timeout process"""
    # * sort all job key:value by time
    jobs = jobs.sort_values(by="stime", ascending=False, ignore_index=True)

    # * query job target and count user number in target
    job_valids, cmds = zip(*map(is_target_job, jobs["jobid"], [cfg.SUDO_PASSWD] * len(jobs["jobid"])))
    user_num = Counter([user for user, valid in zip(jobs["user"], job_valids) if valid]) # * {'user1': 2, 'user2': 1}

    for i, runtime in enumerate(jobs.stime):
        user, in_target, cmd, jobid = jobs["user"][i], job_valids[i], cmds[i], jobs["jobid"][i]
        """ * kill the job if in target and:
        1. runtime > timeout
        2. or gpu > 2
        3. or user_num > 2
        """
        gpus = int(jobs["total_gres"][i].split(":")[-1])
        if in_target and (runtime > timeout * 3600 or gpus > cfg["jp_ngpu"] or user_num[user] > cfg["jp_njob"]):
            try:
                if not cfg["unkill"]:
                    subprocess.check_output(KILL_CMD.format(cfg.SUDO_PASSWD, jobid), shell=True)
                jobs["status"][i] = KILL_STATE
                logger.info(f"== jpyter jobid: [{jobid}], cmd: [{cmd}], was killed")
                
                if user_num[jobs["user"][i]] > cfg["jp_njob"]:
                    user_num[jobs["user"][i]] -= 1
            except:
                logger.info(f"== jpyter jobid: {jobid}, cmd: {cmd}, killing failed!")
        else:
            logger.info(f"== jpyter jobid: [{jobid}], cmd: [{cmd}], stays safe")

        # * add ratio
        jobs["time"][i] += " / {}".format("{:d}-{:02d}:{:02d}:{:02d}".format(timeout // 24, timeout % 24, 0, 0))
    return jobs

def kill_reserved_jobs(jobs: pd.DataFrame, cfg, logger=None):
    """kill the num exceed jobs"""
    # * sort all job key:value by time
    jobs = jobs.sort_values(by="stime", ascending=True, ignore_index=True)

    # * query job target and count user number in target
    user_ngpu = {k: 0 for k in jobs['user'].unique().tolist()}
    job_valids = [False] * len(jobs["jobid"])

    for i, user in enumerate(jobs['user']):
        # print(f"== status: {jobs['status'][i]}, quota_type: {jobs['quota_type'][i]}, priority: {jobs['priority'][i]}")
        if jobs["status"][i] == SAFE_STATE and jobs["quota_type"][i] == "reserved" and jobs["priority"][i] == "normal":
            user_ngpu[user] += int(jobs["total_gres"][i].split(":")[-1])
            job_valids[i] = True
        
    for i, user in enumerate(jobs["user"]):
        """ *kill the job if exceed the number limit. """
        jobid = jobs["jobid"][i]

        if user_ngpu[user] > cfg["reserved_ngpu"] and job_valids[i]:
            try:
                if not cfg["unkill"]:
                    subprocess.check_output(KILL_CMD.format(cfg.SUDO_PASSWD, jobid), shell=True)
                jobs["status"][i] = KILL_STATE
                logger.info(f"== reserved jobid: [{jobid}], exceeded maxgpu number, was killed")

                user_ngpu[user] -= int(jobs["total_gres"][i].split(":")[-1])
            except:
                logger.info(f"== reserved jobid: {jobid}, exceeded maxgpu number, but killing failed!")
        else:
            logger.info(f"== reserved jobid: [{jobid}], under maxgpu number, stays safe")

    return jobs

def kill_spot_jobs(jobs: pd.DataFrame, cfg, logger=None):
    """kill the num exceed jobs"""
    # * sort all job key:value by time
    jobs = jobs.sort_values(by="stime", ascending=True, ignore_index=True)

    # * query job target and count user number in target
    user_ngpu = {k: 0 for k in jobs['user'].unique().tolist()}
    job_valids = [False] * len(jobs["jobid"])

    for i, user in enumerate(jobs['user']):
        if jobs["status"][i] == SAFE_STATE and jobs["quota_type"][i] == "spot":
            user_ngpu[user] += int(jobs["total_gres"][i].split(":")[-1])
            job_valids[i] = True
        
    for i, user in enumerate(jobs["user"]):
        """ *kill the job if exceed the number limit. """
        jobid = jobs["jobid"][i]

        if user_ngpu[user] > cfg["spot_ngpu"] and job_valids[i]:
            try:
                if not cfg["unkill"]:
                    subprocess.check_output(KILL_CMD.format(cfg.SUDO_PASSWD, jobid), shell=True)
                jobs["status"][i] = KILL_STATE
                logger.info(f"== spot jobid: [{jobid}], exceeded maxgpu number, was killed")

                user_ngpu[user] -= int(jobs["total_gres"][i].split(":")[-1])
            except:
                logger.info(f"== spot jobid: {jobid}, exceeded maxgpu number, but killing failed!")
        else:
            logger.info(f"== spot jobid: [{jobid}], under maxgpu number, stays safe")

    return jobs

def viz_jobs(jobs, title="PJKILL"):
    """visualize the jobs"""
    console = Console()
    table = Table(title=title, show_header=True, header_style="bold magenta")
    for i, k in enumerate(jobs.keys()):
        table.add_column(k, style=STYLES[i])

    for i in range(len(jobs["jobid"])):
        table.add_row(*[str(jobs[k][i]) for k in jobs.keys()])
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
    # * get optimal NODES
    NODE_LIST = get_nodes(cfg["partition"])

    # * get all jobs
    all_jobs = get_jobs(cfg["user"], cfg["partition"], "$", logger=logger)
    vp_jobs, nvp_jobs = all_jobs[all_jobs['node'].isin(NODE_LIST)].copy(), all_jobs[~all_jobs['node'].isin(NODE_LIST)].copy()

    # * kill jupyter jobs in vp
    vp_jobs = kill_jp_jobs(cfg["jp_timeout"], vp_jobs, cfg, logger)

    # * kill reserved jobs in vp
    vp_jobs = kill_reserved_jobs(vp_jobs, cfg, logger)

    # * kill spot jobs in other partition
    nvp_jobs = kill_spot_jobs(nvp_jobs, cfg, logger)

    # * show user gpu info
    info_user_ngpu = get_user_gpu(all_jobs)
    viz_user_ngpu(info_user_ngpu)

    # * show vp jobs and nvp jobs 
    logger.info("\n")
    viz_jobs(vp_jobs.drop('stime', axis=1), title=f"PJKILLER is Sweeping the Job List on {cfg['partition']}!")

    logger.info("\n")
    viz_jobs(nvp_jobs.drop('stime', axis=1), title="PJKILLER is Sweeping the Job List on Other Partition!")

def main():
    args = init_args()
    if args["version"]:
        print("pjkill v{}".format(VERSION))
        sys.exit()

    # * load cfg
    cfg = OmegaConf.load(args["cfg"])
    cfg["unkill"] = args["unkill"]
    
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
