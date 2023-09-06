#!/usr/bin/env python3
# coding=utf-8
from __future__ import print_function, annotations
import re
import subprocess
from typing import Dict
import argparse
from rich.console import Console
from rich.table import Table
import sys
import os
from logger import get_logger
from apscheduler.schedulers.blocking import BlockingScheduler

# * version info
VERSION = "0.0.1"

# * table and states
STATES = ["🎄", "🧨"]
STYLES = ["cyan", "magenta", "green", "yellow", "red", "blue", "cyan", "magenta", "green", "yellow"]

# * JOBID VIRTUAL_PARTITION NAME QUOTA_TYPE USER PHX_PRIORITY ST TIME NODES TOTAL_GRES NODELIST(REASON)
HEADER = ["jobid", "partition", "name", "quota_type", "user", "priority", "time", "total_gres", "node"]
valids = [0, 1, 2, 3, 4, 5, 7, 9, 10]

# * commands
SEQ_CMD = "squeue -p {} | grep {} | grep {}"    # * squeue -p optimal | grep reserved | grep $USER
KILL_CMD = "echo {} | sudo -S scancel {}"       # * sudo scancel 123456
SCT_CMD = "scontrol show job -ddd {}"           # * scontrol show job -ddd 123456

def init_args() -> Dict:
    """Parse and return the arguments."""
    parser = argparse.ArgumentParser(description="Sweep all jobs on a partition and kill the timeout process.")
    parser.add_argument("--user", default="$", help="the user your want to query")
    parser.add_argument("--partition", default="optimal", help="your partition")
    parser.add_argument("--type", default="reserved", help="reserved | spot | all")
    parser.add_argument("--cycle", default=1, help="pjkill run every cycle time in hour")
    parser.add_argument("--timeout", default=10, help="timeout in hour")
    parser.add_argument("--sweep", action="store_true", help="sweep around every cycle.")
    parser.add_argument("--version", action="store_true", help="Display version and exit.")
    args = vars(parser.parse_args())
    return args


def get_jobs(user="$", partition="optimal", type="reserved", logger=None) -> dict:
    """get all the job infomation of nodes"""
    try:
        lines = subprocess.check_output(SEQ_CMD.format(partition, type, user), shell=True).decode("ascii")
        logger.info(lines)
        lines = lines.split("\n")
    except:
        return {}
    jobs = {k: [] for k in HEADER}

    for line in lines[:-1]:
        # * filter out the space and empty string
        values = list(filter(lambda x: x != "", line.split(" ")))
        values = [values[i] for i in valids]

        for k, v in zip(HEADER, values):
            jobs[k].append(v)
    return jobs


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


def kill_jobs(timeout, jobs: Dict, logger=None):
    """kill the timeout process"""
    jobs["status"] = []
    for i, ts in enumerate(jobs["time"]):
        runtime = sec_runtime(ts)

        # * kill the job if runtime > timeout
        if runtime > timeout * 3600:
            logger.info("== kill job {} ==".format(jobs["jobid"][i]))
            try:
                # subprocess.check_output(KILL_CMD.format(os.environ["SUDO_PASSWD"], jobs["jobid"][i]), shell=True)
                jobs["status"].append(STATES[1])
            except:
                print("== kill job {} failed ==".format(jobs["jobid"][i]))
                jobs["status"].append(STATES[0])
        else:
            jobs["status"].append(STATES[0])

        # * add ratio
        jobs["time"][i] += " / {}".format("{:d}-{:02d}:{:02d}:{:02d}".format(timeout // 24, timeout % 24, 0, 0))
    return jobs


def viz_jobs(jobs: Dict):
    """visualize the jobs"""
    console = Console()
    table = Table(title="PJKILLER is Sweeping the Job List!", show_header=True, header_style="bold magenta")
    for i, k in enumerate(jobs.keys()):
        table.add_column(k, style=STYLES[i])

    for i in range(len(jobs["jobid"])):
        table.add_row(*[jobs[k][i] for k in jobs.keys()])
    console.print(table)


def threads_job(args, logger):
    jobs = get_jobs(args["user"], args["partition"], args["type"], logger)
    njobs = kill_jobs(args["timeout"], jobs, logger)
    viz_jobs(njobs)


def main():
    args = init_args()
    if args["version"]:
        print("pjkill v{}".format(VERSION))
        sys.exit()

    # * log, name with PJKILLER + date
    log_name = "PJKILLER"
    logger = get_logger(log_name, f'{os.environ["HOME"]}/.pjkill')

    if args["sweep"]:
        # * sweep
        schedule = BlockingScheduler()
        schedule.add_job(threads_job, 'interval', seconds=args["cycle"] * 60 * 60, id='PJ_KILLER', args=[args, logger])
        schedule.start()
    else:
        # * single
        threads_job(args, logger)

if __name__ == "__main__":
    main()
