# Pj-Kill
A lightweight Tool for S-cluster to clean timeout jupyter jobs, which supports scheduler sweep, logging and rich CLI.

### Examples

- A single RUN
```zsh
export SUDO_PASSWD=12345 # set sudo password in env
pjkill 
```
![](.assert/overview.png)

- Sweep by scheduler
```zsh
export SUDO_PASSWD=12345 # set sudo password in env
pjkill --sweep --cycle 2 # sweep every 2 hour (1 by default)
``` 

- All the logs are stored in `~/.pjkill.log` by default
```zsh
(base) tree ~/.pjkill -L 1
/mnt/petrelfs/qudelin/.pjkill
├── PJKILLER_20230906224448.log
├── PJKILLER_20230906224657.log
├── PJKILLER_20230906224659.log
├── PJKILLER_20230906225010.log
├── PJKILLER_20230906225040.log
├── PJKILLER_20230906225159.log
├── PJKILLER_20230906225932.log
├── PJKILLER_20230906230038.log
├── PJKILLER_20230906231724.log
└── PJKILLER_20230906231905.log
```

### Install

Requires python 3.6+, install the dependencies before lunching the tool.

```zsh
pip install -r requirements.txt
pip install git+https://github.com/DelinQu/pj-kill # install from remote repo
```


### Usage

```bash
python pjkill.py --help                                                                                                                                         

usage: pjkill.py [-h] [--user USER] [--partition PARTITION] [--type TYPE] [--cycle CYCLE] [--timeout TIMEOUT] [--sweep] [--version]

Sweep all jobs on a partition and kill the timeout process.

optional arguments:
  -h, --help            show this help message and exit
  --user USER           the user your want to query
  --partition PARTITION
                        your partition
  --type TYPE           reserved | spot | all
  --cycle CYCLE         pjkill run every cycle time in hour
  --timeout TIMEOUT     timeout in hour
  --sweep               sweep around every cycle.
  --version             Display version and exit.
```

### License

MIT License, see [LICENSE.txt](LICENSE.txt)

