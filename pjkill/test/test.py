from pathlib import Path

def is_valid_path(path):
    try:
        Path(path)
        return True
    except:
        return False

path="bash /mnt/petrelfs/jinglinglin/Event_segmentation3"

print(is_valid_path(path))