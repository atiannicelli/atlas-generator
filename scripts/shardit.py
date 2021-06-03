#!/usr/bin/python3

import argparse
import logging
import subprocess
from datetime import date

def setup_logging(default_level=logging.INFO):
    """
    Setup logging configuration
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=default_level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("CloudAtlasChecksControl")

def shardit(args):
	sharding_content = ""
	shard_local = "/tmp/sharding.txt"
	s3ShardFile = args.input + args.shardingFileName
	aws_cmd = ["aws", "s3", "cp", s3ShardFile, shard_local]
	aws_s3_cp = subprocess.run(aws_cmd, stdout=subprocess.PIPE, text=True)
	if 	aws_s3_cp.returncode:
		logger.error(f"Unable to fetch {s3ShardFile}. ({aws_s3_cp.returncode})")
		exit -1
	with open(shard_local, 'r') as input_file:
		sharding_content = input_file.readlines()

	aws_cmd = ["aws", "s3", "ls", "--recursive", args.input]
	s3_files = subprocess.run(aws_cmd, stdout=subprocess.PIPE, text=True)
	if 	s3_files.returncode:
		logger.error(f"Unable to list {args.input}. ({s3_files.returncode})")
		exit -1
	changes = False
	for line in s3_files.stdout.split("\n"):
		if len(line.split()) < 2:
			continue
		shard_size = int(line.split()[2])
		if shard_size > (args.max * 1024 * 1024):
			shard = line.split("/")[6].split(".")[0]
			shardz, shardx, shardy = shard.split("-")
			shardz = int(shardz)
			shardx = int(shardx)
			shardy = int(shardy)
			print (f"Shard {shard} too big ({shard_size} bytes).")

			if shard in sharding_content[-1] and "+" not in sharding_content[-1]:  # Handle last line to prevent IndexError
				print (f"Found {shard} in last line: {sharding_content[-1]}. Breaking shard up...")
				sharding_content[-1] = shard + "+\n"
				sharding_content.append(f"{shardz+1}-{shardx*2}-{shardy*2}\n")
				sharding_content.append(f"{shardz+1}-{shardx*2}-{shardy*2+1}\n")
				sharding_content.append(f"{shardz+1}-{shardx*2+1}-{shardy*2}\n")
				sharding_content.append(f"{shardz+1}-{shardx*2+1}-{shardy*2+1}\n")
				changes=True
			else:
				for index, line in enumerate(sharding_content):
					if shard in line and "+" not in line:
						print (f"Found {shard} in line {index}: {line}.  Breaking shard up...")
						sharding_content[index] = shard + "+\n"
						sharding_content.insert(index + 1, f"{shardz+1}-{shardx*2}-{shardy*2}\n")
						sharding_content.insert(index + 2, f"{shardz+1}-{shardx*2}-{shardy*2+1}\n")
						sharding_content.insert(index + 3, f"{shardz+1}-{shardx*2+1}-{shardy*2}\n")
						sharding_content.insert(index + 4, f"{shardz+1}-{shardx*2+1}-{shardy*2+1}\n")
						changes=True
						break
	if changes:
		with open(shard_local, 'w') as output_file:
			output_file.writelines(sharding_content)
		shard_date = date.today().strftime("%Y%m%d")
		sharding_s3file = args.s3Utils + f"sharding_quadtree_{shard_date}.txt"
		aws_cmd = ["aws", "s3", "cp", shard_local, sharding_s3file]
		aws_s3_cp = subprocess.run(aws_cmd, stdout=subprocess.PIPE, text=True)
		if 	aws_s3_cp.returncode:
			finish(f"ERROR: Unable to put {sharding_s3file}. ({aws_s3_cp.returncode})", -1)

def parse_args():
    """Parse user parameters

    :returns: args
    """
    parser = argparse.ArgumentParser(
        description="This script reads from AWS to look at the size of shards. "
        "If shards are too big then it will split up large shards in the sharding.txt"
    )
    parser.add_argument(
        '--s3Utils',
        default="s3://cosmos-atlas-dev/PBF_Sharding/utils/",
        type=str,
        help="The S3Utils folder",
    )
    parser.add_argument(
        '--input',
        required=True,
        type=str,
        help="The shard file directory to look at for shard sizes and sharding.txt file",
    )
    parser.add_argument(
        '--shardingFileName',
        default="sharding.txt",
        type=str,
        help="The shard file directory to look at for shard sizes and sharding.txt file",
    )
    parser.add_argument(
        '--max',
        default=5,
        type=int,
        help="Maximum size allowed for a shard.",
    )

    args = parser.parse_args()
    return args


logger = setup_logging()

if __name__ == "__main__":
    args = parse_args()
    shardit(args)
