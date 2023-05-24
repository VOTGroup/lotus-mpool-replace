#!/usr/bin/python3

import argparse
import os
import signal
import statistics
import subprocess
import sys
import time
import threading
from datetime import datetime
import logging
import json

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--min-fee",
        help="This is the minimum fee you would like the program to target for auto replacement of mpool messages. (Default: 0.35 Fil)",
        type=float,
        default=os.environ.get("MIN_FEE", 0.35),
        required=False,
    )
    parser.add_argument(
        "--max-fee",
        help="This is the maximum fee you would like the program to target for auto replacement of mpool messages. (Default: 1.5 Fil)",
        type=str,
        default=os.environ.get("MAX_FEE", 1.5),
        required=False,
    )
    parser.add_argument(
        "--pending-wait-epochs",
        help="This is the wait time you would like messages to wait in the mpool before putting them into the working queue. The working queue will then begin replacing messages with fee increases. (Default: 20 epochs)",
        type=int,
        default=os.environ.get("PENDING_WAIT_EPOCHS", 20),
        required=False,
    )
    parser.add_argument(
        "--working-wait-epochs",
        help="This is the amount of time you would like messages to wait in-between fee increases after entering the working queue. (Default: 2 epochs)",
        type=int,
        default=os.environ.get("WORKING_WAIT_EPOCHS", 2),
        required=False,
    )
    parser.add_argument(
        "--working-fee-increase",
        help="This is the percentage of the minimum fee you would like to add to the next round fee each round (linear mode) or the amount of the previous round fee you want to add to the next round fee each round (exponential mode). (Default: 25%)",
        type=float,
        default=os.environ.get("WORKING_FEE_INCREASE", 25),
        required=False,
    )
    parser.add_argument(
        "-working-fee-mode",
        help="This is the fee replacement mode see reference on --working-fee-increase for more detail. (Default: linear)",
        type=str,
        default=os.environ.get("WORKING_FEE_MODE", "linear"),
        required=False,
    )
    parser.add_argument(
        "--data-dir",
        help="This program can persist running data over time and allow restarts of the service - if omitted data will not persist beyond each run (Default: /home/user/.mpool_replace/)",
        type=str,
        default=os.environ.get("DATA_DIR", os.path.expanduser("~") + "/.mpool_replace/"),
        required=False,
    )
    parser.add_argument(
        "--log-file",
        help="This can output logs to a directory - if omitted logs will display on stdout only (Default: /var/log/lotus/mpool_replace.log)",
        type=str,
        default=os.environ.get("LOG_FILE", "/var/log/lotus/mpool_replace.log"),
        required=False,
    )
    return parser.parse_args()

# Get options
options = parse_args()

data = {
    "pending_messages": {
        # "cid" : "timestamp"
    },
    "working_messages": {
        # "cid" : {
        #   "start_time" : "timestamp"
        #   "round_time" : "timestamp"
        #   "round_fee" : "in fil"
        #   "last_round_fee": "in fil"
        #   "total_time" : "in seconds"
        # }
    },
    "statistics": {
        "total_pending": 0,
        "total_worked": 0,
        "total_replaced": 0,
        "max_fee": 0,
        "avg_fee": 0
    }
}
dataFile = "mpool_replace.json"

# Store working threads for termination
worker_threads = []

# Configure the logger for both stdout and file output
def get_logger(options: dict) -> logging.Logger:
    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Add output to file
    if options.log_file:
        file_handler = logging.FileHandler(options.log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Add output to stdout
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger

log = get_logger(options)

def processing_pipeline(options: dict):
    
    # Reference data
    global data
    statistics = data["statistics"]
    pending_messages = data["pending_messages"]
    working_messages = data["working_messages"]

    try:       
        # Run the lotus command to get the pending messages
        process = subprocess.Popen(["lotus", "mpool", "pending", "--local", "--cids"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        # Process the output of pending messages
        if stderr:
            log.error(stderr.decode("utf-8"))
        else:
            # Split the output into individual message CIDs
            mpool_cids = [] if not stdout else stdout.decode("utf-8").strip().split("\n")

            # Check if any new messages have been added to the mpool 
            for cid in mpool_cids:
                if cid not in pending_messages and cid not in working_messages:
                    # Init initial tracking stage of message
                    pending_messages[cid] = time.time()
                    log.info(f"Pending message: {cid} is new adding to initial pending message tracker.")
                    statistics["total_pending"] += 1

            # Process pending messages and move them into working messages if they've aged out
            for cid in list(pending_messages):
                
                # If pending message is no longer in mpool remove it
                if cid not in mpool_cids:
                    log.info(f"Pending message: {cid} was processed on chain with no required fee change so removing from tracker.")
                    del pending_messages[cid]

                # Else if it's not in working messages yet see if it's aged out and add it to working messages
                elif cid not in working_messages and time.time() - pending_messages[cid] > (options.pending_wait_epochs * 30): # Todo maybe change this to not use seconds
                    # Add to working messages
                    working_messages[cid] = {
                        "start_time": pending_messages[cid],
                        "round_time": pending_messages[cid],
                        "round_fee": options.min_fee,
                        "last_round_fee": options.min_fee,
                        "current_age": 0
                    }
                    # Remove from pending messages
                    del pending_messages[cid] #
                    log.info(f"Pending message: {cid} has expired initial wait epochs of {options.pending_wait_epochs} and moved into working tracker.")
                    statistics["total_worked"] += 1


            # Process all working messages to increase fees incrementally to be accepted by chain constrained within min and max fees
            for cid in list(working_messages):
                message = working_messages[cid]
                
                # If working message is no longer in mpool remove it and record basic runtime stats
                if cid not in mpool_cids:
                    last_round_fee = message["last_round_fee"]
                    statistics["max_fee"] = last_round_fee if last_round_fee > statistics["max_fee"] else statistics["max_fee"]
                    statistics["avg_fee"] = last_round_fee if statistics["total_replaced"] == 1 else (statistics["avg_fee"] + last_round_fee) / 2
                    del working_messages[cid]
                    log.info(f"Worker message: {cid} was removed from tracking as it was processed on chain with a round fee of {last_round_fee}.")

                # If it is still in the mpool go ahead and replace it with this rounds specifications
                else:
                    round_age = time.time() - message["round_time"]
                    if round_age > (options.working_wait_epochs * 30): # Todo maybe change this to not use seconds
                        
                        # Replace the old message with a new one using this rounds fee
                        executed_fee = message["round_fee"] if message["round_fee"] <= options.max_fee else options.max_fee
                        log.info(f"Worker message: {cid} expired designated epochs without posting to chain and will be replaced with a fee of {executed_fee}.")
                        process = subprocess.Popen(["lotus", "mpool", "replace", "--auto", "--fee-limit", str(executed_fee), cid], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        stdout, stderr = process.communicate()
                        
                        if stderr:
                            
                            error = stderr.decode('utf-8')
                            if "too low GasPremium" in error:
                                new_fee = get_next_round_fee(options, message["round_fee"])
                                log.warning(f"Tried replacing message, but fee difference was to low. Upping fee in place to {new_fee} for next loop without replacing")
                                message["round_fee"] = new_fee
                            else:
                                log.error(f"Error replacing message {cid}: {stderr.decode('utf-8')}")
                        else:
                            # Extract the new CID from the output and strip the prefix
                            new_cid = stdout.decode("utf-8").strip().split(": ")[-1].replace("new message cid: ", "").strip()
                            
                            # Add new message to be tracked and increase the fee for the next round
                            next_round_fee = get_next_round_fee(options, message["round_fee"])
                            total_age_seconds = time.time() - message["start_time"]
                            working_messages[new_cid] = {
                                "start_time": message["start_time"],
                                "round_time": time.time(), 
                                "round_fee": next_round_fee,
                                "last_round_fee": executed_fee,
                                "total_age_seconds" : total_age_seconds
                            }
                            del working_messages[cid]
                            # Log out some information
                            log.info(f"Worker message: {cid} has been replaced with new worker message: {new_cid}..")
                            log.info(f"Stats for msg: {new_cid} [Total age: {round(total_age_seconds)} seconds] [Next round fee: {next_round_fee}]")
                            statistics["total_replaced"] += 1
    except Exception as e:
        log.error(f"Exception thrown: {e}")

# Used to monitor sync status of lotus daemon
def check_sync_status() -> bool:
    try:
        # Check if the chain is in sync
        output = subprocess.check_output(["lotus", "info"])
        output_str = output.decode('utf-8')
        if "sync ok" not in output_str:
            return False
        return True
    except Exception as e:
        log.error(f"Exception thrown: {e}")
        return False

# Calculate the next rounds fee based on mode
def get_next_round_fee(options: dict, current_fee: float) -> float:
    if options.working_fee_mode == "linear":
        return current_fee + get_percent(options.min_fee, options.working_fee_increase)
    elif options.working_fee_mode == "exponential":
        return current_fee + get_percent(current_fee, options.working_fee_increase)
    else:
        log.error("Working fee mode set incorrectly pass either 'linear' or 'exponetial'")
        return current_fee
     
def get_percent(number: float, percentage: float):
    return number * (percentage / 100)

# Runs every epoch based on thread_scheduler
def run_every_epoch(options: dict):
    
    # Reference data
    global data
    statistics = data["statistics"]
    pending_messages = data["pending_messages"]
    working_messages = data["working_messages"]

    try:
        pen = statistics["total_pending"]
        wrk = statistics["total_worked"]
        rep = statistics["total_replaced"]
        avg = statistics["avg_fee"]
        max = statistics["max_fee"]

        # Log current runtime statistics
        log.info(f"Running stats: [Current Pending: {len(pending_messages)}] [Current Working: {len(working_messages)}] [Total Pending: {pen}] [Total Worked: {wrk}] [Total Replacements: {rep}] [Average Fee: {round(avg,9)}] [Max Fee: {round(max,9)}]")

        # Export or save data each epoch
        if options.data_dir:
            export_data(options)

        # Run processing pipeline
        processing_pipeline(options)

        # Schedule next run on
        thread_scheduler(options)
        
    except Exception as e:
        log.error(f"Exception thrown: {e}")

# Thread scheduler to keep program running every epoch on the epoch start
def thread_scheduler(options: dict) -> bool:
    try:

        # Check that the chain is in sync before scheduling a run could from time to time move timing out an epoch or two
        while not check_sync_status():
            log.warning(f"Chain out of sync waiting for 1 epoch")
            time.sleep(30)

        now = datetime.now()
        wait_seconds = 30 - (now.second % 30)
        thread = threading.Timer(wait_seconds, run_every_epoch, args=[options])
        worker_threads.append(thread)
        thread.start()

    except Exception as e:
        log.error(f"Exception thrown: {e}")
        return False
    
    return True

# Program Main
def main() -> None:
    global options

    try:
        # Log starting options
        log.info(f"Initializing with options: {options}")

        # Import historical data
        if options.data_dir:
            import_data(options)

        # Locate next epoch start time
        now = datetime.now()
        wait_seconds = 30 - (now.second % 30)
        
        log.info(f"Scheduling first run in {wait_seconds} seconds to sync runtimes with chain epochs.")

        # Initialize program thread
        thread = threading.Timer(wait_seconds, run_every_epoch, args=[options])
        worker_threads.append(thread)
        thread.start()
    except Exception as e:
        log.error(f"Exception thrown: {e}")
    
    # Main running thread to catch interrupts
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        cleanup_and_exit(None, None)

# Cleanup and exit program
def cleanup_and_exit(signum, frame):
    log.info("Received termination signal, cancelling threads.")
    for thread in worker_threads:
        thread.cancel()
    log.info("Threads terminated exiting.")
    exit(0)

# Data persistence stuffs import will create items missing if not found
def import_data(options: dict):
    global data
    # Make primary data persistence directory
    os.makedirs(options.data_dir, exist_ok=True)
    try:
        with open(f"{options.data_dir}{dataFile}", 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        log.info(f"The file {options.data_dir}{dataFile} does not exist. Creating that now.")
        with open(f"{options.data_dir}{dataFile}", 'w') as f:
            json.dump(data, f, indent=4)

def export_data(options: dict):
    global data
    # Dump current data
    with open(f"{options.data_dir}{dataFile}", 'w') as f:
        json.dump(data, f, indent=4)

# Catch and handle systemd service termination
signal.signal(signal.SIGTERM, cleanup_and_exit)

# Init Program 
if __name__ == "__main__":
    main()
