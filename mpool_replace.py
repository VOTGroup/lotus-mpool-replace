#!/usr/bin/python3

from dataclasses import replace
import glob
from math import log
import subprocess
import time
import logging

MIN_MSG_FEE = 0.5
MAX_MSG_FEE = 1.5
INIT_WAIT_EPOCHS = 20 # How many epochs to initially wait before starting fee increase cycle
ROUND_WAIT_EPOCHS = 5 # How many epochs to wait to replace fees each round attempting to get message through after init wait has passed
ROUND_VARIABLE_INCREASE = 1.20 # 20 percent over previous
RUNTIME_INTEVERAL = 30 # recommend leaving untouched as 30 seconds is one epoch
LOG_FOLDER='/var/log/lotus/mpool_replace.log' # for logging

# Store the pending messages and their settings in running memory
pending_messages = {
    # "cid" : "timestamp"
}
working_messages = {
    # "cid" : {
    #   "start_time" : "timestamp"
    #   "round_time" : "timestamp"
    #   "round_fee" : "in fil"
    #   "last_round_fee": "in fil"
    #   "total_time" : "in seconds"
    # }
}
replaced_messages = 0
max_fee_used = 0
average_fee_used = 0

# Configure the logging settings
logging.basicConfig(filename=LOG_FOLDER, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Used to monitor sync status of lotus daemon
def check_sync_status():
    try:
        # Check if the chain is in sync
        output = subprocess.check_output(["lotus", "info"])
        output_str = output.decode('utf-8')
        if "sync ok" not in output_str:
            return False
        return True
    except Exception as e:
        logging.error(f"Failed to check chain sync status: {e}")
        return False

def run_processing() -> None:
    global pending_messages
    global working_messages
    global replaced_messages
    global max_fee_used
    global average_fee_used

    # Check if the chain is in sync and wait indefinitely until it is
    while not check_sync_status():
        logging.warning(f"Chain out of sync waiting for 10 seconds..")
        time.sleep(10)

    # Run the lotus command to get the pending messages
    process = subprocess.Popen(["lotus", "mpool", "pending", "--local", "--cids"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    # Process the output of pending messages
    if stderr:
        logging.error(stderr.decode("utf-8"))
    else:
        # Split the output into individual message CIDs
        mpool_cids = [] if not stdout else stdout.decode("utf-8").strip().split("\n")

        # Check if any new messages have been added to the mpool 
        for cid in mpool_cids:
            if cid not in pending_messages and cid not in working_messages:
                # Init initial tracking stage of message
                pending_messages[cid] = time.time()
                logging.info(f"Pending message: {cid} is new adding to initial pending message tracker..")

        # Process pending messages and move them into working messages if they've aged out
        for cid in list(pending_messages):
            
            # If pending message is no longer in mpool remove it
            if cid not in mpool_cids:
                logging.info(f"Pending message: {cid} was processed on chain with no required fee change so removing from tracker..")
                del pending_messages[cid]

            # Else if it's not in working messages yet see if it's aged out and add it to working messages
            elif cid not in working_messages and time.time() - pending_messages[cid] > (INIT_WAIT_EPOCHS * 30):
                # Add to working messages
                working_messages[cid] = {
                    "start_time": pending_messages[cid],
                    "round_time": pending_messages[cid],
                    "round_fee": MIN_MSG_FEE,
                    "last_round_fee": MIN_MSG_FEE,
                    "current_age": 0
                }
                # Remove from pending messages
                del pending_messages[cid] #
                logging.info(f"Pending message: {cid} has expired initial wait epochs of {INIT_WAIT_EPOCHS} and moved into working tracker..")


        # Process all working messages to increase fees incrementally to be accepted by chain constrained within min and max fees
        for cid in list(working_messages):
            message = working_messages[cid]
            
            # If working message is no longer in mpool remove it and record basic runtime stats
            if cid not in mpool_cids:
                last_round_fee = message["last_round_fee"]
                max_fee_used = last_round_fee if last_round_fee > max_fee_used else max_fee_used
                average_fee_used = last_round_fee if replaced_messages == 1 else (average_fee_used + last_round_fee) / 2
                del working_messages[cid]
                logging.info(f"Worker message: {cid} was removed from tracking as it was processed on chain with a round fee of {last_round_fee}..")

            # If it is still in the mpool go ahead and replace it with this rounds specifications
            else:
                round_age = time.time() - message["round_time"]
                if round_age > (ROUND_WAIT_EPOCHS * 30):
                    
                    # Replace the old message with a new one using this rounds fee
                    executed_fee = message["round_fee"] if message["round_fee"] <= MAX_MSG_FEE else MAX_MSG_FEE
                    logging.info(f"Worker message: {cid} expired round epochs and will be replaced with {executed_fee} fee..")
                    process = subprocess.Popen(["lotus", "mpool", "replace", "--auto", "--fee-limit", str(executed_fee), cid], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    stdout, stderr = process.communicate()
                    
                    if stderr:
                        logging.error(f"Error replacing message {cid}: {stderr.decode('utf-8')}")
                    else:                        
                        # Extract the new CID from the output and strip the prefix
                        replaced_messages += 1
                        new_cid = stdout.decode("utf-8").strip().split(": ")[-1].replace("new message cid: ", "").strip()
                        # Add new message to be tracked and increase the fee for the next round
                        next_round_fee = message["round_fee"] * ROUND_VARIABLE_INCREASE
                        total_age_seconds = time.time() - message["start_time"]
                        working_messages[new_cid] = {
                            "start_time": message["start_time"],
                            "round_time": time.time(), 
                            "round_fee": next_round_fee,
                            "last_round_fee": executed_fee,
                            "total_age_seconds" : total_age_seconds
                        }
                        logging.info(f"Worker message: {cid} has been replaced with new worker message: {new_cid}..")
                        logging.info(f"Worker message: {cid} stats [Total age: {total_age_seconds} seconds] [Next round fee: {next_round_fee}]")
                        del working_messages[cid]

def main() -> None:
    # Init primary loop
    while True:
        # Log current runtime stats
        logging.info(f"Running stats: [Replaced: {replaced_messages}] [Average Fee: {average_fee_used}] [Max Fee: {max_fee_used}] [Pending Msgs: {len(pending_messages)}] [Working Msgs: {len(working_messages)}]")
        run_processing()
        time.sleep(RUNTIME_INTEVERAL)
   
if __name__ == "__main__":
    main()