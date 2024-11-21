#!/usr/bin/env python3
import sys
import socket
import struct
import json
from enum import Enum
import math
import time
import random as rd
import signal
import sys
from loguru import logger

logger.remove()

logger.add(
    sink=sys.stdout,
    format="<green>[{time:HH:mm:ss}]</green> <cyan>{level}</cyan> <bold><yellow>[PROPOSER]</yellow></bold> <level>{message}</level>",
    filter=lambda record: record["extra"].get("role") == "proposer",
)

logger.add(
    sink=sys.stdout,
    format="<green>[{time:HH:mm:ss}]</green> <cyan>{level}</cyan> <bold><blue>[LEARNER]</blue></bold> <level>{message}</level>",
    filter=lambda record: record["extra"].get("role") == "learner",
)

logger.add(
    sink=sys.stdout,
    format="<green>[{time:HH:mm:ss}]</green> <cyan>{level}</cyan> <bold><red>[ACCEPTOR]</red></bold> <level>{message}</level>",
    filter=lambda record: record["extra"].get("role") == "acceptor",
)

logger.add(
    sink=sys.stdout,
    format="<green>[{time:HH:mm:ss}]</green> <cyan>{level}</cyan> <bold><green>[CLIENT]</green></bold> <level>{message}</level>",
    filter=lambda record: record["extra"].get("role") == "client",
)


def log_proposer_debug(message):
    logger.bind(role="proposer").debug(message)


def log_learner_debug(message):
    logger.bind(role="learner").debug(message)


def log_acceptor_debug(message):
    logger.bind(role="acceptor").debug(message)


def log_client_debug(message):
    logger.bind(role="client").info(message)


def log_proposer_info(message):
    logger.bind(role="proposer").info(message)


def log_learner_info(message):
    logger.bind(role="learner").info(message)


def log_acceptor_info(message):
    logger.bind(role="acceptor").info(message)


def log_client_info(message):
    logger.bind(role="client").info(message)


class MessageType(Enum):
    PREPARE = "PHASE_1A"
    PROMISE = "PHASE_1B"
    ACCEPT_REQUEST = "PHASE_2A"
    DECIDE = "PHASE_3"
    CLIENT_VALUE = "CLIENT_VALUE"
    CATCHUP_REQUEST = "CATCHUP_REQUEST"
    CATCHUP_VALUES = "CATCHUP_VALUE"


def encode_json_msg(type, **kwargs):
    return json.dumps({"type": type.value, **kwargs}).encode()


def decode_json_msg(msg):
    parsed_msg = json.loads(msg.decode())
    parsed_msg["type"] = MessageType(parsed_msg["type"])
    if "seq" in parsed_msg:
        parsed_msg["seq"] = tuple(parsed_msg["seq"])
    if "c_rnd" in parsed_msg:
        parsed_msg["c_rnd"] = tuple(parsed_msg["c_rnd"])
    if "rnd" in parsed_msg:
        parsed_msg["rnd"] = tuple(parsed_msg["rnd"])
    if "v_rnd" in parsed_msg and parsed_msg["v_rnd"] is not None:
        parsed_msg["v_rnd"] = tuple(parsed_msg["v_rnd"])
    return parsed_msg


def mcast_receiver(hostport):
    """create a multicast socket listening to the address"""
    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    recv_sock.bind(hostport)

    mcast_group = struct.pack("4sl", socket.inet_aton(hostport[0]), socket.INADDR_ANY)
    recv_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mcast_group)
    return recv_sock


def mcast_sender():
    """create a udp socket"""
    send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    return send_sock


def parse_cfg(cfgpath):
    cfg = {}
    with open(cfgpath, "r") as cfgfile:
        for line in cfgfile:
            (role, host, port) = line.split()
            cfg[role] = (host, int(port))
    return cfg


# ----------------------------------------------------


def acceptor(config, id):
    log_acceptor_info(f"[{id}] started")
    states = {}
    decision = {}
    r = mcast_receiver(config["acceptors"])
    s = mcast_sender()
    while True:
        msg = decode_json_msg(r.recv(2**16))
        if msg["type"] == MessageType.PREPARE:
            seq = msg["seq"]
            if seq not in states:
                states[seq] = {"rnd": (0, id), "v_rnd": None, "v_val": None}
            if msg["c_rnd"] > states[seq]["rnd"]:
                states[seq]["rnd"] = msg["c_rnd"]
                promise_msg = encode_json_msg(
                    MessageType.PROMISE,
                    seq=seq,
                    rnd=states[seq]["rnd"],
                    v_rnd=states[seq]["v_rnd"],
                    v_val=states[seq]["v_val"],
                )
                log_acceptor_debug(
                    f"[{id}] Received: {seq} of Type {MessageType.PREPARE} in Rnd: {states[seq]['rnd']}"
                )
                s.sendto(promise_msg, config["proposers"])
        elif msg["type"] == MessageType.ACCEPT_REQUEST:
            seq = msg["seq"]
            if msg["c_rnd"] >= states[seq]["rnd"]:
                states[seq]["v_rnd"] = msg["c_rnd"]
                states[seq]["v_val"] = msg["c_val"]
                accepted_msg = encode_json_msg(
                    MessageType.DECIDE,
                    seq=seq,
                    v_rnd=states[seq]["v_rnd"],
                    v_val=states[seq]["v_val"],
                )
                log_acceptor_debug(
                    f"[{id}] Received {seq} of Type {MessageType.ACCEPT_REQUEST} in v_rnd: {states[seq]['v_rnd']} with v_val: {states[seq]['v_val']}"
                )
                if seq not in decision:
                    decision[seq] = msg["c_val"]
                # NOTE: Used optimization of Paxos, where acceptors send decision directly to learners
                s.sendto(accepted_msg, config["learners"])
                s.sendto(accepted_msg, config["proposers"])
        elif msg["type"] == MessageType.CATCHUP_REQUEST:
            catchup_seq = []
            for m_seq in msg["missing_seq"]:
                m_seq = tuple(m_seq)
                catchup_seq.append([m_seq, decision[m_seq]])
                catchup_msg = encode_json_msg(
                    MessageType.CATCHUP_VALUES, catchup_seq=catchup_seq
                )
                log_acceptor_debug(f"[{id}] send {catchup_seq} for {MessageType.CATCHUP_VALUES}")
                s.sendto(catchup_msg, config["learners"])


def proposer(config, id):
    log_proposer_info(f"[{id}] started")
    r = mcast_receiver(config["proposers"])
    r.setblocking(False)
    s = mcast_sender()
    seq = (0, 0)
    c_rnd_cnt = (0, id)
    c_rnd = {}
    c_val = {}
    promises = {}
    pending = {}
    learned = []

    while True:
        try:
            msg = decode_json_msg(r.recv(2**16))
            if msg["type"] == MessageType.CLIENT_VALUE:
                seq = (msg["prop_id"], msg["client_id"])
                c_rnd_cnt = (c_rnd_cnt[0] + 1, c_rnd_cnt[1])
                c_rnd[seq] = c_rnd_cnt
                c_val[seq] = msg["value"]
                prepare_msg = encode_json_msg(
                    MessageType.PREPARE, c_rnd=c_rnd[seq], seq=seq
                )
                log_proposer_debug(
                    f"[{id}] Received: {MessageType.CLIENT_VALUE} c_rnd: {c_rnd[seq]} seq: {seq}",
                )
                pending[seq] = time.time()
                s.sendto(prepare_msg, config["acceptors"])
            elif msg["type"] == MessageType.PROMISE and msg["rnd"] == c_rnd[msg["seq"]]:
                seq = msg["seq"]
                if seq not in promises:
                    promises[seq] = []

                promises[seq].append(msg)

                # TODO: Number of acceptors hardocoded -> pass number of acceptors with config?
                n_promises = len([p for p in promises[seq] if msg["rnd"] == p["rnd"]])
                if n_promises >= math.ceil(3 / 2):
                    k = max(
                        (p["v_rnd"] for p in promises[seq] if p["v_rnd"]), default=None
                    )
                    if k:
                        c_val[seq] = next(
                            p["v_val"] for p in promises[seq] if p["v_rnd"] == k
                        )
                    log_proposer_debug(
                        f"[{id}] seq {seq} Received: {MessageType.PROMISE} c_rnd: {c_rnd[seq]} c_val: {c_val[seq]}"
                    )
                    accept_msg = encode_json_msg(
                        MessageType.ACCEPT_REQUEST,
                        seq=seq,
                        c_rnd=c_rnd[seq],
                        c_val=c_val[seq],
                    )
                    s.sendto(accept_msg, config["acceptors"])
            elif msg["type"] == MessageType.DECIDE:
                seq = msg["seq"]
                if seq not in learned:
                    learned.append(seq)

        except BlockingIOError:
            pass

        # TODO: Ensure safety by proposing again after timeout.
        # NOTE: Client sends proposed value to both proposers.
        for seq in list(pending.keys()):
            if seq in learned:
                pending.pop(seq)
            else:
                elapsed_time = time.time() - pending[seq]
                if elapsed_time > rd.randint(1, 3):
                    c_rnd_cnt = (c_rnd_cnt[0] + 1, c_rnd_cnt[1])
                    c_rnd[seq] = c_rnd_cnt
                    prepare_msg = encode_json_msg(
                        MessageType.PREPARE, c_rnd=c_rnd[seq], seq=seq
                    )
                    log_proposer_debug(
                        f"[{id}] Received: {MessageType.CLIENT_VALUE} c_rnd: {c_rnd[seq]} seq {seq}"
                    )
                    pending[seq] = time.time()
                    s.sendto(prepare_msg, config["acceptors"])


def learner(config, id):
    r = mcast_receiver(config["learners"])
    r.setblocking(False)
    s = mcast_sender()
    learned = {}
    last_seq = {}
    requested = []

    # TODO: Change: Not so nice workaround, but it works
    def print_learned():
        for key in sorted(learned, key=lambda x: (x[1], x[0])):
            print(learned[key])
        sys.stdout.flush()

    def handle_exit_signal(signum, frame):
        print_learned()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_exit_signal)
    signal.signal(signal.SIGTERM, handle_exit_signal)

    start_time = float("inf")
    timeout = 3
    while True:
        msg = {}
        try:
            msg = decode_json_msg(r.recv(2**16))
            if msg["type"] == MessageType.DECIDE:
                seq = msg["seq"]
                learned[seq] = msg["v_val"]
                if seq[1] not in last_seq:
                    last_seq[seq[1]] = -1
                last_seq[seq[1]] = max(last_seq[seq[1]], seq[0])
                start_time = time.time()
            elif (
                msg["type"] == MessageType.CATCHUP_VALUES
            ):  # TODO: Always multicasted to all in group -> perhaps use id
                for el in msg["catchup_seq"]:
                    seq = tuple(el[0])
                    learned[seq] = el[1]
        except BlockingIOError:
            pass
        if time.time() - start_time > timeout:
            client_ids = max([s[1] for s in learned])
            missing_seq = []
            for cl in range(1, client_ids + 1):
                if cl not in last_seq:
                    last_seq[cl] = max(
                        [last_seq[x] for x in last_seq]
                    )  # Assume everbody sends the same number of client requests
                for seq_m in range(1, last_seq[cl] + 1):
                    if (seq_m, cl) not in list(learned) + requested:
                        missing_seq.append((seq_m, cl))
            if len(missing_seq) != 0:
                requested += missing_seq

                request_msg = encode_json_msg(
                    MessageType.CATCHUP_REQUEST, missing_seq=missing_seq
                )
                s.sendto(request_msg, config["acceptors"])


def client(config, id):
    log_client_info(f"[{id}] started.")
    s = mcast_sender()
    prop_id = 0
    for value in sys.stdin:
        value = value.strip()
        prop_id += 1
        log_client_debug(
            "%s sending %s to proposers with prop_id %s" % (id, value, prop_id)
        )
        client_msg = encode_json_msg(
            MessageType.CLIENT_VALUE, value=value, client_id=id, prop_id=prop_id
        )
        s.sendto(client_msg, config["proposers"])
    log_client_info(f"[{id}] done.")


def unknown(config, id):
    print(f"Role not found for id: [{id}] and config: {config}!")


if __name__ == "__main__":
    cfgpath = sys.argv[1]
    config = parse_cfg(cfgpath)
    role = sys.argv[2]
    id = int(sys.argv[3])
    rolefunc = unknown
    if role == "acceptor":
        rolefunc = acceptor
    elif role == "proposer":
        rolefunc = proposer
    elif role == "learner":
        rolefunc = learner
    elif role == "client":
        rolefunc = client
    rolefunc(config, id)
