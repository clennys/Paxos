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
import atexit
import sys


class MessageType(Enum):
    PREPARE = "PHASE_1A"
    PROMISE = "PHASE_1B"
    ACCEPT_REQUEST = "PHASE_2A"
    DECIDE = "PHASE_3"
    CLIENT_VALUE = "CLIENT_VALUE"
    # CATCHUP_REQUEST = "CATCHUP_REQUEST"
    # CATCHUP_VALUE = "CATCHUP_VALUE"


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
    print("-> acceptor", id)
    states = {}
    r = mcast_receiver(config["acceptors"])
    s = mcast_sender()
    while True:
        msg = decode_json_msg(r.recv(2**16))
        seq = msg["seq"]
        if msg["type"] == MessageType.PREPARE:
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
                print(
                    "-> acceptor",
                    id,
                    " Received:",
                    "seq",
                    seq,
                    MessageType.PREPARE,
                    "Rnd:",
                    states[seq]["rnd"],
                )
                s.sendto(promise_msg, config["proposers"])
        elif msg["type"] == MessageType.ACCEPT_REQUEST:
            if msg["c_rnd"] >= states[seq]["rnd"]:
                states[seq]["v_rnd"] = msg["c_rnd"]
                states[seq]["v_val"] = msg["c_val"]
                accepted_msg = encode_json_msg(
                    MessageType.DECIDE,
                    seq=seq,
                    v_rnd=states[seq]["v_rnd"],
                    v_val=states[seq]["v_val"],
                )
                print(
                    "-> acceptor",
                    id,
                    "seq",
                    seq,
                    "Received:",
                    MessageType.ACCEPT_REQUEST,
                    "v_rnd:",
                    states[seq]["v_rnd"],
                    " v_val:",
                    states[seq]["v_val"],
                )
                # NOTE: Used optimization of Paxos, where acceptors send decision directly to learners
                s.sendto(accepted_msg, config["learners"])
                s.sendto(accepted_msg, config["proposers"])


def proposer(config, id):
    print("-> proposer", id)
    r = mcast_receiver(config["proposers"])
    s = mcast_sender()
    seq = (0, 0)
    c_rnd_cnt = (0, id)
    c_rnd = {}
    c_val = {}
    promises = {}
    pending = {}
    learned = []

    while True:
        msg = decode_json_msg(r.recv(2**16))
        if msg["type"] == MessageType.CLIENT_VALUE:
            seq = (msg["prop_id"], msg["client_id"])
            c_rnd_cnt = (c_rnd_cnt[0] + 1, c_rnd_cnt[1])
            c_rnd[seq] = c_rnd_cnt
            c_val[seq] = msg["value"]
            prepare_msg = encode_json_msg(
                MessageType.PREPARE, c_rnd=c_rnd[seq], seq=seq
            )
            print(
                "-> proposer",
                id,
                " Received: ",
                MessageType.CLIENT_VALUE,
                "c_rnd: ",
                c_rnd[seq],
                "seq,",
                seq,
            )
            pending[seq] = time.time()
            s.sendto(prepare_msg, config["acceptors"])
        elif msg["type"] == MessageType.PROMISE and msg["rnd"] == c_rnd[msg["seq"]]:
            seq = msg["seq"]
            if seq not in promises:
                promises[seq] = []

            promises[seq].append(msg)

            print(
                "-> proposer",
                id,
                "seq",
                seq,
                "promises count:",
                len(promises[seq]),
                "for c_rnd",
                c_rnd[seq],
            )
            # TODO: Number of acceptors hardocoded -> pass number of acceptors with config?
            # if len(promises) > len(config["acceptors"]) // 2:
            if len(promises[seq]) >= math.ceil(3 / 2):
                k = max((p["v_rnd"] for p in promises[seq] if p["v_rnd"]), default=None)
                if k:
                    c_val[seq] = next(
                        p["v_val"] for p in promises[seq] if p["v_rnd"] == k
                    )
                print(
                    "-> proposer",
                    id,
                    "seq",
                    seq,
                    " Received:",
                    MessageType.PROMISE,
                    "c_rnd:",
                    c_rnd[seq],
                    "c_val:",
                    c_val[seq],
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

        # TODO: Ensure safety by proposing again after timeout.
        # NOTE: Client sends proposed value to both proposers.
        for seq in list(pending.keys()):
            if seq in learned:
                pending.pop(seq)
            else:
                elapsed_time = time.time() - pending[seq]
                if elapsed_time > rd.randint(
                    5, 10
                ):  # Try again after a random time between 5 - 10 seconds
                    c_rnd_cnt = (c_rnd_cnt[0] + 1, c_rnd_cnt[1])
                    c_rnd[seq] = c_rnd_cnt
                    prepare_msg = encode_json_msg(
                        MessageType.PREPARE, c_rnd=c_rnd[seq], seq=seq
                    )
                    print(
                        "-> proposer",
                        id,
                        " Received: ",
                        MessageType.CLIENT_VALUE,
                        "c_rnd: ",
                        c_rnd[seq],
                        "seq,",
                        seq,
                    )
                    pending[seq] = time.time()
                    s.sendto(prepare_msg, config["acceptors"])


def learner(config, id):
    r = mcast_receiver(config["learners"])
    # s = mcast_sender()
    learned = {}
    # last_seq = -1

    # TODO: Change: Not so nice workaround, but it works
    def print_learned():
        for key in sorted(learned, key=lambda x: (x[1], x[0])):
            print(learned[key])
        sys.stdout.flush()

    def handle_exit_signal(signum, frame):
        print_learned()
        sys.exit(0)

    atexit.register(print_learned)
    signal.signal(signal.SIGINT, handle_exit_signal)
    signal.signal(signal.SIGTERM, handle_exit_signal) 

    while True:
        msg = decode_json_msg(r.recv(2**16))
        if msg["type"] == MessageType.DECIDE:
            seq = msg["seq"]
            learned[seq] = msg["v_val"]
            # print(msg["v_val"])
            # sys.stdout.flush()

        # TODO: Unfinished learner catchup mechanism -> catchup from other learners or acceptors?
        #     last_seq = max(last_seq, seq[0])
        #     missing_seq = [s for s in range(last_seq + 1) if (s, id) not in learned]
        #     for m_seq in missing_seq:
        #         request_msg = encode_json_msg(
        #         MessageType.CATCHUP_REQUEST, seq=(m_seq, id)
        #     )
        #         s.sendto(request_msg, config["learners"])
        # elif msg["type"] == MessageType.CATCHUP_VALUE:
        #     seq = msg["seq"]
        #     learned[seq] = msg["v_val"]

        # print(
        #     f"Learner {id}: Learned about {msg['v_val']} in rnd {msg['v_rnd']} with seq: {msg['seq']}"
        # )


def client(config, id):
    print("-> client ", id)
    s = mcast_sender()
    prop_id = 0
    for value in sys.stdin:
        value = value.strip()
        prop_id += 1
        print(
            "client %s: sending %s to proposers with prop_id %s" % (id, value, prop_id)
        )
        client_msg = encode_json_msg(
            MessageType.CLIENT_VALUE, value=value, client_id=id, prop_id=prop_id
        )
        s.sendto(client_msg, config["proposers"])
    print("client done.")


def unknown(config, id):
    print(f"Role not found for id: {id} and config: {config}!")


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
