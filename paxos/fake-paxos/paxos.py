#!/usr/bin/env python3
import sys
import socket
import struct
import json
from enum import Enum
import math


class MessageType(Enum):
    PREPARE = "PHASE_1A"
    PROMISE = "PHASE_1B"
    ACCEPT_REQUEST = "PHASE_2A"
    DECIDE = "PHASE_3"
    CLIENT_VALUE = "CLIENT_VALUE"


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
                s.sendto(accepted_msg, config["learners"])


def proposer(config, id):
    print("-> proposer", id)
    r = mcast_receiver(config["proposers"])
    s = mcast_sender()
    seq = (0, 0)
    c_rnd_cnt = (0, id)
    c_rnd = {}
    c_val = {}
    promises = {}

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
            s.sendto(prepare_msg, config["acceptors"])
        # TODO: CHECK
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
            # if len(promises) > len(config["acceptors"]) // 2:
            if len(promises[seq]) > math.ceil(3 / 2):
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


def learner(config, id):
    r = mcast_receiver(config["learners"])
    AB_val = {}
    while True:
        msg = decode_json_msg(r.recv(2**16))
        if msg["type"] == MessageType.DECIDE:
            AB_val[msg["seq"]] = msg["v_val"]
            print(
                f"Learner {id}: Learned about {msg['v_val']} in rnd {msg['v_rnd']} with seq: {msg['seq']}"
            )
        sys.stdout.flush()


def client(config, id):
    print("-> client ", id)
    s = mcast_sender()
    prop_id = 0
    for value in sys.stdin:
        value = value.strip()
        prop_id += 1
        print("client %s: sending %s to proposers with prop_id %s" % (id, value, prop_id))
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
