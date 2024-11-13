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
    # TODO: Check for correct type
    parsed_msg["type"] = MessageType(parsed_msg["type"])
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
    # state = {}
    r = mcast_receiver(config["acceptors"])
    s = mcast_sender()
    rnd = (0, id)
    v_rnd = None
    v_val = None
    while True:
        msg = decode_json_msg(r.recv(2**16))
        if msg["type"] == MessageType.PREPARE:
            if msg["c_rnd"] > rnd:
                rnd = msg["c_rnd"]
                promise_msg = encode_json_msg(
                    MessageType.PROMISE, rnd=rnd, v_rnd=v_rnd, v_val=v_val
                )
                print("-> acceptor", id, " Received:", MessageType.PREPARE, "Rnd:", rnd)
                s.sendto(promise_msg, config["proposers"])
        elif msg["type"] == MessageType.ACCEPT_REQUEST:
            if msg["c_rnd"] >= rnd:
                v_rnd = msg["c_rnd"]
                v_val = msg["c_val"]
                accepted_msg = encode_json_msg(
                    MessageType.DECIDE, v_rnd=v_rnd, v_val=v_val
                )
                print("-> acceptor", id, " Received: ", MessageType.ACCEPT_REQUEST, " v_rnd: ", v_rnd, " v_val: ", v_val)
                s.sendto(accepted_msg, config["learners"])


def proposer(config, id):
    print("-> proposer", id)
    r = mcast_receiver(config["proposers"])
    s = mcast_sender()
    c_rnd = (0, id)
    c_val = None
    promises = []

    while True:
        msg = decode_json_msg(r.recv(2**16))
        if msg["type"] == MessageType.CLIENT_VALUE:
            c_val = msg["value"]
            c_rnd = (c_rnd[0] + 1, c_rnd[1])
            prepare_msg = encode_json_msg(MessageType.PREPARE, c_rnd=c_rnd)
            print("-> proposer", id, " Received: ", MessageType.CLIENT_VALUE, "c_rnd: ", c_rnd)
            s.sendto(prepare_msg, config["acceptors"])
        elif msg["type"] == MessageType.PROMISE and msg["rnd"] == c_rnd:
            promises.append(msg)
            print("-> proposer", id, "promises count:", len(promises), "for c_rnd", c_rnd)
            # if len(promises) > len(config["acceptors"]) // 2:
            if len(promises) > math.ceil(3 / 2):
                k = max((p["v_rnd"] for p in promises if p["v_rnd"]), default=None)
                if k:
                    c_val = next(p["v_val"] for p in promises if p["v_rnd"] == k)
                print("-> proposer", id, " Received:", MessageType.PROMISE, "c_rnd:", c_rnd, "c_val:", c_val)
                accept_msg = encode_json_msg(
                    MessageType.ACCEPT_REQUEST, c_rnd=c_rnd, c_val=c_val
                )
                s.sendto(accept_msg, config["acceptors"])


def learner(config, id):
    r = mcast_receiver(config["learners"])
    while True:
        msg = decode_json_msg(r.recv(2**16))
        if msg["type"] == MessageType.DECIDE:
            print(f"Learner {id}: Learned about {msg}")
        sys.stdout.flush()


def client(config, id):
    print("-> client ", id)
    s = mcast_sender()
    for value in sys.stdin:
        value = value.strip()
        print("client: sending %s to proposers" % (value))
        client_msg = encode_json_msg(
            MessageType.CLIENT_VALUE, value=value, client_id=id
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
