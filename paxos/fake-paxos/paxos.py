#!/usr/bin/env python3
import sys
import socket
import struct
import json
from enum import Enum
import math
import time
import random as rd
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
    CATCHUP_VALUES = "CATCHUP_VALUES"


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


def mcast_receiver(hostport, buffer_size=65536):
    """create a multicast socket listening to the address"""
    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    recv_sock.bind(hostport)

    # recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buffer_size)

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
        # log_acceptor_debug(f"{[id]} {msg}")
        if msg["type"] == MessageType.PREPARE:
            inst = msg["inst"]
            if inst not in states:
                states[inst] = {
                    "rnd": (0, id),
                    "v_rnd": None,
                    "v_val": None,
                    "seq": None,
                }
            if msg["c_rnd"] > states[inst]["rnd"]:
                states[inst]["rnd"] = msg["c_rnd"]
                states[inst]["seq"] = msg["seq"]
                promise_msg = encode_json_msg(
                    MessageType.PROMISE,
                    inst=inst,
                    seq=states[inst]["seq"],
                    rnd=states[inst]["rnd"],
                    v_rnd=states[inst]["v_rnd"],
                    v_val=states[inst]["v_val"],
                )
                log_acceptor_debug(
                    f"[{id}]({inst}) Received: {states[inst]['seq']} of Type {MessageType.PREPARE} in Rnd: {states[inst]['rnd']}"
                )
                s.sendto(promise_msg, config["proposers"])
        elif msg["type"] == MessageType.ACCEPT_REQUEST:
            inst = msg["inst"]
            if msg["c_rnd"] >= states[inst]["rnd"]:
                states[inst]["v_rnd"] = msg["c_rnd"]
                states[inst]["v_val"] = msg["c_val"]
                accepted_msg = encode_json_msg(
                    MessageType.DECIDE,
                    inst=inst,
                    seq=states[inst]["seq"],
                    v_rnd=states[inst]["v_rnd"],
                    v_val=states[inst]["v_val"],
                )
                log_acceptor_debug(
                    f"[{id}]({inst}) Received {states[inst]['seq']} of Type {MessageType.ACCEPT_REQUEST} in v_rnd: {states[inst]['v_rnd']} with v_val: {states[inst]['v_val']}"
                )
                if inst not in decision:
                    decision[inst] = (msg["seq"], msg["c_val"])
                # NOTE: Used optimization of Paxos, where acceptors send decision directly to learners
                s.sendto(accepted_msg, config["learners"])
                s.sendto(accepted_msg, config["proposers"])
        elif msg["type"] == MessageType.CATCHUP_REQUEST:
            catchup_inst = []
            for m_inst in msg["missing_inst"]:
                catchup_inst.append([m_inst, decision[m_inst][0], decision[m_inst][1]])
                catchup_msg = encode_json_msg(
                    MessageType.CATCHUP_VALUES, catchup_inst=catchup_inst
                )
                log_acceptor_debug(
                    f"[{id}] send {catchup_inst} for {MessageType.CATCHUP_VALUES}"
                )
                s.sendto(catchup_msg, config["learners"])


def proposer(config, id):
    log_proposer_info(f"[{id}] started")
    r = mcast_receiver(config["proposers"])
    r.setblocking(False)
    s = mcast_sender()
    seq = (0, 0)
    inst = -1
    c_rnd_cnt = (0, id)
    c_rnd = {}
    c_val = {}
    promises = {}
    pending = {}
    inst_learned = []
    seq_learned = []
    open_inst = []

    while True:
        try:
            msg = decode_json_msg(r.recv(2**16))
            log_proposer_debug(f"[{id}] {msg}")
            if msg["type"] == MessageType.CLIENT_VALUE:
                inst += 1
                open_inst.append(inst)
                seq = (msg["prop_id"], msg["client_id"])
                c_rnd_cnt = (c_rnd_cnt[0] + 1, c_rnd_cnt[1])
                c_rnd[inst] = c_rnd_cnt
                c_val[inst] = msg["value"]
                prepare_msg = encode_json_msg(
                    MessageType.PREPARE, c_rnd=c_rnd[inst], seq=seq, inst=inst
                )
                log_proposer_debug(
                    f"[{id}]({inst}) Received: {MessageType.CLIENT_VALUE} c_rnd: {c_rnd[inst]} seq: {seq}",
                )
                pending[seq] = time.time()
                s.sendto(prepare_msg, config["acceptors"])
            elif (
                msg["type"] == MessageType.PROMISE and msg["rnd"] == c_rnd[msg["inst"]]
            ):
                m_inst = msg["inst"]
                m_seq = msg["seq"]
                if m_inst not in promises:
                    promises[m_inst] = []
                promises[m_inst].append(msg)

                n_promises = len(
                    [p for p in promises[m_inst] if msg["rnd"] == p["rnd"]]
                )
                # TODO: Number of acceptors hardocoded -> pass number of acceptors with config?
                if n_promises >= math.ceil(3 / 2):
                    k = max(
                        (p["v_rnd"] for p in promises[m_inst] if p["v_rnd"]),
                        default=None,
                    )
                    if k:
                        c_val[m_inst] = next(
                            p["v_val"] for p in promises[m_inst] if p["v_rnd"] == k
                        )
                    log_proposer_debug(
                        f"[{id}]({m_inst}) Received: {MessageType.PREPARE} c_rnd: {c_rnd[m_inst]} c_val: {c_val[m_inst]} seq {m_seq}"
                    )
                    accept_msg = encode_json_msg(
                        MessageType.ACCEPT_REQUEST,
                        inst=m_inst,
                        seq=m_seq,
                        c_rnd=c_rnd[m_inst],
                        c_val=c_val[m_inst],
                    )
                    s.sendto(accept_msg, config["acceptors"])
            elif msg["type"] == MessageType.DECIDE:
                m_inst = msg["inst"]
                m_seq = msg["seq"]
                if m_inst not in inst_learned:
                    inst_learned.append(m_inst)
                    open_inst.remove(m_inst)
                    pending.pop(m_seq)
                    if m_seq not in seq_learned:
                        seq_learned.append(m_seq)
                log_proposer_debug(
                    f"[{id}]({m_inst}) Received: {MessageType.DECIDE} val: {msg["v_val"]} seq: {m_seq}"
                )



        except BlockingIOError:
            pass

        # TODO: Ensure safety by proposing again after timeout.
        # NOTE: Client sends proposed value to both proposers.
        for p_seq in list(pending.keys()):
            tmp_open_inst = open_inst[:]
            if p_seq not in seq_learned:
                elapsed_time = time.time() - pending[p_seq]
                if elapsed_time > rd.randint(1, 3) * 1000:
                    if len(tmp_open_inst) > 0:
                        re_inst = tmp_open_inst.pop(0)
                    else:
                        inst += 1
                        re_inst = inst
                    c_rnd_cnt = (c_rnd_cnt[0] + 1, c_rnd_cnt[1])
                    c_rnd[re_inst] = c_rnd_cnt
                    prepare_msg = encode_json_msg(
                        MessageType.PREPARE,
                        c_rnd=c_rnd[re_inst],
                        inst=re_inst,
                        seq=p_seq,
                    )
                    log_proposer_debug(
                        f"[{id}]({re_inst}) Repropose: {MessageType.CLIENT_VALUE} c_rnd: {c_rnd[re_inst]} seq {p_seq}"
                    )
                    pending[p_seq] = time.time()
                    s.sendto(prepare_msg, config["acceptors"])


def learner(config, id):
    r = mcast_receiver(config["learners"])
    r.setblocking(False)
    s = mcast_sender()
    learned = {}
    last_printed = -1
    start_time = float("inf")
    pending = {}

    # TODO: Change: Not so nice workaround, but it works
    # Change Sequence Number to instance number use sequence number to stop dublication
    # Check for quorum of acceptor messages

    timeout = 499999
    while True:
        msg = {}
        try:
            msg = decode_json_msg(r.recv(2**16))
            if msg["type"] == MessageType.DECIDE:
                inst = msg["inst"]
                if inst not in learned:
                    learned[inst] = msg["v_val"]
                    pending[inst] = msg["v_val"]
                    start_time = time.time()
                # log_learner_debug(
                #     f"[{id}]({inst}) Received: {MessageType.DECIDE} val: {msg["v_val"]} seq: {msg["seq"]}"
                # )

            elif msg["type"] == MessageType.CATCHUP_VALUES:
                for el in msg["catchup_inst"]:
                    pending[el[0]] = el[2]

        except BlockingIOError:
            pass

        if len(pending) > 0:
            for p in sorted(list(pending.keys())):
                if p - 1 == last_printed:
                    print(pending[p], flush=True)
                    pending.pop(p)
                    last_printed = p

        if time.time() - start_time > timeout:
            max_learned = max(list(learned.keys()))
            missing_inst = [i for i in range(0, max_learned) if i not in learned]
            request_msg = encode_json_msg(
                MessageType.CATCHUP_REQUEST, missing_inst=missing_inst
            )
            s.sendto(request_msg, config["acceptors"])
            start_time = time.time()


def client(config, id):
    log_client_info(f"[{id}] started.")
    s = mcast_sender()
    prop_id = 0
    for value in sys.stdin:
        value = value.strip()
        prop_id += 1
        log_client_debug(
            "[%s] sending %s to proposers with prop_id %s" % (id, value, prop_id)
        )
        client_msg = encode_json_msg(
            MessageType.CLIENT_VALUE, value=value, client_id=id, prop_id=prop_id
        )
        time.sleep(1)
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
