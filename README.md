# Paxos

## Disclaimer
In my implementation I use a non-blocking UDP socket and depending on the test computer this could lead to buffer overflows.
To prevent this from happening I introduced a timeout of 0.1 seconds in between messages that go from the client to the proposer.
Please keep this in mind when testing my implementation and if necessary increase the timeout until paxos is getting killed.

## Dependencies
You need bash and basic unix tools (grep, sed, etc).
If you want to run the test with msg losses, you need `iptables` with sudo access.

Depending on the network interface used, ip multicast might not be
enabled. You can check using "ifconfig" and checking that the
"MULTICAST" flag is set. You *might* have to enable it using:
```
ifconfig IFACE multicast
```
where `IFACE` is the name of the interface. Using a connected cable/wifi interface probably will not have this problem (e.g. "eth0", "wlan0").

### Python 
- **Option 1**: I only added a single package to the project, which is called [loguruan](https://github.com/Delgan/loguru) and can be installed with:
```bash
pip install loguru
```
- **Option 2**: I used the [Poetry](https://python-poetry.org/) package manager for this python project and you can replicate my setup by using the following commands:
```bash
# Install all the dependencies
poetry install

# Enter virtual env
poetry shell
```

## Logging output
By using loguru you can control the level of output as follows

```bash
# WIthout debugging messages
export LOGURU_LEVEL="INFO"

# Inculding debug messages
export LOGURU_LEVEL="DEBUG"
```

## How to run the tests

1) Place the root of your paxos implementation inside this
directory. For example, if you have the folder `~/paxos-tests/MyPaxos`, your `*.sh` scripts should be directly inside it (e.g. `~/paxos-tests/MyPaxos/acceptor.sh`) and should work when called
from inside the directory itself.

2) Run one of the `run*.sh` from inside THIS folder. When the run
finishes, run `check_all.sh` to check the output of the run. For
example:
```
cd ~/paxos-tests/
./run.sh MyPaxos 100  # each client will submit 100 values
# wait for the run to finish
./check_all.sh # check the run
```

3) After a run ends, run `check_all.sh` to see if everything went OK.
"Test 3" might FAIL in some cases, but with few proposed values and no message loss it should also be OK.

```
./run.sh fake-paxos 100
./check_all.sh
```
