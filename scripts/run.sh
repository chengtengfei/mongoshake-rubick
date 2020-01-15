./hypervisor --daemon --exec="./collector.linux -conf=collector.conf" 1>>hypervisor.output 2>&1

nohup ./receiver.linux -conf=receiver.conf 1>/dev/null 2>&1